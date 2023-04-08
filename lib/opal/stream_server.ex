defmodule Opal.StreamServer do
  use GenServer

  alias Opal.Index

  require Logger

  @enforce_keys [
    :avro_header,
    :avro_header_size,
    :stream_dir,
    :primary_index,
    :device
  ]
  defstruct [
    :avro_header,
    :avro_header_size,
    :stream_dir,
    :primary_index,
    :device,
    secondary_indices: [],
    block_sizes: %{},
    current_position: 0,
    current_rownum: 0,
  ]

  @default_opts [
    primary_index: Opal.BTree.new(max_node_length: 1024),
    secondary_indices: %{
      [:source, :id] => Opal.BTree.new(max_node_length: 1024)
    }
  ]

  def start_link(opts) do
    stream_id = Keyword.fetch!(opts, :stream_id)
    GenServer.start_link(__MODULE__, opts, [name: {:global, stream_id}])
  end

  def init(opts) do
    stream_id = Keyword.fetch!(opts, :stream_id)
    database = Keyword.fetch!(opts, :database)
    stream_dir = Path.join(database, stream_id)
    File.mkdir_p(stream_dir)


    opts = Keyword.merge(@default_opts, opts)

    index = Keyword.fetch!(opts, :primary_index)

    events_file = Path.join(stream_dir, "events")

    device = File.open!(events_file, [:read, :append])

    {:ok, schema} = Avrora.Resolver.resolve("io.cloudevents.AvroCloudEvent")
    {:ok, encoded_schema} = Avrora.Schema.Encoder.to_erlavro(schema)
    header = :avro_ocf.make_header(encoded_schema)
    header_bytes = :avro_ocf.encode_header(header)

    {:ok, %__MODULE__{
      avro_header: header,
      avro_header_size: IO.iodata_length(header_bytes),
      stream_dir: stream_dir,
      primary_index: index,
      secondary_indices: Keyword.fetch!(opts, :secondary_indices),
      device: device
    }, {:continue, :build_indices}}
  end

  def store(stream_id, event) do
    GenServer.call({:global, stream_id}, {:store, event})
  end

  def read(stream_id, seq) do
    with {:ok, row} <- GenServer.call({:global, stream_id}, {:read, seq}) do
      deserialize_row(row)
    end
  end

  defp deserialize_row(nil) do
    {:ok, nil}
  end

  defp deserialize_row(row) do
    case Base.decode64(String.trim_trailing(row)) do
      {:ok, event} ->
        Cloudevents.from_json(event)
      err ->
        err
    end
  end

  def query(stream_id, query) do
    GenServer.call({:global, stream_id}, {:query, query})
  end

  def explain(stream_id, query) do
    GenServer.call({:global, stream_id}, {:explain, query})
  end

  def metrics(stream_id) do
    GenServer.call({:global, stream_id}, :metrics)
  end

  def handle_call({:read, seq}, _from, %__MODULE__{} = state) do
    event = get_row(seq - 1, state)
    {:reply, {:ok, event}, state}
  end

  def handle_call({:store, event}, _from, %__MODULE__{} = state) do
    avro_map =
      event
      |> Map.from_struct()
      |> then(fn map ->
        attributes =
          Map.take(map, [:specversion, :source, :id, :type, :subject, :time, :datacontenttype, :dataschema])
          |> Map.new(fn {key, val} -> {Atom.to_string(key), val} end)
          |> Enum.into(map.extensions)

        %{attribute: attributes, data: map.data}
      end)
    {:ok, bytes_to_write} = Avrora.encode_plain(avro_map, schema_name: "io.cloudevents.AvroCloudEvent")

    :avro_ocf.append_file(state.device, state.avro_header, [bytes_to_write])

    event_row_offset = state.current_position
    event_row_index = state.current_rownum

    state =
      state
      |> Map.update!(:current_position, &(&1 + byte_size(bytes_to_write)))
      |> Map.update!(:current_rownum, &(&1 + 1))

    {:reply, :ok, state, {:continue, {:update_indices, event, event_row_index, event_row_offset}}}
  end

  def handle_call({:find, source, id}, _from, %__MODULE__{} = state) do
    {:ok, _newpos} = :file.position(state.device, :bof)

    index = Map.get(state.secondary_indices, [:source, :id])

    event =
    if rownum = Index.get(index, source <> "\0" <> id) do
      get_row(rownum, state)
    else
      nil
    end

    {:reply, {:ok, event}, state}
  end

  def handle_call({:query, query}, _from, %__MODULE__{} = state) do
    matching_events =
      plan(query, state)
      |> Stream.map(fn row_index ->
        get_row(row_index, state)
      end)
      |> Stream.filter(fn event ->
        Enum.all?(query, fn {key, value} ->
          Map.get(event, key) == value
        end)
      end)
      |> Enum.to_list()

    {:reply, {:ok, matching_events}, state}
  end

  def handle_call({:explain, query}, _from, %__MODULE__{} = state) do
    plan = %{
      row_count: Enum.count(plan(query, state))
    }

    {:reply, {:ok, plan}, state}
  end

  def handle_call(:metrics, _from, %__MODULE__{} = state) do
    metrics = %{
      byte_size: state.current_position,
      row_count: state.current_rownum
    }

    {:reply, metrics, state}
  end

  def handle_continue(:load_database, %__MODULE__{} = state) do
    events_file = Path.join(state.stream_dir, "events")
    events_file_stats = File.stat!(events_file)

    if events_file_stats.size == 0 do
      :ok = :avro_ocf.write_header(state.device, state.avro_header)
      {:ok, _file_position} = :file.position(state.device, :eof)
    end

    {:ok, file_position} = :file.position(state.device, :eof)

    state = %__MODULE__{state | current_position: file_position}

    {:noreply, state}
  end

  def handle_continue(:build_indices, %__MODULE__{} = state) do
    # events_file = Path.join(state.stream_dir, "events")

    # stats = File.stat!(events_file)


    # if stats.size > 0 do
    #   {_header, _type, rows} = :avro_ocf.decode_file(events_file)

    #   Enum.reduce(rows, state, fn last_event, state ->
    #     last_rownum = state.current_rownum
    #     last_offset = state.current_position

    #     state
    #     |> Map.update!(:current_position, &(&1 + byte_size(encoded_event) + 1))
    #     |> Map.update!(:current_rownum, &(&1 + 1))
    #     |> update_primary_index(last_rownum, last_offset)
    #     |> update_secondary_indices(last_event, last_offset)
    #   end)
    #   {:noreply, state}
    # else
    #   {:noreply, state}
    # end
    {:noreply, state}
  end

  def handle_continue({:update_indices, last_event, last_rownum, last_offset}, %__MODULE__{} = state) do
    state =
      state
      |> update_primary_index(last_rownum, last_offset)
      |> update_secondary_indices(last_event, last_rownum)

    {:noreply, state}
  end

  defp plan(query, state) do
    relevant_indices = find_relevant_indices(state.secondary_indices, Map.keys(query))

    if Enum.any?(relevant_indices) do
      Enum.map(relevant_indices, fn {index_attributes, index} ->
        index_key = form_index_binary_key(index_attributes, query)
        MapSet.new(List.wrap(Index.get(index, index_key)))
      end)
      |> Enum.reduce(fn rowset, rowacc ->
        MapSet.intersection(rowset, rowacc)
      end)
      |> Enum.to_list()
      |> Enum.sort()
    else
      if state.current_rownum == 0 do
        []
      else
        0..(state.current_rownum - 1)
      end
    end
  end

  defp get_row(rownum, state) do
    with row_binary when is_binary(row_binary) <- Opal.Avro.ocf_object_at(state.device, rownum),
         {:ok, row} = Avrora.decode_plain(row_binary, schema_name: "io.cloudevents.AvroCloudEvent") do
      row
    else
      :eof -> :eof
      err -> err
    end
  end

  # Returns all indices whose attributes are all requested in the query.
  defp find_relevant_indices(indices, query_attributes) do
    query_attr_set = MapSet.new(query_attributes)

    Map.filter(indices, fn {index_attributes, _index} ->
      index_attr_set = MapSet.new(index_attributes)

      MapSet.subset?(index_attr_set, query_attr_set)
    end)
  end

  defp update_primary_index(%__MODULE__{} = state, last_rownum, last_offset) do
    Map.update!(state, :primary_index, &Index.put(&1, last_rownum, last_offset))
  end

  defp update_secondary_indices(%__MODULE__{} = state, event, rownum) do
    secondary_indices =
      Map.new(state.secondary_indices, fn {event_fields, index} ->
        case event_fields do
          [:source, :id] ->
            key = form_index_binary_key([:source, :id], event)
            {event_fields, Index.put(index, key, rownum)}
        end
      end)

    %__MODULE__{state | secondary_indices: secondary_indices}
  end

  defp form_index_binary_key(attribute_list, attribute_source) do
    Enum.map_join(attribute_list, "\0", fn attribute_key ->
      Map.get(attribute_source, attribute_key)
    end)
  end

  def terminate(_reason, %__MODULE__{} = state) do
    File.close(state.device)
  end
end
