defmodule Opal.StreamServer do
  use GenServer

  alias Opal.Index

  require Logger

  @enforce_keys [
    :stream_dir,
    :primary_index,
    :index_period_bytes,
    :device
  ]
  defstruct [
    :stream_dir,
    :primary_index,
    :index_period_bytes,
    :device,
    secondary_indices: [],
    last_index_position: 0,
    current_position: 0,
    current_rownum: 0,
  ]

  @default_opts [
    primary_index: Opal.BTree.new(max_node_length: 1024),
    secondary_indices: %{
      [:source, :id] => Opal.BTree.new(max_node_length: 1024)
    },
    index_period_bytes: 100,
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

    index_period_bytes = Keyword.fetch!(opts, :index_period_bytes)
    {:ok, %__MODULE__{
      stream_dir: stream_dir,
      primary_index: index,
      secondary_indices: Keyword.fetch!(opts, :secondary_indices),
      index_period_bytes: index_period_bytes,
      device: File.open!(Path.join(stream_dir, "events"), [:utf8, :read, :append])
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
    encoded_event = Base.encode64(Cloudevents.to_json(event))
    IO.puts(state.device, encoded_event)

    event_row_index = state.current_rownum

    state =
      state
      |> Map.update!(:current_position, &(&1 + byte_size(encoded_event) + 1))
      |> Map.update!(:current_rownum, &(&1 + 1))

    {:reply, :ok, state, {:continue, {:update_indices, event, event_row_index}}}
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
        {:ok, event} =
          get_row(row_index, state)
          |> deserialize_row()
        event
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

  def handle_continue(:build_indices, %__MODULE__{} = state) do
    state =
      state.device
      |> IO.stream(:line)
      |> Enum.reduce(state, fn encoded_event, state ->
        {:ok, last_event} = deserialize_row(encoded_event)
        last_offset = state.current_position

        state
        |> Map.update!(:current_position, &(&1 + byte_size(encoded_event) + 1))
        |> Map.update!(:current_rownum, &(&1 + 1))
        |> update_primary_index()
        |> update_secondary_indices(last_event, last_offset)
      end)

    {:noreply, state}
  end

  def handle_continue({:update_indices, last_event, last_offset}, %__MODULE__{} = state) do
    state =
      state
      |> update_primary_index()
      |> update_secondary_indices(last_event, last_offset)

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
    {indexed_rownum, indexed_offset} =
      case Index.get_closest_before(state.primary_index, rownum) do
        nil -> {0, 0}
        val -> val
      end

    {:ok, _newpos} = :file.position(state.device, indexed_offset)

    state.device
    |> IO.stream(:line)
    |> Enum.at(rownum - indexed_rownum)
  end

  # Returns all indices whose attributes are all requested in the query.
  defp find_relevant_indices(indices, query_attributes) do
    query_attr_set = MapSet.new(query_attributes)

    Map.filter(indices, fn {index_attributes, _index} ->
      index_attr_set = MapSet.new(index_attributes)

      MapSet.subset?(index_attr_set, query_attr_set)
    end)
  end

  defp update_primary_index(%__MODULE__{} = state) do
    if (state.current_position - state.last_index_position) > state.index_period_bytes do
      Map.update!(state, :primary_index, &Index.put(&1, state.current_rownum, state.current_position))
      |> Map.put(:last_index_position, state.current_position)
    else
      state
    end
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
