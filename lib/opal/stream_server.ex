defmodule Opal.StreamServer do
  use GenServer

  alias Opal.Index

  require Logger

  @enforce_keys [
    :stream_dir,
    :index,
    :index_period_bytes,
    :device
  ]
  defstruct [
    :stream_dir,
    :index,
    :source_id_index,
    :index_period_bytes,
    :device,
    last_index_position: 0,
    current_position: 0,
    current_seqnum: 0,
  ]

  @default_opts [
    index: Opal.BTree.new(max_node_length: 1024),
    source_id_index: Opal.BTree.new(max_node_length: 1024),
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

    index = Keyword.fetch!(opts, :index)

    index_period_bytes = Keyword.fetch!(opts, :index_period_bytes)
    {:ok, %__MODULE__{
      stream_dir: stream_dir,
      index: index,
      source_id_index: Keyword.fetch!(opts, :source_id_index),
      index_period_bytes: index_period_bytes,
      device: File.open!(Path.join(stream_dir, "events"), [:utf8, :read, :append])
    }}
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

  def find(stream_id, source, id) do
    with {:ok, row} <- GenServer.call({:global, stream_id}, {:find, source, id}) do
      deserialize_row(row)
    end
  end

  def metrics(stream_id) do
    GenServer.call({:global, stream_id}, :metrics)
  end

  def handle_call({:read, seq}, _from, %__MODULE__{} = state) do
    {indexed_seq, indexed_offset} =
      case Index.get_closest_before(state.index, seq) do
        nil -> {1, 0}
        val -> val
      end

    # Logger.debug(seq: seq, indexed_seq: indexed_seq, indexed_offset: indexed_offset)

    {:ok, _newpos} = :file.position(state.device, indexed_offset)
    event =
      state.device
      |> IO.stream(:line)
      |> Enum.at(seq - indexed_seq)

    {:reply, {:ok, event}, state}
  end

  def handle_call({:store, event}, _from, %__MODULE__{} = state) do
    encoded_event = Base.encode64(Cloudevents.to_json(event))
    IO.puts(state.device, encoded_event)

    event_offset = state.current_position

    state =
      state
      |> Map.update(:current_position, byte_size(encoded_event), &(&1 + byte_size(encoded_event) + 1))
      |> Map.update(:current_seqnum, 1, &(&1 + 1))

    {:reply, :ok, state, {:continue, {:update_indices, event, event_offset}}}
  end

  def handle_call({:find, source, id}, _from, %__MODULE__{} = state) do
    {:ok, _newpos} = :file.position(state.device, :bof)

    if offset = Index.get(state.source_id_index, source <> "\0" <> id) do
      {:ok, _newpos} = :file.position(state.device, offset)
      event =
        state.device
        |> IO.stream(:line)
        |> Enum.at(0)

      {:reply, {:ok, event}, state}
    else
      {:reply, {:ok, nil}, state}
    end
  end

  def handle_call(:metrics, _from, %__MODULE__{} = state) do
    metrics = %{
      byte_size: state.current_position,
      current_revision: state.current_seqnum
    }

    {:reply, metrics, state}
  end

  def handle_continue({:update_indices, last_event, last_offset}, %__MODULE__{} = state) do
    state =
      state
      |> update_primary_index()
      |> update_source_id_index(last_event, last_offset)

    {:noreply, state}
  end

  defp update_primary_index(%__MODULE__{} = state) do
    if (state.current_position - state.last_index_position) > state.index_period_bytes do
      Map.update!(state, :index, &Index.put(&1, state.current_seqnum + 1, state.current_position))
      |> Map.put(:last_index_position, state.current_position)
    else
      state
    end
  end

  defp update_source_id_index(%__MODULE__{} = state, event, offset) do
    key = event.source <> "\0" <> event.id

    Map.update!(state, :source_id_index, &Index.put(&1, key, offset))
  end

  def terminate(_reason, %__MODULE__{} = state) do
    File.close(state.device)
  end
end
