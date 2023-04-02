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
    :index_period_bytes,
    :device,
    last_index_position: 0,
    current_position: 0,
    current_seqnum: 0,
  ]

  @default_opts [
    index: Opal.BTree.new(max_node_length: 1024),
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
      index_period_bytes: index_period_bytes,
      device: File.open!(Path.join(stream_dir, "events"), [:utf8, :read, :append])
    }}
  end

  def store(stream_id, event) do
    event = Base.encode64(to_string(event))
    GenServer.call({:global, stream_id}, {:store, event})
  end

  def read(stream_id, seq) do
    with {:ok, event} <- GenServer.call({:global, stream_id}, {:read, seq}) do
      Base.decode64(event)
    end
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

    event =
      if is_nil(event) do
        nil
      else
        String.trim_trailing(event)
      end

    {:reply, {:ok, event}, state}
  end

  def handle_call({:store, event}, _from, %__MODULE__{} = state) do
    IO.puts(state.device, event)

    state =
      state
      |> Map.update(:current_position, byte_size(event), &(&1 + byte_size(event) + 1))
      |> Map.update(:current_seqnum, 1, &(&1 + 1))

    if (state.current_position - state.last_index_position) > state.index_period_bytes do
      {:reply, :ok, state, {:continue, :update_index}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_continue(:update_index, %__MODULE__{} = state) do
    state =
      Map.update!(state, :index, &Index.put(&1, state.current_seqnum + 1, state.current_position))
      |> Map.put(:last_index_position, state.current_position)

    {:noreply, state}
  end

  def terminate(_reason, %__MODULE__{} = state) do
    File.close(state.device)
  end
end
