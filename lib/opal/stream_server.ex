defmodule Opal.StreamServer do
  use GenServer

  require Logger

  def start_link(opts) do
    stream_id = Keyword.fetch!(opts, :stream_id)
    GenServer.start_link(__MODULE__, opts, [name: {:global, stream_id}])
  end

  def init(opts) do
    stream_id = Keyword.fetch!(opts, :stream_id)
    database = Keyword.fetch!(opts, :database)
    stream_dir = Path.join(database, stream_id)
    File.mkdir_p(stream_dir)

    index_period_bytes = Keyword.get(opts, :index_period_bytes, 100)
    {:ok, %{stream_dir: stream_dir, index: [], index_period_bytes: index_period_bytes}}
  end

  def store(stream_id, event) do
    GenServer.call({:global, stream_id}, {:store, event})
  end

  def read(stream_id, seq) do
    GenServer.call({:global, stream_id}, {:read, seq})
  end

  def handle_call({:read, seq}, _from, state) do
    events_file_path = Path.join(state.stream_dir, "events")


    {indexed_seq, indexed_offset} =
      Enum.reduce_while(state.index, {1, 0}, fn {next_seq, next_offset}, {prev_seq, prev_offset} ->
        if next_seq > seq do
          {:halt, {prev_seq, prev_offset}}
        else
          {:cont, {next_seq, next_offset}}
        end
      end)

    # Logger.debug(seq: seq, indexed_seq: indexed_seq, indexed_offset: indexed_offset)

    event =
      File.open(events_file_path, [:read], fn file ->
        {:ok, _newpos} = :file.position(file, indexed_offset)
        event =
          file
          |> IO.stream(:line)
          |> Enum.at(seq - indexed_seq)
        if is_nil(event) do
          nil
        else
          String.trim_trailing(event)
        end
      end)

    {:reply, event, state}
  end

  def handle_call({:store, event}, _from, state) do
    events_file_path = Path.join(state.stream_dir, "events")

    {:ok, :ok} = File.open(events_file_path, [:append], fn file ->
      IO.puts(file, event)
    end)

    {:reply, :ok, state, {:continue, :update_index}}
  end

  def handle_continue(:update_index, state) do
    events_file_path = Path.join(state.stream_dir, "events")

    {:ok, %File.Stat{} = stat} = File.stat(events_file_path)
    last_index_position = Map.get(state, :last_index_position, 0)

    state =
      if (stat.size - last_index_position) > state.index_period_bytes do
        Map.update!(state, :index, fn index ->
          event_count =
            File.stream!(events_file_path, [], :line)
            # |> tap(&Logger.debug([event_stream: Enum.to_list(&1)]))
            |> Enum.count()
#          Logger.debug("Indexed at #{event_count + 1}, offset #{stat.size}")
          index ++ [{event_count + 1, stat.size}]
        end)
        |> Map.put(:last_index_position, stat.size)
      else
        state
      end

    {:noreply, state}
  end
end
