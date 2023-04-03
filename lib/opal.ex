defmodule Opal do
  @moduledoc """
  Documentation for `Opal`.
  """

  def start_stream(database, stream_id, opts \\ []) do
    opts =
      opts
      |> Keyword.put(:database, database)
      |> Keyword.put(:stream_id, stream_id)

    DynamicSupervisor.start_child(Opal.StreamServerSupervisor, {Opal.StreamServer, opts})
  end

  def stop_stream(stream_id) do
    stream_pid = GenServer.whereis({:global, stream_id})
    DynamicSupervisor.terminate_child(Opal.StreamServerSupervisor, stream_pid)
  end

  def delete_stream_data(database, stream_id) do
    File.rm_rf(Path.join(database, stream_id))
  end

  def store(stream_id, event) do
    Opal.StreamServer.store(stream_id, event)
  end

  def read(stream_id, seq) do
    Opal.StreamServer.read(stream_id, seq)
  end

  def find(stream_id, source, id) do
    Opal.StreamServer.find(stream_id, source, id)
  end

  def query(stream_id, params) do
    Opal.StreamServer.query(stream_id, params)
  end

  def stream_metrics(stream_id) do
    Opal.StreamServer.metrics(stream_id)
  end
end
