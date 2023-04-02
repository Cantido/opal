defmodule OpalTest do
  use ExUnit.Case
  doctest Opal

  @tag :tmp_dir
  test "writes events to file", %{tmp_dir: dir} do
    stream_id = "writeseventstofiletest"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    :ok = Opal.store(stream_id, "hello")

    {:ok, "hello"} = Opal.read(stream_id, 1)
  end

  @tag :tmp_dir
  test "can write multiple events", %{tmp_dir: dir} do
    stream_id = "multipleevents"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id, index_period_bytes: 10})

    :ok = Opal.store(stream_id, "one")
    :ok = Opal.store(stream_id, "two")
    :ok = Opal.store(stream_id, "three")
    :ok = Opal.store(stream_id, "four")
    :ok = Opal.store(stream_id, "five")
    :ok = Opal.store(stream_id, "six")
    :ok = Opal.store(stream_id, "seven")

    {:ok, "five"} = Opal.read(stream_id, 5)
  end
end
