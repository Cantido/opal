defmodule OpalTest do
  use ExUnit.Case
  doctest Opal

  import Opal.StoreFixtures

  @tag :tmp_dir
  test "writes events to file", %{tmp_dir: dir} do
    stream_id = "writeseventstofiletest"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    event = event_fixture()
    :ok = Opal.store(stream_id, event)

    {:ok, actual} = Opal.read(stream_id, 1)

    assert event.id == actual.id
  end

  @tag :tmp_dir
  test "looking up a nonexistent revision number returns nil", %{tmp_dir: dir} do
    stream_id = "nonexistentrevision"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    {:ok, actual} = Opal.read(stream_id, 1)

    assert is_nil(actual)
  end

  describe "query/2" do
    @tag :tmp_dir
    test "can make a query matching a type", %{tmp_dir: dir} do
      stream_id = "eventquerytype"

      {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

      event = event_fixture()
      :ok = Opal.store(stream_id, event)

      {:ok, events} = Opal.query(stream_id, %{type: event.type})

      [actual] = events
      assert event.id == actual.id
    end

    @tag :tmp_dir
    test "can make a query matching multiple attributes with no index", %{tmp_dir: dir} do
      stream_id = "eventquerytypesource"

      {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

      event = event_fixture()
      :ok = Opal.store(stream_id, event)

      {:ok, events} = Opal.query(stream_id, %{type: event.type, source: event.source})

      [actual] = events
      assert event.id == actual.id
    end

    @tag :tmp_dir
    test "can make a query matching multiple attributes with an index", %{tmp_dir: dir} do
      stream_id = "eventquerysourceid"

      {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

      event = event_fixture()
      :ok = Opal.store(stream_id, event)

      {:ok, events} = Opal.query(stream_id, %{source: event.source, id: event.id})

      [actual] = events
      assert event.id == actual.id
    end
  end

  @tag :tmp_dir
  test "can write multiple events", %{tmp_dir: dir} do
    stream_id = "multipleevents"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    five = event_fixture()

    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, five)
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())

    {:ok, actual} = Opal.read(stream_id, 5)

    assert five.id == actual.id
  end

  @tag :tmp_dir
  test "stores values with newlines", %{tmp_dir: dir} do
    stream_id = "newlinetest"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    second = event_fixture()

    :ok = Opal.store(stream_id, event_fixture(data: "\n"))
    :ok = Opal.store(stream_id, second)

    {:ok, actual} = Opal.read(stream_id, 2)

    assert second.id == actual.id
  end

  @tag :tmp_dir
  test "can get stream metrics", %{tmp_dir: dir} do
    stream_id = "streammetrics"

    {:ok, _pid} = start_supervised({Opal.StreamServer, database: dir, stream_id: stream_id})

    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())
    :ok = Opal.store(stream_id, event_fixture())

    metrics = Opal.stream_metrics(stream_id)

    assert metrics.row_count == 6
    assert metrics.byte_size > 0
  end
end
