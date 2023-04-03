defmodule QueryBench do
  use Benchfella

  @db_dir "tmp/query_bench"
  @stream_id "query_bench"

  @records_count 1_000

  @sought_source "mysource"
  @sought_id "myid"

  setup_all do
    Application.ensure_all_started(:opal)

    Opal.delete_stream_data(@db_dir, @stream_id)

    {:ok, stream_pid} = Opal.start_stream(@db_dir, @stream_id)

    for _i <- 1..@records_count do
      Opal.store(@stream_id, gen_event())
    end

    sought_event = %{
      specversion: "1.0",
      source: @sought_source,
      id: @sought_id,
      type: "doesn't matter"
    }
    |> Cloudevents.from_map!()

    Opal.store(@stream_id, sought_event)

    for _i <- 1..@records_count do
      Opal.store(@stream_id, gen_event())
    end

    {:ok, stream_pid}
  end

  teardown_all _stream_pid do
    Opal.stop_stream(@stream_id)
    Opal.delete_stream_data(@db_dir, @stream_id)
  end

  bench "query with index" do
    Opal.query(@stream_id, %{source: @sought_source, id: @sought_id})
  end

  bench "query without index" do
    Opal.query(@stream_id, %{source: @sought_source})
  end

  defp gen_event do
    %{
      specversion: "1.0",
      id: Uniq.UUID.uuid7(),
      source: Uniq.UUID.uuid7(),
      type: Uniq.UUID.uuid7()
    } |> Cloudevents.from_map!()
  end
end
