defmodule ReadBench do
  use Benchfella

  @db_dir "tmp/read_bench"
  @stream_id "read_bench"

  @records_count 10_000

  setup_all do
    Application.ensure_all_started(:opal)

    Opal.delete_stream_data(@db_dir, @stream_id)

    {:ok, stream_pid} = Opal.start_stream(@db_dir, @stream_id)

    for _i <- 1..@records_count do
      Opal.store(@stream_id, gen_event())
    end

    metrics = Opal.stream_metrics(@stream_id)

    IO.inspect(metrics, pretty: true, label: "Stream metrics")

    {:ok, stream_pid}
  end

  teardown_all _stream_pid do
    Opal.stop_stream(@stream_id)
    Opal.delete_stream_data(@db_dir, @stream_id)
  end

  bench "read" do
    Opal.read(@stream_id, div(@records_count, 2))
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
