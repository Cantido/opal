defmodule StoreBench do
  use Benchfella

  @db_dir "tmp/store_bench"
  @stream_id "store_bench"

  setup_all do
    Application.ensure_all_started(:opal)
    {:ok, pid} = Opal.start_stream(@db_dir, @stream_id)
  end

  teardown_all _pid do
    metrics = Opal.stream_metrics(@stream_id)

    Map.put(metrics, :average_event_size, metrics.byte_size / metrics.current_revision)
    |> IO.inspect(pretty: true, label: "Stream metrics")

    Opal.stop_stream(@stream_id)
    Opal.delete_stream_data(@db_dir, @stream_id)
  end

  bench "store", [event: gen_event()] do
    Opal.store(@stream_id, event)
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
