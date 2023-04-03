defmodule ReadBench do
  use Benchfella

  @db_dir "tmp/read_bench"
  @stream_id "read_bench"

  @records_count 100_000

  setup_all do
    Application.ensure_all_started(:opal)
    {:ok, stream_pid} = Opal.start_stream(@db_dir, @stream_id)

    for i <- 1..@records_count do
      Opal.store(@stream_id, to_string(i))
    end

    {:ok, stream_pid}
  end

  teardown_all _stream_pid do
    Opal.stop_stream(@stream_id)
    Opal.delete_stream_data(@db_dir, @stream_id)
  end

  bench "read" do
    Opal.read(@stream_id, div(@records_count, 2))
  end
end
