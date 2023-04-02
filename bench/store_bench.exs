defmodule StoreBench do
  use Benchfella

  @db_dir "tmp/store_bench"
  @stream_id "store_bench"

  setup_all do
    Application.ensure_all_started(:opal)
    {:ok, pid} = Opal.start_stream(@db_dir, @stream_id)
  end

  teardown_all _pid do
    Opal.stop_stream(@stream_id)
    Opal.delete_stream_data(@db_dir, @stream_id)
  end

  bench "store" do
    Opal.store(@stream_id, "hello")
  end
end
