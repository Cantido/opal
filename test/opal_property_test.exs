defmodule OpalPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @basedir "tmp/opal_property_test"

  setup do
    on_exit fn ->
      File.rm_rf!(@basedir)
    end
  end

  def event do
    binary(min_length: 100)
  end

  property "inserted data is accessible" do
    check all events <- list_of(event(), min_length: 1) do
      stream_id = Uniq.UUID.uuid7()
      {:ok, _pid} = Opal.start_stream(@basedir, stream_id)

      Enum.with_index(events)
      |> Enum.each(fn {event, index} ->
        :ok = Opal.store(stream_id, event)

        {:ok, result} = Opal.read(stream_id, index + 1)

        assert event == result
      end)

      Opal.stop_stream(stream_id)
    end
  end
end
