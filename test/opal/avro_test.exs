defmodule Opal.AvroTest do
  use ExUnit.Case, async: true
  alias Opal.Avro
  doctest Opal.Avro

  @tag :tmp_dir
  test "header", %{tmp_dir: dir} do
    payload = %{
      attribute: %{
        attr: "asdf"
      },
      data: "asdf"
    }
    {:ok, bin} = Avrora.encode(payload, schema_name: "io.cloudevents.AvroCloudEvent", format: :ocf)

    path = Path.join(dir, "db")
    File.write!(path, bin)

    assert Avro.header(path) == 0
  end
end
