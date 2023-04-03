defmodule Opal.StoreFixtures do
  def event_fixture(attrs \\ []) do
    attrs
    |> Enum.into(%{
      specversion: "1.0",
      id: Uniq.UUID.uuid7(),
      source: Uniq.UUID.uuid7(),
      type: Uniq.UUID.uuid7()
    })
    |> Cloudevents.from_map!()
  end
end
