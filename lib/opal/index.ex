defprotocol Opal.Index do
  def put(index, key, value)
  def get(index, key)
  def get_closest_before(index, key)
  def update(index, key, default, fun)
end

defimpl Opal.Index, for: List do
  def put(list, key, value) do
    list
    |> List.keystore(key, 0, {key, value})
    |> List.keysort(0)
  end

  def get(list, key) do
    List.keyfind(list, key, 0)
  end

  def get_closest_before(list, key) do
    Enum.reverse(list)
    |> Enum.drop_while(&(elem(&1, 0) > key))
    |> List.first()
  end

  def update(list, key, default, fun) do
    if index = List.keyfind(list, key, 0) do
      List.update_at(list, index, fn {key, value} -> {key, fun.(value)} end)
    else
      put(list, key, default)
    end
  end
end
