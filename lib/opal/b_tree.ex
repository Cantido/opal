defmodule Opal.BTree do
  defstruct [:max_node_length, is_leaf: true, children: []]

  def new(opts \\ []) do
    %__MODULE__{
      max_node_length: Keyword.get(opts, :max_node_length, 1024)
    }
  end

  def put(%__MODULE__{} = tree, key, value) do
    update(tree, key, value, fn _ -> value end)
  end

  def update(%__MODULE__{is_leaf: true} = tree, key, default, fun) do
    if index = Enum.find_index(tree.children, &(elem(&1, 0) == key)) do
      children = List.update_at(tree.children, index, fn {key, value} -> {key, fun.(value)} end)
      %__MODULE__{tree | children: children}
    else
      if Enum.count(tree.children) >= tree.max_node_length do
        tree
        |> split()
        |> update(key, default, fun)
      else
        child = {key, default}
        children = Enum.sort_by([child | tree.children], &elem(&1, 0))
        %__MODULE__{tree | children: children}
      end
    end
  end

  def update(%__MODULE__{is_leaf: false} = tree, key, default, fun) do
    insert_at_position = find_child_index(tree, key)

    if is_nil(insert_at_position) do
      raise "nil position for child found in a non-leaf node"
    end

    children = List.update_at(tree.children, insert_at_position, &update(&1, key, default, fun))

    %__MODULE__{tree | children: children}
  end

  def update({key, value}, key, _default, fun) do
    {key, fun.(value)}
  end

  defp split(%__MODULE__{} = tree) do
    median_index = div(Enum.count(tree.children), 2)

    {left_children, [median_child | right_children]} = Enum.split(tree.children, median_index)

    left_tree = %__MODULE__{tree | is_leaf: true, children: left_children}
    right_tree = %__MODULE__{tree | is_leaf: true, children: right_children}

    new_children = [left_tree, median_child, right_tree]

    %__MODULE__{tree | is_leaf: false, children: new_children}
  end

  defp find_child_index(%__MODULE__{is_leaf: true} = tree, key) do
    Enum.find_index(tree.children, &(elem(&1, 0) == key))
  end

  defp find_child_index(%__MODULE__{is_leaf: false} = tree, key) do
    tree.children
    |> Enum.with_index()
    |> Enum.reduce_while(0, fn {next_child, child_index}, prev_child_index ->
      case next_child do
        %__MODULE__{} ->
          {:cont, child_index}
        {child_key, _child_value} when key == child_key ->
          {:halt, child_index}
        {child_key, _child_value} when key > child_key ->
          {:cont, child_index + 1}
        {child_key, _child_value} when key < child_key ->
          {:halt, prev_child_index}
      end
    end)
  end

  def get(%__MODULE__{} = tree, key) do
    case find_child_index(tree, key) do
      nil ->
        nil
      index ->
        Enum.at(tree.children, index)
        |> get(key)
    end
  end

  def get({key, value}, key), do: value

  def get_closest_before(%__MODULE__{is_leaf: true} = tree, key) do
    Enum.reverse(tree.children)
    |> Enum.drop_while(fn {child_key, _val} -> child_key > key end)
    |> List.first()
  end

  def get_closest_before(%__MODULE__{is_leaf: false} = tree, key) do
    case find_child_index(tree, key) do
      nil ->
       nil
      index ->
        Enum.at(tree.children, index)
        |> get_closest_before(key)
    end
  end

  def get_closest_before({key, value}, key) do
    value
  end

  defimpl Opal.Index do
    def put(tree, key, value) do
      Opal.BTree.put(tree, key, value)
    end

    def get(tree, key) do
      Opal.BTree.get(tree, key)
    end

    def get_closest_before(tree, key) do
      Opal.BTree.get_closest_before(tree, key)
    end
  end
end
