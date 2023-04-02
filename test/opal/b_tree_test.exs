defmodule Opal.BTreeTest do
  use ExUnit.Case, async: true
  alias Opal.BTree
  doctest Opal.BTree

  test "put and get" do
    result =
      BTree.new()
      |> BTree.put("key1", "asdf")
      |> BTree.get("key1")

    assert result == "asdf"
  end

  test "put and get multiple values" do
    tree =
      BTree.new(max_node_length: 5)
      |> BTree.put(<<1>>, "1")
      |> BTree.put(<<2>>, "2")
      |> BTree.put(<<3>>, "3")
      |> BTree.put(<<4>>, "4")
      |> BTree.put(<<5>>, "5")
      |> BTree.put(<<6>>, "6")
      |> BTree.put(<<7>>, "7")
      |> BTree.put(<<8>>, "8")

    assert BTree.get(tree, <<1>>) == "1"
    assert BTree.get(tree, <<2>>) == "2"
    assert BTree.get(tree, <<3>>) == "3"
    assert BTree.get(tree, <<4>>) == "4"
    assert BTree.get(tree, <<5>>) == "5"
    assert BTree.get(tree, <<6>>) == "6"
    assert BTree.get(tree, <<7>>) == "7"
    assert BTree.get(tree, <<8>>) == "8"
  end

  describe "get_closest/2" do
    test "on a leaf node" do
      tree =
        BTree.new()
        |> BTree.put(1, "1")
        |> BTree.put(5, "5")
        |> BTree.put(8, "8")

      assert BTree.get_closest_before(tree, 6) == {5, "5"}
    end

    test "on a non-leaf node" do
      tree =
        BTree.new(max_node_length: 3)
        |> BTree.put(1, "1")
        |> BTree.put(3, "3")
        |> BTree.put(5, "5")
        |> BTree.put(7, "7")
        |> BTree.put(9, "9")
        |> BTree.put(11, "11")

      assert BTree.get_closest_before(tree, 6) == {5, "5"}
    end
  end
end
