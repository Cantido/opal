defmodule Opal.Avro do
  @moduledoc """
  Functions for working with Avro-formatted files and binaries.
  """

  def ocf_object_at(device, n) do
    {:ok, 0} = :file.position(device, :bof)
    _header = ocf_header(device)


    with block_with_row when is_map(block_with_row) <- read_block_containing_row(device, n),
         {:ok, rows} <- Avrora.decode_plain(block_with_row.block, schema_name: "BinaryList") do
      IO.iodata_to_binary(Enum.at(rows, n - block_with_row.range.first))
    end
  end

  def read_block_containing_row(device, rownum, rows_so_far \\ 0) do
    with object_count when is_integer(object_count) <- long(device),
         block_size when is_integer(block_size) <- long(device) do
      row_range = (rows_so_far + 1)..(rows_so_far + object_count)
      if rownum in  row_range do
        with block when is_binary(block) <- IO.binread(device, block_size),
             sync_marker when is_binary(sync_marker) <- IO.binread(device, 16) do
          %{
            range: row_range,
            object_count: object_count,
            block_size: block_size,
            block: block,
            sync_marker: sync_marker
          }
        end
      else
        :file.position(device, {:cur, block_size + 16})

        read_block_containing_row(device, rownum, rows_so_far + block_size + 16)
      end
    end
  end

  def ocf_data_block(device) do
    with object_count when is_integer(object_count) <- long(device),
         block_size when is_integer(block_size) <- long(device),
         block when is_binary(block) <- IO.binread(device, block_size),
         sync_marker when is_binary(sync_marker) <- IO.binread(device, 16) do
      %{
        object_count: object_count,
        block_size: block_size,
        block: block,
        sync_marker: sync_marker
      }
    end
  end

  def ocf_header(device) do
    with <<"Obj", 1>> <- IO.binread(device, 4),
         header_map when is_binary(header_map) <- map(device),
         sync_marker when is_binary(sync_marker) <- IO.binread(device, 16) do
      %{
        header: header_map,
        sync_marker: sync_marker
      }
    end
  end

  def map(device, prev_blocks \\ <<>>) do
    keypair_count = long(device)

    cond do
      keypair_count == :eof ->
        :eof
      keypair_count == 0 ->
        prev_blocks
      keypair_count < 0 ->
        with block_length when is_integer(block_length) <- long(device),
             block when is_binary(block) <- IO.binread(device, block_length) do
          map(device, prev_blocks <> block)
        end
      keypair_count > 0 ->
        raise "TODO"
    end
  end

  def skip_map(device) do
    keypair_count = long(device)

    cond do
      keypair_count == :eof ->
        :eof
      keypair_count == 0 ->
        :ok
      keypair_count < 0 ->
        case long(device) do
          :eof ->
            :eof
          block_length ->
            :file.position(device, {:cur, block_length})
            skip_map(device)
        end
      keypair_count > 0 ->
        raise "I can't skip a map if it doesn't provide the block size"
    end
  end

  def boolean(device) do
    case IO.binread(device, 1) do
      :eof -> :eof
      <<value>> -> value == 1
    end
  end

  def string(device), do: bytes(device)

  def bytes(device) do
    case long(device) do
      :eof -> :eof
      length -> IO.binread(device, length)
    end
  end

  def float(device) do
    case IO.binread(device, 4) do
      :eof -> :eof
      <<value::float-size(32)>> -> value
    end
  end

  def double(device) do
    case IO.binread(device, 8) do
      :eof -> :eof
      <<value::float-size(64)>> -> value
    end
  end

  def int(device) do
    case varint(device, 0, 0, 32) do
      :eof -> :eof
      int -> zigzag(int)
    end
  end

  def long(device) do
    case varint(device, 0, 0, 64) do
      :eof -> :eof
      int -> zigzag(int)
    end
  end

  def zigzag(int), do: Bitwise.bsr(int, 1) |> Bitwise.bxor(-Bitwise.band(int, 1))

  def varint(device, acc, acc_bits, max_bits) do
    case IO.binread(device, 1) do
      :eof ->
        :eof
      <<tag::1, value::7>> ->

      if acc_bits >= max_bits do
        raise "Can't finish decoding varint, we can only decode #{max_bits} bits but it's taking at least #{acc_bits}"
      end

      new_acc = Bitwise.bsl(value, acc_bits) |> Bitwise.bor(acc)

      if tag == 0 do
        new_acc
      else
        varint(device, new_acc, acc_bits + 7, max_bits)
      end
    end
  end
end
