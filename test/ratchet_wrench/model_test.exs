defmodule RatchetWrench.ModelTest do
  use ExUnit.Case

  test "defined pk" do
    assert Singer.__pk__ == :singer_id
  end

  # TODO: fix this
  # test "fetch table_name" do
  #   assert Singer.__table_name__ == "singers"
  # end
end
