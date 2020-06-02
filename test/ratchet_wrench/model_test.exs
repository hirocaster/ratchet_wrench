defmodule RatchetWrench.ModelTest do
  use ExUnit.Case

  test "defined pk" do
    assert Singer.__pk__ == :singer_id
  end

  test "fetch table_name" do
    assert Singer.__table_name__ == "singers"
    assert UserItem.__table_name__ == "user_items"
  end
end
