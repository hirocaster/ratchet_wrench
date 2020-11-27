defmodule RatchetWrench.ModelTest do
  use ExUnit.Case

  test "defined uuid" do
    assert Singer.__uuid__() == :singer_id
  end

  test "fetch table_name" do
    assert Singer.__table_name__() == "singers"
    assert UserItem.__table_name__() == "user_items"
  end

  test "defined pk" do
    assert Singer.__pk__() == [:singer_id]
    assert UserItem.__pk__() == [:user_id, :user_item_id]
  end

  test "defined interleave" do
    assert Singer.__interleave__() == []
    assert UserLog.__interleave__() == [:user_id]
  end
end
