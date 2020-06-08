defmodule RatchetWrenchTest do
  use ExUnit.Case

  # setup_all do
  #   on_exit fn ->
  #   end
  # end

  test "GenServer Sample" do
    {:ok, _pid} = RatchetWrench.SessionPool.start_link()
    assert RatchetWrench.SessionPool.checkout() == "con1"
    assert RatchetWrench.SessionPool.checkin("con1")
    assert RatchetWrench.SessionPool.checkout() == "con2"
    assert RatchetWrench.SessionPool.checkout() == "con3"
  end

  test "Session" do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    session = RatchetWrench.create_session(connection)
    assert session != nil
    {:ok, _} = RatchetWrench.delete_session(connection, session)
  end

  test "Session Batch create" do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    session_list = RatchetWrench.batch_create_session(connection, 3)

    Enum.each(session_list, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
  end
end
