defmodule RatchetWrench.SessionPoolTest do
  use ExUnit.Case

  setup_all do
    session_min = RatchetWrench.SessionPool.session_min()
    System.put_env("RATCHET_WRENCH_SESSION_MIN", "3")

    session_monitor_interval = RatchetWrench.SessionPool.session_monitor_interval()
    System.put_env("RATCHET_WRENCH_SESSION_MONITOR_INTERVAL", "1000") # 1sec

    session_bust_num = RatchetWrench.SessionPool.session_bust_num()
    System.put_env("RATCHET_WRENCH_SESSION_BUST", "10")

    start_supervised({RatchetWrench.SessionPool, %RatchetWrench.Pool{}})

    on_exit fn ->
      System.put_env("RATCHET_WRENCH_SESSION_MIN", session_min  |> Integer.to_string())
      System.put_env("RATCHET_WRENCH_SESSION_MONITOR_INTERVAL", session_monitor_interval |> Integer.to_string())
      System.put_env("RATCHET_WRENCH_SESSION_BUST", session_bust_num |> Integer.to_string())
    end
  end

  test "Check setup config" do
    assert RatchetWrench.SessionPool.session_min() == 3
    assert RatchetWrench.SessionPool.session_monitor_interval() == 1000
    assert RatchetWrench.SessionPool.session_bust_num() == 10
  end

  # test "async .checkout() / .checkin()" do
  #   session = RatchetWrench.SessionPool.checkout()
  #   assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
  #   RatchetWrench.SessionPool.checkin(session)

  #   0..3
  #   |> Enum.map(fn(_) ->
  #     Task.async(fn ->
  #       session = RatchetWrench.SessionPool.checkout()
  #       assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
  #       RatchetWrench.SessionPool.checkin(session)
  #     end)
  #     # session = RatchetWrench.SessionPool.checkout()
  #     # assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
  #     # RatchetWrench.SessionPool.checkin(session)
  #   end)
  #   |> Enum.map(&Task.await &1, 60000)
  # end

  test "FIFO in session pool" do
    session = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session)
    pool = RatchetWrench.SessionPool.pool()
    assert session.name == List.last(pool.idle).name
  end

  test ".is_safe_session?" do
    session = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session)
    assert RatchetWrench.SessionPool.is_safe_session?(session)
  end

  test "Old session not into idle pool at checkin" do
    session = RatchetWrench.SessionPool.checkout()
    old_datetime = DateTime.utc_now() |> DateTime.add(-3600, :second)
    old_session = Map.merge(session, %{approximateLastUseTime: old_datetime, createTime: old_datetime})

    RatchetWrench.SessionPool.checkin(old_session)

    new_pool = RatchetWrench.SessionPool.pool()

    session_name_list = Enum.reduce(new_pool.checkout, [], fn(session, acc) ->
                          acc ++ [session.name]
                        end)
    assert Enum.any?(session_name_list, fn(name) -> name == old_session.name end) == false
  end

  test "session bust at interval" do
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 3

    session1 = RatchetWrench.SessionPool.checkout()

    Process.sleep(3000) # Wait session bust(session batch create for idle pool) by interval monitor

    RatchetWrench.SessionPool.checkin(session1)

    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 13

    RatchetWrench.SessionPool.delete_over_idle_sessions()

    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 3
  end

  test "create new session at empty idle session in pool" do
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 3
    session1 = RatchetWrench.SessionPool.checkout()
    session2 = RatchetWrench.SessionPool.checkout()
    session3 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 0

    session4 = RatchetWrench.SessionPool.checkout() # idle empty and create new session

    RatchetWrench.SessionPool.checkin(session1)
    RatchetWrench.SessionPool.checkin(session2)
    RatchetWrench.SessionPool.checkin(session3)
    RatchetWrench.SessionPool.checkin(session4)

    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 4

    RatchetWrench.SessionPool.delete_over_idle_sessions()
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 3
  end

  test "Status at checkout/checkin" do
    session_a = RatchetWrench.SessionPool.checkout()

    pool = RatchetWrench.SessionPool.pool
    assert Enum.count(pool.idle) == 2
    assert Enum.count(pool.checkout) == 1

    session_b = RatchetWrench.SessionPool.checkout()
    pool = RatchetWrench.SessionPool.pool
    assert Enum.count(pool.idle) == 1
    assert Enum.count(pool.checkout) == 2

    RatchetWrench.SessionPool.checkin(session_a)
    pool = RatchetWrench.SessionPool.pool
    assert Enum.count(pool.idle) == 2
    assert Enum.count(pool.checkout) == 1

    RatchetWrench.SessionPool.checkin(session_b)
    pool = RatchetWrench.SessionPool.pool
    assert Enum.count(pool.idle) == 3
    assert Enum.count(pool.checkout) == 0
  end

  test "Update approximate last use time for old session at checkin" do
    session = RatchetWrench.SessionPool.checkout()
    session_name = session.name

    {:ok, datetime, 0} = DateTime.from_iso8601("2015-01-23 23:50:07Z")
    old_session = Map.merge(session, %{approximateLastUseTime: datetime})

    Process.sleep(1000)

    RatchetWrench.SessionPool.checkin(old_session)
    pool = RatchetWrench.SessionPool.pool

    updated_session = List.last(pool.idle)
    assert session_name == updated_session.name

    time_diff = DateTime.diff(updated_session.approximateLastUseTime, old_session.approximateLastUseTime)
    assert time_diff > 0
    assert updated_session.__struct__ == GoogleApi.Spanner.V1.Model.Session
  end

  test "Batch create many sessions" do
    sessions_list = RatchetWrench.SessionPool.session_batch_create(95)
    assert Enum.count(sessions_list) == 95

    Enum.each(sessions_list, fn(x) -> RatchetWrench.SessionPool.delete_session(x) end)

    sessions_list = RatchetWrench.SessionPool.session_batch_create(1122)
    assert Enum.count(sessions_list) == 1122

    Enum.each(sessions_list, fn(x) -> RatchetWrench.SessionPool.delete_session(x) end)
  end

  # test "loop replace sessions in pool" do
  #   loop()
  # end

  # def loop do
  #   session = RatchetWrench.SessionPool.checkout()
  #   RatchetWrench.ping(session)
  #   session2 = RatchetWrench.SessionPool.checkout()
  #   RatchetWrench.ping(session2)

  #   RatchetWrench.SessionPool.checkin(session)
  #   RatchetWrench.SessionPool.checkin(session2)
  #   loop()
  # end
end
