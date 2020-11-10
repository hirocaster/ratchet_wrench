defmodule RatchetWrench.SessionPoolTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  setup_all do
    session_min = RatchetWrench.SessionPool.session_min()
    System.put_env("RATCHET_WRENCH_SESSION_MIN", "3")

    session_max = RatchetWrench.SessionPool.session_max()
    System.put_env("RATCHET_WRENCH_SESSION_MAX", "15")

    session_monitor_interval = RatchetWrench.SessionPool.session_monitor_interval()
    System.put_env("RATCHET_WRENCH_SESSION_MONITOR_INTERVAL", "2000") # 2sec

    session_bust_num = RatchetWrench.SessionPool.session_bust_num()
    System.put_env("RATCHET_WRENCH_SESSION_BUST", "10")

    on_exit fn ->
      System.put_env("RATCHET_WRENCH_SESSION_MIN", session_min  |> Integer.to_string())
      System.put_env("RATCHET_WRENCH_SESSION_MAX", session_max  |> Integer.to_string())
      System.put_env("RATCHET_WRENCH_SESSION_MONITOR_INTERVAL", session_monitor_interval |> Integer.to_string())
      System.put_env("RATCHET_WRENCH_SESSION_BUST", session_bust_num |> Integer.to_string())
    end
  end

  setup do
    {:ok, _pid} = start_supervised({RatchetWrench.SessionPool, %RatchetWrench.Pool{}})

    on_exit fn ->
      nil
    end
  end

  test "Check setup config" do
    assert RatchetWrench.SessionPool.session_min() == 3
    assert RatchetWrench.SessionPool.session_max() == 15
    assert RatchetWrench.SessionPool.session_monitor_interval() == 2000
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

  test "FILO in session pool" do
    pool = RatchetWrench.SessionPool.pool()
    first_idle_session = List.first(pool.idle)

    session1 = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session1)

    pool = RatchetWrench.SessionPool.pool()
    last_idle_session = List.last(pool.idle)
    assert first_idle_session.name == last_idle_session.name

    session2 = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session2)

    pool = RatchetWrench.SessionPool.pool()
    last_idle_session = List.last(pool.idle)
    assert session2.name == last_idle_session.name

    refute session1.name == session2.name

    session3 = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session3)

    refute session1.name == session3.name
    refute session2.name == session3.name

    reuse_session1 = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(reuse_session1)

    assert session1.name == reuse_session1.name
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

  test "Session bust at empty idle session" do
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 3
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 0

    session1 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 1
    session2 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 2
    session3 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 3
    session4 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 9
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 4
    session5 = RatchetWrench.SessionPool.checkout()
    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 8
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 5
    session6 = RatchetWrench.SessionPool.checkout()
    session7 = RatchetWrench.SessionPool.checkout()
    session8 = RatchetWrench.SessionPool.checkout()
    session9 = RatchetWrench.SessionPool.checkout()
    session10 = RatchetWrench.SessionPool.checkout()
    session11 = RatchetWrench.SessionPool.checkout()
    session12 = RatchetWrench.SessionPool.checkout()
    session13 = RatchetWrench.SessionPool.checkout()
    session14 = RatchetWrench.SessionPool.checkout()
    session15 = RatchetWrench.SessionPool.checkout()

    assert capture_log(fn ->
      session16 = RatchetWrench.SessionPool.checkout()
      assert :error == session16
    end) =~ "Empty idle session and max session pool."

    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 15

    RatchetWrench.SessionPool.checkin(session1)
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 14
    RatchetWrench.SessionPool.checkin(session2)
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 13
    RatchetWrench.SessionPool.checkin(session3)
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 12
    RatchetWrench.SessionPool.checkin(session4)
    RatchetWrench.SessionPool.checkin(session5)
    RatchetWrench.SessionPool.checkin(session6)
    RatchetWrench.SessionPool.checkin(session7)
    RatchetWrench.SessionPool.checkin(session8)
    RatchetWrench.SessionPool.checkin(session9)
    RatchetWrench.SessionPool.checkin(session10)
    RatchetWrench.SessionPool.checkin(session11)
    RatchetWrench.SessionPool.checkin(session12)
    RatchetWrench.SessionPool.checkin(session13)
    RatchetWrench.SessionPool.checkin(session14)
    RatchetWrench.SessionPool.checkin(session15)

    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 15
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 0
  end

  test "session bust at all expired sessions" do
    empty_pool = %RatchetWrench.Pool{}
    assert Enum.count(empty_pool.idle) == 0
    assert Enum.count(empty_pool.checkout) == 0

    busted_pool = RatchetWrench.SessionPool.session_bust(empty_pool)
    assert Enum.count(busted_pool.idle) == 10
    assert Enum.count(busted_pool.checkout) == 0
  end

  test "should_session_bust?/1" do
    should_bust_pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3]}
    assert RatchetWrench.SessionPool.should_session_bust?(should_bust_pool)

    should_bust_pool = %RatchetWrench.Pool{idle: [3], checkout: [1, 2]}
    assert RatchetWrench.SessionPool.should_session_bust?(should_bust_pool)

    should_bust_pool = %RatchetWrench.Pool{idle: [2, 3, 4, 5], checkout: [1]}
    assert RatchetWrench.SessionPool.should_session_bust?(should_bust_pool)

    should_bust_pool = %RatchetWrench.Pool{idle: [3, 4, 5, 6, 7, 8, 9, 10], checkout: [1, 2]}
    assert RatchetWrench.SessionPool.should_session_bust?(should_bust_pool)

    should_not_bust_pool = %RatchetWrench.Pool{idle: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], checkout: []}
    refute RatchetWrench.SessionPool.should_session_bust?(should_not_bust_pool)

    should_not_bust_pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]}
    refute RatchetWrench.SessionPool.should_session_bust?(should_not_bust_pool)

    should_not_bust_pool = %RatchetWrench.Pool{idle: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], checkout: [1, 2]}
    refute RatchetWrench.SessionPool.should_session_bust?(should_not_bust_pool)

    should_not_bust_pool = %RatchetWrench.Pool{idle: [2, 3, 4, 5, 6, 7, 8, 9, 10], checkout: [1]}
    refute RatchetWrench.SessionPool.should_session_bust?(should_not_bust_pool)

    should_not_bust_pool = %RatchetWrench.Pool{idle: [1, 2, 3], checkout: []}
    refute RatchetWrench.SessionPool.should_session_bust?(should_not_bust_pool)
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

  test "session_batch_create/1" do
    assert [] == RatchetWrench.SessionPool.session_batch_create(nil)
    assert [] == RatchetWrench.SessionPool.session_batch_create(0)
  end

  test "calculation_session_bust_num/1" do
    pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3]}
    assert RatchetWrench.SessionPool.calculation_session_bust_num(pool) == 10

    pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
    assert RatchetWrench.SessionPool.calculation_session_bust_num(pool) == 5

    pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]}
    assert RatchetWrench.SessionPool.calculation_session_bust_num(pool) == 0

    pool = %RatchetWrench.Pool{idle: [], checkout: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]} # Bug over sessions
    assert RatchetWrench.SessionPool.calculation_session_bust_num(pool) == 0
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
