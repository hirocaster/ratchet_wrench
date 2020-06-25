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
    session1 = RatchetWrench.SessionPool.checkout()
    session2 = RatchetWrench.SessionPool.checkout()
    session3 = RatchetWrench.SessionPool.checkout()
    session4 = RatchetWrench.SessionPool.checkout() # idle empty and create new session
    Process.sleep(3000) # wait bust(session batch create)
    RatchetWrench.SessionPool.checkin(session1)
    RatchetWrench.SessionPool.checkin(session2)
    RatchetWrench.SessionPool.checkin(session3)
    RatchetWrench.SessionPool.checkin(session4)

    assert Enum.count(RatchetWrench.SessionPool.pool.idle) == 14
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
