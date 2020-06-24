defmodule RatchetWrench.SessionPoolTest do
  use ExUnit.Case

  setup_all do
    children = [
      {RatchetWrench.SessionPool, %RatchetWrench.Pool{}},
      {Mutex, name: RatchetWrenchSessionPoolMutex, meta: :ratchet_wrench_session_pool}
    ]
    {:ok, _pid} = Supervisor.start_link(children, strategy: :one_for_one)
    :ok
  end

  test "async .checkout() / .checkin()" do
    session = RatchetWrench.SessionPool.checkout()
    assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
    RatchetWrench.SessionPool.checkin(session)

    0..3
    |> Enum.map(fn(_) ->
      Task.async(fn ->
        session = RatchetWrench.SessionPool.checkout()
        assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
        RatchetWrench.SessionPool.checkin(session)
      end)
      # session = RatchetWrench.SessionPool.checkout()
      # assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
      # RatchetWrench.SessionPool.checkin(session)
    end)
    |> Enum.map(&Task.await &1, 60000)
  end

  test "FIFO in session pool" do
    session = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session)
    pool = RatchetWrench.SessionPool.pool()
    assert session.name == List.last(pool.idle).name
  end

  # test "Update approximateLastUseTime at checkin" do
  #   session = RatchetWrench.SessionPool.checkout()
  #   RatchetWrench.ping(session)
  #   RatchetWrench.SessionPool.checkin(session)
  #   pool = RatchetWrench.SessionPool.pool()
  #   assert DateTime.diff(List.last(pool).approximateLastUseTime, session.approximateLastUseTime) > 0
  # end

  test ".is_safe_session?" do
    session = RatchetWrench.SessionPool.checkout()
    RatchetWrench.SessionPool.checkin(session)
    assert RatchetWrench.SessionPool.is_safe_session?(session)
  end

  test "Replace old session at checkin" do
    session = RatchetWrench.SessionPool.checkout()
    old_datetime = DateTime.utc_now() |> DateTime.add(-3600, :second)
    old_session = Map.merge(session, %{approximateLastUseTime: old_datetime, createTime: old_datetime})

    Process.sleep(1000)
    RatchetWrench.SessionPool.checkin(old_session)

    new_pool = RatchetWrench.SessionPool.pool()
    new_session = List.last(new_pool.idle).approximateLastUseTime

    assert DateTime.diff(new_session, session.approximateLastUseTime) > 0
    assert session.name != new_session
    assert Enum.count(new_pool.idle) == 3
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
