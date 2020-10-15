defmodule RatchetWrench.TransactionManagerTest do
  use ExUnit.Case

  setup_all do
    start_supervised({RatchetWrench.SessionPool, %RatchetWrench.Pool{}})
    :ok
  end

  test "Create transaction in not exist transaction manager" do
    assert RatchetWrench.TransactionManager.get_keys() == [:rand_seed] # default key ???

    transaction = RatchetWrench.TransactionManager.begin()

    assert transaction.__struct__ == RatchetWrench.Transaction
    assert transaction.seqno == 1
    assert transaction.transaction.__struct__ == GoogleApi.Spanner.V1.Model.Transaction

    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys
  end

  test "Commit transaction" do
    RatchetWrench.TransactionManager.begin()

    pool = RatchetWrench.SessionPool.pool()
    before_session = List.first(pool.checkout)

    {:ok, commit_response} = RatchetWrench.TransactionManager.commit()
    assert commit_response.__struct__ == GoogleApi.Spanner.V1.Model.CommitResponse

    pool = RatchetWrench.SessionPool.pool()
    after_session = List.last(pool.idle)

    assert before_session.name == after_session.name

    time_diff = DateTime.diff(after_session.approximateLastUseTime, before_session.approximateLastUseTime)

    refute before_session.approximateLastUseTime == after_session.approximateLastUseTime
    assert time_diff > 0

    keys = RatchetWrench.TransactionManager.get_keys()
    assert (self() in keys) == false
  end

  test "Return equal transaction at begin()" do
    transaction_1st = RatchetWrench.TransactionManager.begin()
    transaction_2nd = RatchetWrench.TransactionManager.begin()

    assert transaction_1st.transaction == transaction_2nd.transaction
    assert transaction_1st.transaction.id == transaction_2nd.transaction.id
    assert transaction_1st.session == transaction_2nd.session

    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys

    {:ok, _commit_response} = RatchetWrench.TransactionManager.commit()

    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys == false
  end
end
