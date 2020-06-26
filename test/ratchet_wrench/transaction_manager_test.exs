defmodule RatchetWrench.TransactionManagerTest do
  use ExUnit.Case

  setup_all do
    start_supervised({RatchetWrench.SessionPool, %RatchetWrench.Pool{}})
    :ok
  end

  test "Create transaction in not exist transaction manager" do
    session = RatchetWrench.SessionPool.checkout()

    assert RatchetWrench.TransactionManager.get_keys() == [:rand_seed] # default key ???

    transaction = RatchetWrench.TransactionManager.begin(session)

    assert transaction.__struct__ == RatchetWrench.Transaction
    assert transaction.seqno == 1
    assert transaction.session == session
    assert transaction.transaction.__struct__ == GoogleApi.Spanner.V1.Model.Transaction

    keys = RatchetWrench.TransactionManager.get_keys()
    assert transaction.session.name in keys

    RatchetWrench.SessionPool.checkin(session)
  end

  test "Commit transaction" do
    session = RatchetWrench.SessionPool.checkout()
    transaction = RatchetWrench.TransactionManager.begin(session)

    {:ok, commit_response} = RatchetWrench.TransactionManager.commit(transaction)
    assert commit_response.__struct__ == GoogleApi.Spanner.V1.Model.CommitResponse

    keys = RatchetWrench.TransactionManager.get_keys()
    assert (transaction.session.name in keys) == false

    RatchetWrench.SessionPool.checkin(session)
  end

  test "Increment seqno in exist transaction manager" do
    session = RatchetWrench.SessionPool.checkout()
    transaction_1st = RatchetWrench.TransactionManager.begin(session)
    transaction_2nd = RatchetWrench.TransactionManager.begin(session)

    assert transaction_2nd.seqno == 2
    assert transaction_1st.transaction == transaction_2nd.transaction
    assert transaction_1st.transaction.id == transaction_2nd.transaction.id
    assert transaction_1st.session == transaction_2nd.session

    keys = RatchetWrench.TransactionManager.get_keys()
    assert transaction_2nd.session.name in keys

    {:ok, _commit_response} = RatchetWrench.TransactionManager.commit(transaction_2nd)

    keys = RatchetWrench.TransactionManager.get_keys()
    assert transaction_2nd.session.name in keys == false

    RatchetWrench.SessionPool.checkin(session)
  end
end
