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

    transaction2 = RatchetWrench.TransactionManager.begin()
    assert transaction2.seqno == 2
    transaction3 = RatchetWrench.TransactionManager.begin()
    assert transaction3.seqno == 3
    transaction4 = RatchetWrench.TransactionManager.begin()
    assert transaction4.seqno == 4
    transaction5 = RatchetWrench.TransactionManager.begin()
    assert transaction5.seqno == 5


    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys
  end

  test "Commit transaction" do
    transaction = RatchetWrench.TransactionManager.begin()

    RatchetWrench.TransactionManager.delete_key()

    {:ok, commit_response} = RatchetWrench.TransactionManager.commit(transaction)
    assert commit_response.__struct__ == GoogleApi.Spanner.V1.Model.CommitResponse

    keys = RatchetWrench.TransactionManager.get_keys()
    assert (self() in keys) == false
  end

  test "Increment seqno in exist transaction manager" do
    transaction_1st = RatchetWrench.TransactionManager.begin()
    transaction_2nd = RatchetWrench.TransactionManager.begin()

    assert transaction_2nd.seqno == 2
    assert transaction_1st.transaction == transaction_2nd.transaction
    assert transaction_1st.transaction.id == transaction_2nd.transaction.id
    assert transaction_1st.session == transaction_2nd.session

    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys

    RatchetWrench.TransactionManager.delete_key()
    {:ok, _commit_response} = RatchetWrench.TransactionManager.commit(transaction_2nd)

    keys = RatchetWrench.TransactionManager.get_keys()
    assert self() in keys == false
  end
end
