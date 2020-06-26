defmodule RatchetWrench.TransactionManager do
  def begin(session) do
    key = session.name
    transaction = Process.get(key)

    if transaction == nil do
      transaction = _begin_transaction(session)
      Process.put(key, transaction)
      transaction
    else
      seqno = transaction.seqno
      Map.merge(transaction, %{seqno: seqno + 1})
    end
  end

  def _begin_transaction(session) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    {:ok, cloudspanner_transaction} = RatchetWrench.begin_transaction(connection, session)
    %RatchetWrench.Transaction{session: session, transaction: cloudspanner_transaction}
  end

  def commit(transaction) do
    {:ok, commit_response} = _commit_transaction(transaction)
    key = transaction.session.name
    Process.delete(key)
    {:ok, commit_response}
  end

  def _commit_transaction(transaction) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = transaction.session
    RatchetWrench.commit_transaction(connection, session, transaction.transaction)
  end

  def get_keys() do
    Process.get_keys()
  end
end
