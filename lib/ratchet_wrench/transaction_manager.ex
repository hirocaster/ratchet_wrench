defmodule RatchetWrench.TransactionManager do
  def begin() do
    key = self()
    transaction = Process.get(key)

    if transaction == nil do
      transaction = _begin_transaction()
      Process.put(key, transaction)
      transaction
    else
      seqno = transaction.seqno
      transaction = Map.merge(transaction, %{seqno: seqno + 1})
      Process.put(key, transaction)
      transaction
    end
  end

  def _begin_transaction() do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = RatchetWrench.SessionPool.checkout()

    {:ok, cloudspanner_transaction} = RatchetWrench.begin_transaction(connection, session)
    %RatchetWrench.Transaction{session: session, transaction: cloudspanner_transaction}
  end

  def exist_transaction?() do
    key = self()
    transaction = Process.get(key)
    if transaction == nil do
      false
    else
      true
    end
  end

  def rollback(transaction) do
    if exist_transaction?() do
      {:ok, empty} = _rollback_transaction(transaction)
      {:ok, empty}
    else
      {:error, "not found transaction"}
    end
  end

  def _rollback_transaction(transaction) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = transaction.session
    RatchetWrench.rollback_transaction(connection, session, transaction.transaction)
  end


  def delete_key() do
    key = self()
    Process.delete(key)
  end

  def commit(transaction) do
    {:ok, commit_response} = _commit_transaction(transaction)

    key = self()
    if Process.get(key) == nil do
      RatchetWrench.SessionPool.checkin(transaction.session)
      {:ok, commit_response}
    else
      {:ok, nil}
    end
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
