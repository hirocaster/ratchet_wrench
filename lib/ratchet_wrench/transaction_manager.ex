defmodule RatchetWrench.TransactionManager do
  def begin() do
    key = self()
    transaction = Process.get(key)

    if transaction == nil do
      transaction = begin_transaction()
      put_transaction(transaction)
      transaction
    else
      seqno = transaction.seqno
      transaction = transaction
                    |> Map.merge(%{seqno: seqno + 1})
      put_transaction(transaction)
      transaction
    end
  end

  def skip_countup_begin_transaction(transaction) do
    transaction = Map.merge(transaction, %{skip: transaction.skip + 1})
    put_transaction(transaction)
    transaction
  end

  def skip_countdown_begin_transaction(transaction) do
    if transaction.skip > 0 do
      transaction = Map.merge(transaction, %{skip: transaction.skip - 1})
      put_transaction(transaction)
      transaction
    else
      transaction
    end
  end

  defp begin_transaction() do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = RatchetWrench.SessionPool.checkout()

    {:ok, cloudspanner_transaction} = RatchetWrench.begin_transaction(connection, session)
    %RatchetWrench.Transaction{session: session, transaction: cloudspanner_transaction}
  end

  def exist_transaction?() do
    if get_transaction() == nil do
      false
    else
      true
    end
  end

  defp get_transaction() do
    key = self()
    Process.get(key)
  end

  defp put_transaction(transaction) do
    key = self()
    Process.put(key, transaction)
  end

  defp delete_transaction() do
    key = self()
    Process.delete(key)
  end

  def rollback() do
    if exist_transaction?() do
      {:ok, empty} = rollback_transaction()
      delete_transaction()
      {:ok, empty}
    else
      {:error, "not found transaction"}
    end
  end

  defp rollback_transaction() do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    transaction = get_transaction()
    session = transaction.session
    RatchetWrench.rollback_transaction(connection, session, transaction.transaction)
  end

  def commit() do
    transaction = get_transaction()

    if transaction.skip == 0 do
      {:ok, commit_response} = commit_transaction(transaction)
      delete_transaction()
      RatchetWrench.SessionPool.checkin(transaction.session)
      {:ok, commit_response}
    else
      put_transaction(transaction)
      {:ok, :skip}
    end
  end

  defp commit_transaction(transaction) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = transaction.session
    RatchetWrench.commit_transaction(connection, session, transaction.transaction)
  end

  def get_keys() do
    Process.get_keys()
  end
end
