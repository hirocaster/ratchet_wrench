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
      transaction = transaction
                    |> skip_countup_begin_transaction()
                    |> Map.merge(%{seqno: seqno + 1})
      Process.put(key, transaction)
      transaction
    end
  end

  defp skip_countup_begin_transaction(transaction) do
    Map.merge(transaction, %{skip: transaction.skip + 1})
  end

  defp skip_countdown_begin_transaction(transaction) do
    if transaction.skip > 0 do
      Map.merge(transaction, %{skip: transaction.skip - 1})
    else
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

  def rollback() do
    if exist_transaction?() do
      {:ok, empty} = rollback_transaction()
      delete_key()
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


  defp delete_key() do
    key = self()
    Process.delete(key)
  end

  def commit() do
    transaction = get_transaction()
                  |> skip_countdown_begin_transaction()
    key = self()

    if transaction.skip == 0 do
      {:ok, commit_response} = _commit_transaction(transaction)
      delete_key()
      RatchetWrench.SessionPool.checkin(transaction.session)
      {:ok, commit_response}
    else
      Process.put(key, transaction)
      {:ok, :skip}
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
