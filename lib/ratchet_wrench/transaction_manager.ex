defmodule RatchetWrench.TransactionManager do
  def get_or_begin_transaction() do
    case get_transaction() do
      nil -> begin()
      transaction -> transaction
    end
  end

  def countup_seqno(transaction) do
      seqno = transaction.seqno
      transaction = transaction
                    |> Map.merge(%{seqno: seqno + 1})
      put_transaction(transaction)
      transaction
  end

  def begin!() do
    case begin() do
      {:ok, transaction} -> transaction
      {:error, err} -> raise err
    end
  end

  def begin() do
    transaction = get_transaction()

    if transaction == nil do
      case begin_transaction() do
        {:ok, transaction} ->
          put_transaction(transaction)
          {:ok, transaction}
        {:error, err} -> {:error, err}
      end
    else
      {:ok, transaction}
    end
  end

  defp begin_transaction() do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = RatchetWrench.SessionPool.checkout()

    if session == :error do
      {:error, %RatchetWrench.Exception.EmptyIdleSessionAndMaxSession{}}
    else
      case RatchetWrench.begin_transaction(connection, session) do
        {:ok, cloudspanner_transaction} ->
          {:ok, %RatchetWrench.Transaction{session: session, transaction: cloudspanner_transaction}}
        {:error, err} -> {:error, err}
      end
    end
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

  def delete_transaction() do
    checkin_for_session_pool()
    key = self()
    Process.delete(key)
  end

  defp checkin_for_session_pool() do
    transaction = get_transaction()
    RatchetWrench.SessionPool.checkin(transaction.session)
  end

  defp update_approximate_last_use_time_in_transaction() do
    transaction = get_transaction()
    updated_session = RatchetWrench.SessionPool.only_update_approximate_last_use_time_from_now(transaction.session)
    updated_transaction = Map.merge(transaction, %{session: updated_session})
    put_transaction(updated_transaction)
    updated_transaction
  end

  def rollback() do
    if exist_transaction?() do
      result = rollback_transaction()

      delete_transaction()

      result
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

  def commit!() do
    case commit() do
      {:ok, commit_response} -> commit_response
      {:error, err} -> raise err
    end
  end

  def commit() do
    transaction = get_transaction()

    case commit_transaction(transaction) do
      {:ok, commit_response} ->
        update_approximate_last_use_time_in_transaction()
        delete_transaction()
        {:ok, commit_response}
      {:error, err} -> {:error, err}
    end
  end

  defp commit_transaction(transaction) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()
    session = transaction.session
    case RatchetWrench.commit_transaction(connection, session, transaction.transaction) do
      {:ok, commit_response} -> {:ok, commit_response}
      {:error, err} -> {:error, err}
    end
  end

  def get_keys() do
    Process.get_keys()
  end
end
