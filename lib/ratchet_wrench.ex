defmodule RatchetWrench do
  @moduledoc """
  RatchetWrench is a easily use Google Cloud Spanner by Elixir.
  """

  require Logger

  def execute() do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    {:ok, session} = RatchetWrench.Session.create(connection)
    json = %{sql: "SELECT 1"}
    {:ok, result_set} = GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_execute_sql(connection, session.name, [{:body, json}])
    {:ok, _} = RatchetWrench.Session.delete(connection, session)
    {:ok, result_set}
  end

  def token() do
    case Goth.Token.for_scope(token_scope()) do
      {:ok, token} -> token
      {:error, reason} -> {:error, reason}
    end
  end

  def token_data() do
    scope = "https://www.googleapis.com/auth/spanner.data"
    case Goth.Token.for_scope(scope) do
      {:ok, token} -> token
      {:error, reason} -> {:error, reason}
    end
  end

  def token_admin() do
    scope = "https://www.googleapis.com/auth/spanner.admin"
    case Goth.Token.for_scope(scope) do
      {:ok, token} -> token
      {:error, reason} -> {:error, reason}
    end
  end

  def token_scope() do
    System.get_env("RATCHET_WRENCH_TOKEN_SCOPE") || "https://www.googleapis.com/auth/spanner.data"
  end

  def connection(token) do
    GoogleApi.Spanner.V1.Connection.new(token.token)
  end

  def create_session(connection) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_create(connection, database()) do
      {:ok, session} -> session
      {:error, _} -> raise "Database config error. Check env `RATCHET_WRENCH_DATABASE` or config"
    end
  end

  def batch_create_session(connection, session_count) do
    json = %{sessionCount: session_count}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_batch_create(connection, database(), [{:body, json}]) do
      {:ok, response} -> response.session
      {:error, _reason} ->
        raise "Database config error. Check env `RATCHET_WRENCH_DATABASE` or config"
    end
  end

  def delete_session(connection, session) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_delete(connection, session.name) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  def update_ddl(ddl_list) do
    connection = RatchetWrench.token_admin() |> RatchetWrench.connection()
    json = %{statements: ddl_list}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_update_ddl(connection, database(), [{:body, json}]) do
      {:ok, operation} -> {:ok, operation}
      {:error, reason} -> {:error, Poison.Parser.parse!(reason.body, %{})}
    end
  end

  def database() do
    System.get_env("RATCHET_WRENCH_DATABASE") || Application.fetch_env(:ratchet_wrench, :database)
  end

  def begin_transaction(connection, session) do
    json = %{options: %{readWrite: %{}} }
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_begin_transaction(connection, session.name, [{:body, json}]) do
      {:ok, transaction} -> {:ok, transaction}
      {:error, reason} -> {:error, Poison.Parser.parse!(reason.body, %{})}
    end
  end

  def rollback_transaction(connection, session, transaction) do
    json = %{transactionId: transaction.id}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_rollback(connection, session.name, [{:body, json}]) do
      {:ok, empty} -> {:ok, empty}
      {:error, reason} -> {:error, Poison.Parser.parse!(reason.body, %{})}
    end
  end

  def commit_transaction(connection, session, transaction) do
    RatchetWrench.Logger.info("Commit transaction request, transaction_id: #{transaction.id}")
    json = %{transactionId: transaction.id}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_commit(connection, session.name, [{:body, json}]) do
      {:ok, commit_response} ->
        RatchetWrench.Logger.info("Commited transaction, transaction_id: #{transaction.id}, time_stamp: #{commit_response.commitTimestamp}")
        {:ok, commit_response}
      {:error, reason} -> {:error, Poison.Parser.parse!(reason.body, %{})}
    end
  end

  def select_execute_sql(sql, params) do
    json = %{sql: sql, params: params}
    connection = RatchetWrench.token |> RatchetWrench.connection
    session = RatchetWrench.SessionPool.checkout()

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_execute_sql(connection, session.name, [{:body, json}]) do
      {:ok, result_set} ->
        RatchetWrench.SessionPool.checkin(session)
        {:ok, result_set}
      {:error, client} ->
        RatchetWrench.SessionPool.checkin(session)
        request_api_error(client)
    end
  end

  def request_api_error(api_client) do
    try do
      raise RatchetWrench.Exception.APIRequestError, api_client
    rescue
      err in _ ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  def execute_sql(sql, params, param_types) when is_binary(sql) and is_map(params) do
    if RatchetWrench.TransactionManager.exist_transaction?() do
      case do_execute_sql(sql, params, param_types) do
        {:ok, result_set} -> {:ok, result_set}
        {:error, client} -> request_api_error(client)
      end
    else
      transaction fn ->
        case do_execute_sql(sql, params, param_types) do
          {:ok, result_set} -> {:ok, result_set}
          {:error, client} -> request_api_error(client)
        end
      end
    end
  end

  def do_execute_sql(sql, params, param_types) do
    connection = RatchetWrench.token |> RatchetWrench.connection
    transaction = RatchetWrench.TransactionManager.get_or_begin_transaction()

    session = transaction.session

    json = %{seqno: transaction.seqno,
             transaction: %{id: transaction.transaction.id},
             sql: sql,
             params: params,
             paramTypes: param_types}

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_execute_sql(connection, session.name, [{:body, json}]) do
      {:ok, result_set} ->
        RatchetWrench.TransactionManager.countup_seqno(transaction)
        {:ok, result_set}
      {:error, client} -> {:error, client}
    end
  end

  def auto_limit_offset_execute_sql(sql, params, params_type, limit \\ 1_000_000) do
    {:ok, result_set_list} = do_auto_limit_offset_execute_sql(sql, params, params_type, limit)
    {:ok, result_set_list}
  end

  def do_auto_limit_offset_execute_sql(sql, params, params_type, limit, offset \\ 0, seqno \\ 1, acc \\ []) do
    limit_offset_sql = sql <> " LIMIT #{limit} OFFSET #{offset}"

    if RatchetWrench.TransactionManager.exist_transaction?() do
      case execute_sql(limit_offset_sql, params, params_type) do
        {:ok, result_set} ->
          do_auto_limit_offset_next_execute_sql(result_set, sql, params, params_type, limit, offset, seqno, acc)
        {:error, exception} ->
          do_result_set_to_large(exception, sql, params, params_type, limit)
      end
    else
      case select_execute_sql(limit_offset_sql, params) do
        {:ok, result_set} ->
          do_auto_limit_offset_next_execute_sql(result_set, sql, params, params_type, limit, offset, seqno, acc)
        {:error, exception} ->
          do_result_set_to_large(exception, sql, params, params_type, limit)
      end
    end
  end

  defp do_auto_limit_offset_next_execute_sql(result_set, sql, params, params_type, limit, offset, seqno, acc) do
    if result_set.rows == nil do
      {:ok, []}
    else
      result_set_list = acc ++ [result_set]
      if limit == Enum.count(result_set.rows) do
        offset = offset + limit
        do_auto_limit_offset_execute_sql(sql, params, params_type, limit, offset, seqno + 1, result_set_list)
      else
        {:ok, result_set_list}
      end
    end
  end

  @too_large_error_message "Result set too large. Result sets larger than 10.00M can only be yielded through the streaming API."
  defp do_result_set_to_large(exception, sql, params, params_type, limit) do
    try do
      reason = Poison.Parser.parse!(exception.client.body, %{})
      if reason["error"]["message"] == @too_large_error_message do
        limit = div(limit, 2)
        auto_limit_offset_execute_sql(sql, params, params_type, limit)
      else
        raise exception
      end
    rescue
      err in _ ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        {:error, err}
    end
  end

  def transaction!(callback) when is_function(callback) do
    case transaction(callback) do
      {:ok, callback_result} -> callback_result
      {:error, e} -> raise e
    end
  end

  def transaction(callback) when is_function(callback) do
    if RatchetWrench.TransactionManager.exist_transaction? do
      callback.()
    else
      try do
        RatchetWrench.TransactionManager.begin()
        result = callback.()

        case RatchetWrench.TransactionManager.commit() do
          {:ok, _commit_response} -> result
          {:error, err} -> {:error, err}
        end
      rescue
        err in _ ->
          Logger.error(Exception.format(:error, err, __STACKTRACE__))
          if RatchetWrench.TransactionManager.exist_transaction? do
            RatchetWrench.TransactionManager.rollback()
          end
          {:error, err}
      end
    end
  end
end
