defmodule RatchetWrench do
  @moduledoc """
  RatchetWrench is a easily use Google Cloud Spanner by Elixir.
  """

  @retry_count_limit 3
  @retry_wait_time 1000 # 1sec
  @do_retry_http_status_code 409

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

  @doc "Create max 100 sessions at 1 request in API"
  def batch_create_session(connection, session_count) when is_number(session_count) do
    if session_count > 0 do
      json = %{sessionCount: session_count}
      case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_batch_create(connection, database(), [{:body, json}]) do
        {:ok, response} -> response.session
        {:error, _reason} ->
          raise "Database config error. Check env `RATCHET_WRENCH_DATABASE` or config"
      end
    else
      []
    end
  end

  def delete_session(connection, session) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_delete(connection, session.name) do
      {:ok, result} -> {:ok, result}
      {:error, client} -> request_api_error(client)
    end
  end

  def update_ddl(ddl_list) do
    connection = RatchetWrench.token_admin() |> RatchetWrench.connection()
    json = %{statements: ddl_list}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_update_ddl(connection, database(), [{:body, json}]) do
      {:ok, operation} -> {:ok, operation}
      {:error, client} -> request_api_error(client)
    end
  end

  def database() do
    System.get_env("RATCHET_WRENCH_DATABASE") || Application.fetch_env(:ratchet_wrench, :database)
  end

  def project_id() do
    {project_id, _other} = Regex.split(~r{/}, RatchetWrench.database()) |> List.pop_at(1)
    project_id
  end

  def instance_id() do
    {instance_id, _other} = Regex.split(~r{/}, RatchetWrench.database()) |> List.pop_at(3)
    instance_id
  end

  def database_id() do
    {database_id, _other} =Regex.split(~r{/}, RatchetWrench.database()) |> List.pop_at(5)
    database_id
  end

  def begin_transaction(connection, session) do
    json = %{options: %{readWrite: %{}} }
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_begin_transaction(connection, session.name, [{:body, json}]) do
      {:ok, transaction} -> {:ok, transaction}
      {:error, client} -> request_api_error(client)
    end
  end

  def rollback_transaction(connection, session, transaction) do
    json = %{transactionId: transaction.id}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_rollback(connection, session.name, [{:body, json}]) do
      {:ok, empty} -> {:ok, empty}
      {:error, client} -> request_api_error(client)
    end
  end

  def commit_transaction(connection, session, transaction) do
    RatchetWrench.Logger.info("Commit transaction request, transaction_id: #{transaction.id}")
    json = %{transactionId: transaction.id}
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_commit(connection, session.name, [{:body, json}]) do
      {:ok, commit_response} ->
        RatchetWrench.Logger.info("Commited transaction, transaction_id: #{transaction.id}, time_stamp: #{commit_response.commitTimestamp}")
        {:ok, commit_response}
      {:error, client} -> request_api_error(client)
    end
  end

  def select_execute_sql(sql, params) do
    json = %{sql: sql, params: params}
    connection = RatchetWrench.token |> RatchetWrench.connection
    session = RatchetWrench.SessionPool.checkout()

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_execute_sql(connection, session.name, [{:body, json}]) do
      {:ok, result_set} ->
        RatchetWrench.SessionPool.only_update_approximate_last_use_time_from_now(session)
        |> RatchetWrench.SessionPool.checkin()
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
      case transaction(fn -> do_execute_sql(sql, params, param_types) end) do
        {:ok, result} -> result
        {:error, client} -> request_api_error(client)
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

  def transaction!(callback, retry_count \\ 0) when is_function(callback) do
    if RatchetWrench.TransactionManager.exist_transaction? do
      callback.()
    else
      try do
        RatchetWrench.TransactionManager.begin()
        result = callback.()

        case RatchetWrench.TransactionManager.commit() do
          {:ok, _commit_response} -> {:ok, result}
          {:error, err} ->
            if err.client.status == @do_retry_http_status_code do
              retry_transaction(callback, err, retry_count)
            else
              raise err
            end
        end
      rescue
        err in _ ->
          if RatchetWrench.TransactionManager.exist_transaction? do
            case RatchetWrench.TransactionManager.rollback() do
              {:ok, _} -> reraise(err, __STACKTRACE__)
              {:error, rollback_error} ->
                Logger.error(Exception.format(:error, rollback_error, __STACKTRACE__))
                reraise(err, __STACKTRACE__)
            end
          else
            reraise(err, __STACKTRACE__)
          end
      end
    end
  end

  def transaction(callback, retry_count \\ 0) when is_function(callback) do
    if RatchetWrench.TransactionManager.exist_transaction? do
      callback.()
    else
      try do
        RatchetWrench.TransactionManager.begin()
        result = callback.()

        case RatchetWrench.TransactionManager.commit() do
          {:ok, _commit_response} -> {:ok, result}
          {:error, err} ->
            if err.client.status == @do_retry_http_status_code do
              retry_transaction(callback, err, retry_count)
            else
              raise err
            end
        end
      rescue
        err in _ ->
          if RatchetWrench.TransactionManager.exist_transaction? do
            case RatchetWrench.TransactionManager.rollback() do
              {:ok, _} -> {:error, err}
              {:error, rollback_error} ->
                Logger.error(Exception.format(:error, rollback_error, __STACKTRACE__))
                {:error, err}
            end
          else
            {:error, err}
          end
      end
    end
  end

  defp retry_transaction(callback, err, retry_count) when is_function(callback) do
    # TODO: Change loglevel to info
    Logger.error("Retry transaction: { callback: #{inspect callback}, err: #{inspect err}, retry_count: #{retry_count} }")
    if @retry_count_limit >= retry_count  do
      RatchetWrench.TransactionManager.delete_transaction()
      retry_sleep(err, retry_count)
      transaction(callback, retry_count + 1)
    else
      {:error, err}
    end
  end

  defp retry_sleep(err, retry_count) do
    try do
      client = err.client
      if retry_count == 0 do
        json = parse_response_body(client)
        retry_delay_time = retry_delay(json)
        Process.sleep(retry_delay_time)
      else
        Process.sleep(@retry_wait_time)
      end
    rescue
      err in _ ->
        Logger.error(Exception.format(:error, err, __STACKTRACE__))
        Process.sleep(@retry_wait_time)
    end
  end

  defp parse_response_body(client) do
    Poison.Parser.parse!(client.body, %{})
  end

  defp retry_delay(json) when is_map(json) do
    if json["error"]["code"] == 409 do
      if json["error"]["message"] == "Transaction was aborted." do
        detail = List.first(json["error"]["details"])
        if detail["@type"] == "type.googleapis.com/google.rpc.RetryInfo" do
          detail["retryDelay"] # ex) "0.012063175s"
          |> parse_retry_delay()
        end
      end
    end
  end

  defp parse_retry_delay(str) do
    {time_microsecond, _s} = Float.parse(str) # ex) "0.012063175s"
    Float.ceil(time_microsecond, 3) * 1_000 |> Kernel.trunc
  end
end
