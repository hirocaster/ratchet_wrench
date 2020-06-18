defmodule RatchetWrenchTest do
  use ExUnit.Case
  doctest RatchetWrench

  setup_all do
    env_scope = RatchetWrench.token_scope()

    System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.admin")
    ddl_singer = "CREATE TABLE data (
                   data_id STRING(36) NOT NULL,
                   string STRING(MAX),
                   bool BOOL,
                   int INT64,
                   float FLOAT64,
                   time_stamp TIMESTAMP,
                   date DATE,
                   ) PRIMARY KEY(data_id)"

    ddl_data = "CREATE TABLE singers (
                 singer_id STRING(36) NOT NULL,
                 first_name STRING(1024),
                 last_name STRING(1024),
                 inserted_at TIMESTAMP,
                 updated_at TIMESTAMP,
                 ) PRIMARY KEY(singer_id)"

    ddl_list = [ddl_singer, ddl_data]
    {:ok, _} = RatchetWrench.update_ddl(ddl_list)

    System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", env_scope)

    Process.sleep(10_000) # Wait apply DML
    TestHelper.check_ready_table(%Singer{})
    TestHelper.check_ready_table(%Data{})

    RatchetWrench.Repo.insert(%Singer{singer_id: "1", first_name: "Marc", last_name: "Richards"})
    RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})

    on_exit fn ->
      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.admin")
      {:ok, _} = RatchetWrench.update_ddl(["DROP TABLE singers",
                                           "DROP TABLE data"])

      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", env_scope)
    end
  end

  describe "Operation DDL" do
    setup do
      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.admin")

      on_exit fn ->
        System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.data")
      end
    end

    test "update ddl, error syntax" do
      ddl_error = "Error Syntax DDL"
      ddl_list = [ddl_error]
      {:error, reason} = RatchetWrench.update_ddl(ddl_list)
      assert reason["error"]["code"] == 400
      assert reason["error"]["message"] == "Error parsing Spanner DDL statement: Error Syntax DDL : Syntax error on line 1, column 1: Encountered 'Error' while parsing: ddl_statement"
    end
  end

  test "get token" do
    assert RatchetWrench.token().token !=  nil
    assert RatchetWrench.token().expires > :os.system_time(:second)
  end

  describe "Change bad token scope" do
    setup do
      env_scope = RatchetWrench.token_scope()
      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "bad/scope/token")

      on_exit fn ->
        System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", env_scope)
      end
    end

    test "Goth scope config error" do
      {:error, reason} = RatchetWrench.token()
      assert reason =~ "invalid_scope"
    end
  end

  test "get connection" do
    assert RatchetWrench.connection(RatchetWrench.token) != nil
  end

  test "get session/delete session" do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    session = RatchetWrench.create_session(connection)
    assert session != nil
    {:ok, _} = RatchetWrench.delete_session(connection, session)
  end

  test "Connection check CloudSpanner" do
    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT 1", %{})
    assert result_set != nil
    assert result_set.rows == [["1"]]
  end

  test "execute SELECT SQL" do
    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    assert result_set != nil

    [singer_id, first_name, last_name | _] = List.first(result_set.rows)
    assert singer_id == "1"
    assert first_name == "Marc"
    assert last_name == "Richards"
    [singer_id, first_name, last_name | _] = List.last(result_set.rows)
    assert singer_id == "3"
    assert first_name == "Kena"
    assert last_name == nil
  end

  test "SQL INSERT/UPDATE/DELETE" do
    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    before_rows_count = Enum.count(result_set.rows)

    insert_sql = "INSERT INTO singers(singer_id, first_name, last_name) VALUES(@singer_id, @first_name, @last_name)"
    insert_params = %{singer_id: "2", first_name: "Catalina", last_name: "Smith"}
    insert_param_types = RatchetWrench.Repo.param_types(Singer)
    {:ok, result_set} = RatchetWrench.execute_sql(insert_sql, insert_params, insert_param_types)
    assert result_set != nil

    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    assert result_set != nil

    Enum.with_index(result_set.rows, 1)
    |> Enum.map(fn({raw_list, singer_id}) ->
      assert List.first(raw_list) == "#{singer_id}"
    end)

    delete_sql = "DELETE FROM singers WHERE singer_id = @singer_id"
    delete_params = %{singer_id: "2"}
    delete_param_types = RatchetWrench.Repo.param_types(Singer)

    {:ok, result_set} = RatchetWrench.execute_sql(delete_sql, delete_params, delete_param_types)
    assert result_set != nil
    assert result_set.stats.rowCountExact == "1"


    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    after_rows_count = Enum.count(result_set.rows)

    assert before_rows_count == after_rows_count
  end

  test ".valid_transaction_execute_sql_map_list!" do
    insert_sql_map = %{sql: "INSERT INTO singers(singer_id, first_name, last_name) VALUES(@singer_id, @first_name, @last_name)",
                       params: %{singer_id: "2", first_name: "Catalina", last_name: "Smith"},
                       param_types: RatchetWrench.Repo.param_types(Singer)}
    assert :ok = RatchetWrench.valid_transaction_execute_sql_map_list!([insert_sql_map])


    raise_sql_map = %{sql: "INSERT INTO singers(singer_id, first_name, last_name) VALUES(@singer_id, @first_name, @last_name)",
                      params: %{singer_id: "2", first_name: "Catalina", last_name: "Smith"}}
    assert_raise RuntimeError, fn ->
      RatchetWrench.valid_transaction_execute_sql_map_list!([raise_sql_map])
    end
  end

  test "SQL SELECT/INSERT/UPDATE/DELETE in Transaction" do
    select_sql_map = %{sql: "SELECT * FROM singers",
                       params: %{},
                       param_types: %{}}

    insert_sql_map = %{sql: "INSERT INTO singers(singer_id, first_name, last_name) VALUES(@singer_id, @first_name, @last_name)",
                       params: %{singer_id: "2", first_name: "Catalina", last_name: "Smith"},
                       param_types: RatchetWrench.Repo.param_types(Singer)}

    update_sql_map = %{sql: "UPDATE singers SET first_name = @first_name WHERE singer_id = @singer_id",
                       params: %{first_name: "Cat", singer_id: "2"},
                       param_types: RatchetWrench.Repo.param_types(Singer)}

    delete_sql_map = %{sql: "DELETE FROM singers WHERE singer_id = @singer_id",
                       params: %{singer_id: "2"},
                       param_types: RatchetWrench.Repo.param_types(Singer)}

    sql_map_list = [select_sql_map, insert_sql_map, select_sql_map, update_sql_map, select_sql_map, delete_sql_map, select_sql_map]

    result_set_list = RatchetWrench.transaction_execute_sql(sql_map_list)
    assert result_set_list != nil

    [select_result_set | tail_set_list] = result_set_list
    assert select_result_set != nil
    assert Enum.count(select_result_set.rows) == 2

    [insert_result_set | tail_set_list] = tail_set_list
    assert insert_result_set != nil
    assert insert_result_set.stats.rowCountExact == "1"

    [select_result_set | tail_set_list] = tail_set_list
    assert select_result_set != nil
    assert Enum.count(select_result_set.rows) == 3

    [update_result_set | tail_set_list] = tail_set_list
    assert update_result_set != nil
    assert update_result_set.stats.rowCountExact == "1"

    [select_result_set | tail_set_list] = tail_set_list
    assert select_result_set != nil
    assert Enum.count(select_result_set.rows) == 3

    {update_raw, _} = List.pop_at(select_result_set.rows, 1)
    [singer_id, first_name, last_name | _] = update_raw
    assert singer_id == "2"
    assert first_name == "Cat"
    assert last_name == "Smith"

    [delete_result_set | tail_set_list] = tail_set_list
    assert delete_result_set.stats.rowCountExact == "1"

    [select_result_set | tail_set_list] = tail_set_list
    assert select_result_set != nil

    [singer_id, first_name, last_name | _] = List.first(select_result_set.rows)
    assert singer_id == "1"
    assert first_name == "Marc"
    assert last_name == "Richards"
    [singer_id, first_name, last_name | _] = List.last(select_result_set.rows)
    assert singer_id == "3"
    assert first_name == "Kena"
    assert last_name == nil

    assert tail_set_list == []
  end
end
