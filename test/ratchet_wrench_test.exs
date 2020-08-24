defmodule RatchetWrenchTest do
  use ExUnit.Case
  import ExUnit.CaptureLog
  doctest RatchetWrench

  setup_all do
    start_supervised({RatchetWrench.SessionPool, %RatchetWrench.Pool{}})

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
    Process.sleep(20_000) # Wait apply DML

    TestHelper.check_ready_table(%Singer{})
    TestHelper.check_ready_table(%Data{})

    RatchetWrench.Repo.insert(%Singer{singer_id: "1", first_name: "Marc", last_name: "Richards"})
    RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})

    on_exit fn ->
      {:ok, _} = RatchetWrench.update_ddl(["DROP TABLE singers",
                                           "DROP TABLE data"])
      Process.sleep(10_000) # Wait DML
    end
  end

  test "update ddl, error syntax" do
    ddl_error = "Error Syntax DDL"
    ddl_list = [ddl_error]
    capture_log(fn ->
      {:error, err} = RatchetWrench.update_ddl(ddl_list)
      assert err.__struct__ == RatchetWrench.Exception.APIRequestError
      assert err.message =~ "Error parsing Spanner DDL statement: Error Syntax DDL : Syntax error on line 1, column 1: Encountered 'Error' while parsing: ddl_statement"
    end)
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
    {:ok, session} = RatchetWrench.Session.create(connection)
    assert session != nil
    {:ok, _} = RatchetWrench.Session.delete(connection, session)
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

    RatchetWrench.transaction(fn ->
      {:ok, result_set} = RatchetWrench.execute_sql(insert_sql, insert_params, insert_param_types)
      assert result_set != nil
    end)

    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    assert result_set != nil

    Enum.with_index(result_set.rows, 1)
    |> Enum.map(fn({raw_list, singer_id}) ->
      assert List.first(raw_list) == "#{singer_id}"
    end)

    delete_sql = "DELETE FROM singers WHERE singer_id = @singer_id"
    delete_params = %{singer_id: "2"}
    delete_param_types = RatchetWrench.Repo.param_types(Singer)

    RatchetWrench.transaction(fn ->
      {:ok, result_set} = RatchetWrench.execute_sql(delete_sql, delete_params, delete_param_types)
      assert result_set != nil
      assert result_set.stats.rowCountExact == "1"
    end)

    {:ok, result_set} = RatchetWrench.select_execute_sql("SELECT * FROM singers", %{})
    after_rows_count = Enum.count(result_set.rows)

    assert before_rows_count == after_rows_count
  end

  test ".transaction/1" do
    refute RatchetWrench.TransactionManager.exist_transaction?
    RatchetWrench.transaction(fn ->
      assert RatchetWrench.TransactionManager.exist_transaction?()

      {:ok, singer } = RatchetWrench.Repo.insert(%Singer{singer_id: "test transaction function",
                                                        first_name: "trans func"})
      assert singer == RatchetWrench.Repo.get(Singer, ["test transaction function"])

      update_singer = Map.merge(singer, %{first_name: "trans2"})
      Process.sleep(1000) # wait time diff
      {:ok, updated_singer} = RatchetWrench.Repo.set(update_singer)

      assert updated_singer.first_name == "trans2"
      assert singer.inserted_at == updated_singer.inserted_at
      diff = DateTime.diff(updated_singer.updated_at, updated_singer.inserted_at)
      assert diff >= 1

      assert RatchetWrench.Repo.exists?(Singer, %{singer_id: "test transaction function"})

      RatchetWrench.Repo.delete(Singer, ["test transaction function"])
      assert RatchetWrench.TransactionManager.exist_transaction?()
    end)
    refute RatchetWrench.TransactionManager.exist_transaction?
    assert nil == RatchetWrench.Repo.get(Singer, ["test transaction function"])
  end

  test ".transaction!/1" do
    RatchetWrench.transaction!(fn ->
      {:ok, singer } = RatchetWrench.Repo.insert(%Singer{singer_id: "test transaction function",
                                                        first_name: "trans func"})
      assert singer == RatchetWrench.Repo.get(Singer, ["test transaction function"])

      update_singer = Map.merge(singer, %{first_name: "trans2"})
      Process.sleep(1000) # wait time diff
      {:ok, updated_singer} = RatchetWrench.Repo.set(update_singer)

      assert updated_singer.first_name == "trans2"
      assert singer.inserted_at == updated_singer.inserted_at
      diff = DateTime.diff(updated_singer.updated_at, updated_singer.inserted_at)
      assert diff >= 1

      {:ok, result_list} = RatchetWrench.Repo.where(%Singer{}, "first_name = @first_name", %{first_name: "trans2"})
      assert List.first(result_list) == updated_singer

      {:ok, result_list} = RatchetWrench.Repo.all(%Singer{})
      assert Enum.count(result_list) == 3
      assert List.last(result_list).singer_id == "test transaction function"

      {:ok, result_list} = RatchetWrench.Repo.all(%Singer{},
        "singer_id = @singer_id",
        %{singer_id: "test transaction function"})
      assert Enum.count(result_list) == 1
      assert List.last(result_list).singer_id == "test transaction function"

      RatchetWrench.Repo.delete(Singer, ["test transaction function"])
    end)

    assert nil == RatchetWrench.Repo.get(Singer, ["test transaction function"])
  end

  test "Use transaction in async" do
    0..4
    |> Enum.map(fn(_) ->
      Task.async(fn ->
        RatchetWrench.transaction(fn ->
          id = UUID.uuid4()
          {:ok, singer} = RatchetWrench.Repo.insert(%Singer{singer_id: id,
                                                             first_name: "trans func #{id}"})
          assert singer == RatchetWrench.Repo.get(Singer, [id])

          update_singer = Map.merge(singer, %{first_name: "trans2 #{id}"})
          Process.sleep(1000) # wait time diff
          {:ok, updated_singer} = RatchetWrench.Repo.set(update_singer)

          assert updated_singer.first_name == "trans2 #{id}"
          assert singer.inserted_at == updated_singer.inserted_at
          diff = DateTime.diff(updated_singer.updated_at, updated_singer.inserted_at)
          assert diff >= 1

          RatchetWrench.Repo.delete(Singer, [id])
          assert RatchetWrench.Repo.get(Singer, [id]) == nil
        end)
      end)
    end)
    |> Enum.map(&Task.await &1, 10000)
  end

  test "Transaction commit, only call .execute_sql" do
    RatchetWrench.execute_sql("SELECT * FROM singers", %{}, %{})
    assert RatchetWrench.TransactionManager.exist_transaction? == false
  end

  test "Error rollback request at raise in transaction" do
    log = capture_log(fn ->
      assert {:error, err} =
        RatchetWrench.transaction(fn ->
          # Finish tranaction at commit
          transaction = RatchetWrench.TransactionManager.get_or_begin_transaction()
          connection = RatchetWrench.token_data() |> RatchetWrench.connection()
          session = transaction.session
          RatchetWrench.commit_transaction(connection, session, transaction.transaction)

          # Rollback request is error response at after commit.
          raise "Call rollback by this raise"
        end)
      assert err.__struct__ == RuntimeError
    end)

    assert log =~ "Cannot rollback a transaction after Commit() has been called"
    assert log =~ "RatchetWrench.Exception.APIRequestError"
  end

  test "Nest transactions" do
    outside_singer_id = UUID.uuid4()
    inside_singer_id = UUID.uuid4()

    assert RatchetWrench.TransactionManager.exist_transaction? == false

    RatchetWrench.transaction fn ->
      assert RatchetWrench.TransactionManager.exist_transaction?

      RatchetWrench.Repo.insert(%Singer{singer_id: outside_singer_id,
                                        first_name: "trans outside func #{outside_singer_id}"})

      RatchetWrench.transaction fn ->
        assert RatchetWrench.TransactionManager.exist_transaction?

        RatchetWrench.Repo.insert(%Singer{singer_id: inside_singer_id,
                                          first_name: "trans inside func #{inside_singer_id}"})
      end

      RatchetWrench.Repo.delete(Singer, [inside_singer_id])
      RatchetWrench.Repo.delete(Singer, [outside_singer_id])

      assert RatchetWrench.TransactionManager.exist_transaction?
    end
    assert RatchetWrench.TransactionManager.exist_transaction? == false
  end

  test "Rollback in nest transactions(outer)" do
    singer_id1 = UUID.uuid4()
    singer_id2 = UUID.uuid4()

    assert RatchetWrench.TransactionManager.exist_transaction? == false

    assert capture_log(fn ->
      {:error, error} =
        RatchetWrench.transaction fn ->
        assert RatchetWrench.TransactionManager.exist_transaction?

        RatchetWrench.transaction fn ->
          assert RatchetWrench.TransactionManager.exist_transaction? == true
          {:ok, _singer} = RatchetWrench.Repo.insert(%Singer{singer_id: singer_id1,
                                                             first_name: "trans func #{singer_id1}"})
        end

        assert RatchetWrench.TransactionManager.exist_transaction? == true

        raise "error from transaction"

        {:ok, _singer} = RatchetWrench.Repo.insert(%Singer{singer_id: singer_id2,
                                                           first_name: "trans func #{singer_id2}"})
      end

      assert %RuntimeError{message: "error from transaction"} = error
    end) =~ "error from transaction"

    assert RatchetWrench.Repo.get(Singer, [singer_id1]) == nil
    assert RatchetWrench.Repo.get(Singer, [singer_id2]) == nil

    assert RatchetWrench.TransactionManager.exist_transaction? == false
  end

  test "Rollback in nest transactions(inner)" do
    singer_id1 = UUID.uuid4()
    singer_id2 = UUID.uuid4()

    assert RatchetWrench.TransactionManager.exist_transaction? == false

    assert capture_log(fn ->
      {:error, error} =
        RatchetWrench.transaction fn ->
        assert RatchetWrench.TransactionManager.exist_transaction?

        RatchetWrench.transaction fn ->
          assert RatchetWrench.TransactionManager.exist_transaction? == true
          {:ok, _singer} = RatchetWrench.Repo.insert(%Singer{singer_id: singer_id1,
                                                             first_name: "trans func #{singer_id1}"})

          raise "error from transaction"
        end
        {:ok, _singer} = RatchetWrench.Repo.insert(%Singer{singer_id: singer_id2,
                                                           first_name: "trans func #{singer_id2}"})
      end

      assert %RuntimeError{message: "error from transaction"} = error
    end) =~ "error from transaction"

    assert RatchetWrench.Repo.get(Singer, [singer_id1]) == nil
    assert RatchetWrench.Repo.get(Singer, [singer_id2]) == nil

    assert RatchetWrench.TransactionManager.exist_transaction? == false
  end

  test "Duplicate insert for sample data" do
    assert capture_log(fn ->
      RatchetWrench.transaction fn ->
        RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})
      end
    end) =~ "singers already exists"
  end

  test "Duplicate insert! for sample data" do
    assert capture_log(fn ->
      {:error, err} = RatchetWrench.transaction fn ->
        RatchetWrench.Repo.insert!(%Singer{singer_id: "3", first_name: "Kena"})
      end
      assert err.__struct__ == RatchetWrench.Exception.APIRequestError
    end) =~ "singers already exists"
  end

  test "Rollback .insert/1" do
    assert capture_log(fn ->
      RatchetWrench.transaction fn ->
        RatchetWrench.Repo.insert(%Singer{singer_id: "test", first_name: "test"})
        assert RatchetWrench.Repo.get(Singer, ["test"])

        # duplicate insert
        RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})
      end
    end) =~ "singers already exists"

    assert RatchetWrench.Repo.get(Singer, ["test"]) == nil
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 0
  end

  # test "Rollback .set/1" do
  #   RatchetWrench.transaction fn ->
  #     # TODO: raise .set/1 and rollback
  #     RatchetWrench.Repo.set(%Singer{singer_id: "3", first_name: "TEST"})
  #   end
  #   # TODO: rollback check
  #   singer = RatchetWrench.Repo.get(Singer, ["3"])
  #   assert singer.first_name == "Kena"
  # end

  # TODO: this test
  # test "Rollback, delete/1" do
  # end

  test "Rollback, duplicate insert and nest transactions" do
    singer_id = UUID.uuid4()
    singer_id_b = UUID.uuid4()

    refute RatchetWrench.TransactionManager.exist_transaction?()

    assert capture_log(fn ->
      {:error, _} = RatchetWrench.transaction fn ->
        assert RatchetWrench.TransactionManager.exist_transaction?()

        RatchetWrench.Repo.insert(%Singer{singer_id: singer_id,
                                          first_name: "trans func #{singer_id}"})

        RatchetWrench.transaction fn ->
          assert RatchetWrench.TransactionManager.exist_transaction?()

          RatchetWrench.Repo.insert(%Singer{singer_id: singer_id,
                                            first_name: "trans func #{singer_id}"})

          assert RatchetWrench.TransactionManager.exist_transaction?()
        end

        {:error, :rollback} = RatchetWrench.Repo.insert(%Singer{singer_id: singer_id_b,
                                                                first_name: "trans func #{singer_id_b}"})

        assert RatchetWrench.TransactionManager.exist_transaction?()
      end

      assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 0

    end) =~ "singers already exists"

    assert RatchetWrench.Repo.get(Singer, [singer_id]) == nil
    assert RatchetWrench.Repo.get(Singer, [singer_id_b]) == nil
    refute RatchetWrench.TransactionManager.exist_transaction?()
    assert Enum.count(RatchetWrench.SessionPool.pool.checkout) == 0
  end
end
