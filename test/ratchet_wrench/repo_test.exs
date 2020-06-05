defmodule RatchetWrench.RepoTest do
  use ExUnit.Case

  setup_all do
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

    System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.data")

    Process.sleep(10_000) # Wait DML
    TestHelper.check_ready_table(%Singer{})
    TestHelper.check_ready_table(%Data{})

    RatchetWrench.Repo.insert(%Singer{singer_id: "1", first_name: "Marc", last_name: "Richards"})
    RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})

    1..100
    |> Enum.map(fn(_) ->
      Task.async(fn ->
                  new_data = %Data{string: Faker.String.base64(1_024_000),
                                   bool: List.first(Enum.take_random([true, false], 1)),
                                   int: List.first(Enum.take_random(0..9, 1)),
                                   float: 99.9,
                                   date: Faker.Date.date_of_birth(),
                                   time_stamp: Faker.DateTime.forward(365)
                                  }
        TestHelper.insert_loop(new_data)
      end)
    end)
    |> Enum.map(&Task.await &1, 60000)

    now_tz = System.get_env("TZ")
    System.put_env("TZ", "Asia/Tokyo")

    on_exit fn ->
      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.admin")
      {:ok, _} = RatchetWrench.update_ddl(["DROP TABLE singers",
                                           "DROP TABLE data"])

      if now_tz == nil do
        System.delete_env("TZ")
      else
        System.put_env("TZ", now_tz)
      end
    end
  end

  test "get id 3" do
    id_3_singer = RatchetWrench.Repo.get(Singer, ["3"])

    assert id_3_singer.__struct__ == Singer
    assert id_3_singer.singer_id == "3"
    assert id_3_singer.first_name == "Kena"
  end

  test "get not found id" do
    not_exist_id = ["999999999"]
    assert RatchetWrench.Repo.get(Singer, not_exist_id) == nil
  end

  test "insert/get/delete new record at auto uuid value" do
    new_singer = %Singer{first_name: "example_first_name_auto_uuid"}
    now_timestamp = DateTime.utc_now

    assert new_singer.singer_id == nil
    assert new_singer.inserted_at == nil
    assert new_singer.updated_at == nil

    {:ok, struct} = RatchetWrench.Repo.insert(new_singer)

    assert struct.singer_id != nil
    assert byte_size(struct.singer_id) == 36 # UUIDv4
    assert struct.first_name == "example_first_name_auto_uuid"
    assert struct.inserted_at == struct.updated_at
    assert now_timestamp < struct.inserted_at

    sql = RatchetWrench.Repo.get_sql(Singer, [struct.singer_id])
    params = RatchetWrench.Repo.params_pk_map(Singer, [struct.singer_id])
    {:ok, raw_result_set} = RatchetWrench.select_execute_sql(sql, params)

    raw_update_at_string = raw_result_set.rows |> List.first |> List.last

    d = DateTime.utc_now
    utc_now_string = String.slice("#{d}", 11, 5) # ex) "13:23"
    raw_timestamp_string = String.slice("#{raw_update_at_string}", 11, 5)
    assert utc_now_string == raw_timestamp_string

    result = RatchetWrench.Repo.get(Singer, [struct.singer_id])
    assert result.singer_id == struct.singer_id
    assert result.inserted_at.time_zone == "Asia/Tokyo"
    assert result.updated_at.time_zone == "Asia/Tokyo"

    {:ok, result_set} = RatchetWrench.Repo.delete(Singer, [struct.singer_id])

    assert result_set.stats.rowCountExact == "1"
    assert RatchetWrench.Repo.get(Singer, [struct.singer_id]) == nil
  end

  test "update record" do
    id_3_singer = RatchetWrench.Repo.get(Singer, ["3"])
    update_id_3_singer = Map.merge(id_3_singer, %{first_name: "Hana"})

    assert update_id_3_singer.first_name == "Hana"

    {:ok, updated_id3_singer} = RatchetWrench.Repo.set(update_id_3_singer)
    assert id_3_singer.singer_id == updated_id3_singer.singer_id
    assert updated_id3_singer.first_name == "Hana"

    updated_new_id_3_singer = RatchetWrench.Repo.get(Singer, ["3"])
    assert updated_new_id_3_singer.first_name == "Hana"

    update_new_id_3_singer = Map.merge(updated_new_id_3_singer, %{first_name: "Kena"})

    {:ok, updated_new_id_3_singer} = RatchetWrench.Repo.set(update_new_id_3_singer)
    assert updated_new_id_3_singer.singer_id == "3"
    assert updated_new_id_3_singer.first_name == "Kena"

    singer_kena = RatchetWrench.Repo.get(Singer, ["3"])
    assert singer_kena.first_name == "Kena"
  end

  test "get all records from Singer" do
    singers = RatchetWrench.Repo.all(%Singer{})
    assert Enum.count(singers) == 2
    assert List.last(singers).__struct__ == Singer
    assert List.last(singers).first_name == "Kena"
    assert List.last(singers).singer_id == "3"
  end

  test "get all records from Data" do
    all_data_list = RatchetWrench.Repo.all(%Data{})
    assert Enum.count(all_data_list) == 100
  end

  test "get all records and where from Singer" do
    where_sql = "singer_id = @singer_id"
    params = %{singer_id: "3"}
    singer_list = RatchetWrench.Repo.all(%Singer{}, where_sql, params)
    assert List.first(singer_list).singer_id == "3"
    assert List.first(singer_list).first_name == "Kena"
  end

  test "not found all records and where from Singer" do
    where_sql = "singer_id = @singer_id"
    params = %{singer_id: "not found id"}

    singer_list = RatchetWrench.Repo.all(%Singer{}, where_sql, params)
    assert singer_list == []
  end

  test "get where records" do
    where_sql = "first_name = @first_name"
    params    = %{first_name: "Marc"}
    {:ok, singers_list} = RatchetWrench.Repo.where(%Singer{}, where_sql, params)
    assert List.first(singers_list).singer_id == "1"
    assert List.first(singers_list).first_name == "Marc"
  end

  test "get where records at not found record" do
    where_sql = "first_name = @first_name"
    params    = %{first_name: "not found first name"}
    {:ok, singers_list} = RatchetWrench.Repo.where(%Singer{}, where_sql, params)
    assert singers_list == []
  end

  test "where DML syntax error" do
    where_sql = "first_name >< @first_name"
    params    = %{first_name: "syntax DML error"}
    {:error, reason} = RatchetWrench.Repo.where(%Singer{}, where_sql, params)
    assert reason["error"]["code"] == 400
    assert reason["error"]["status"] == "INVALID_ARGUMENT"
    assert reason["error"]["message"] =~ "Syntax error: Unexpected "
  end

  test "convert value to SQL value" do
    assert RatchetWrench.Repo.convert_value(nil) == nil
    assert RatchetWrench.Repo.convert_value(true) == true
    assert RatchetWrench.Repo.convert_value(false) == false
    assert RatchetWrench.Repo.convert_value(999) == "999"
    assert RatchetWrench.Repo.convert_value(99.9) == 99.9
    assert RatchetWrench.Repo.convert_value("string") == "string"
  end

  test ".set_uuid_value/1" do
    singer = RatchetWrench.Repo.set_uuid_value(%Singer{})
    assert singer.singer_id != nil
  end

  describe "return SQL" do
    test ".where_pk_sql" do
      where_sql = RatchetWrench.Repo.where_pk_sql(UserItem)
      assert where_sql == "user_id = @user_id AND user_item_id = @user_item_id"
    end

    test ".get_sql" do
      get_sql = RatchetWrench.Repo.get_sql(UserItem, [1, 10])
      assert get_sql == "SELECT * FROM user_items WHERE user_id = @user_id AND user_item_id = @user_item_id"
    end

    test ".insert_sql/1" do
      singer = %Singer{first_name: "test_first_name"}
      insert_sql = RatchetWrench.Repo.insert_sql(singer)
      assert insert_sql == "INSERT INTO singers(first_name, inserted_at, last_name, singer_id, updated_at) VALUES(@first_name, @inserted_at, @last_name, @singer_id, @updated_at)"
    end

    test ".delete_sql/1" do
      delete_sql = RatchetWrench.Repo.delete_sql(Singer)
      assert delete_sql == "DELETE FROM singers WHERE singer_id = @singer_id"

      delete_sql = RatchetWrench.Repo.delete_sql(UserItem)
      assert delete_sql == "DELETE FROM user_items WHERE user_id = @user_id AND user_item_id = @user_item_id"
    end

    test ".update_sql/1" do
      singer = %Singer{first_name: "test_first_name"}
      update_sql = RatchetWrench.Repo.update_sql(singer)
      assert update_sql == "UPDATE singers SET first_name = @first_name, inserted_at = @inserted_at, last_name = @last_name, updated_at = @updated_at WHERE singer_id = @singer_id"
    end
  end

  test ".params_pk_map" do
    params = RatchetWrench.Repo.params_pk_map(UserItem, [1, 10])
    assert params == %{user_id: 1, user_item_id: 10}
  end

  test ".valid_pk_value_list!/2" do
    assert RatchetWrench.Repo.valid_pk_value_list!(UserItem, [1, 10]) == nil

    assert_raise RuntimeError, fn ->
      RatchetWrench.Repo.valid_pk_value_list!(UserItem, [1])
    end
  end

  test ".params_insert_values_map/1" do
    singer = %Singer{first_name: "test_first_name"}
    params = RatchetWrench.Repo.params_insert_values_map(singer)
    assert params.first_name == "test_first_name"
    assert params.last_name == nil
    assert byte_size(params.singer_id ) == 36 # uuidv4
    assert is_binary(params.inserted_at)
    assert is_binary(params.updated_at)
    assert params.inserted_at == params.updated_at
  end

  test ".paramTypes/1" do
    assert RatchetWrench.Repo.param_types(Singer) == %{first_name: %{code: "STRING"},
                                                       inserted_at: %{code: "TIMESTAMP"},
                                                       last_name: %{code: "STRING"},
                                                       singer_id: %{code: "STRING"},
                                                       updated_at: %{code: "TIMESTAMP"}}
  end
end
