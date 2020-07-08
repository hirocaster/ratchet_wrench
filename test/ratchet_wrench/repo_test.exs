defmodule RatchetWrench.RepoTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

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
    Process.sleep(10_000) # Wait DML

    TestHelper.check_ready_table(%Singer{})
    TestHelper.check_ready_table(%Data{})

    RatchetWrench.Repo.insert(%Singer{singer_id: "1", first_name: "Marc", last_name: "Richards"})
    RatchetWrench.Repo.insert(%Singer{singer_id: "3", first_name: "Kena"})

    test_data = %Data{data_id: "test_data",
                      string: Faker.String.base64(1_024_000),
                      bool: List.first(Enum.take_random([true, false], 1)),
                      int: List.first(Enum.take_random(0..9, 1)),
                      float: 99.9,
                      date: Faker.Date.date_of_birth(),
                      time_stamp: Faker.DateTime.forward(365)}
    RatchetWrench.Repo.insert(test_data)

    1..99
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
    |> Enum.map(&Task.await &1, 60_000 * 5)

    now_tz = System.get_env("TZ")
    System.put_env("TZ", "Asia/Tokyo")

    on_exit fn ->
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

  test ".get/2 bad args" do
    assert_raise FunctionClauseError, fn ->
      RatchetWrench.Repo.get(Singer, "3")
    end
  end

  test ".get/2 bad pk list args `PkCountMissMatchInListError`" do
    assert capture_log(fn ->
      assert_raise RatchetWrench.Exception.PkCountMissMatchInListError, fn ->
        RatchetWrench.Repo.get!(Singer, ["3", "over_count_pk"])
      end
    end) =~ "Pk count mismatch in args List type."
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

    sql = RatchetWrench.Repo.get_sql(Singer)
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

  test ".set!/1" do
    id_3_singer = RatchetWrench.Repo.get(Singer, ["3"])
    update_id_3_singer = Map.merge(id_3_singer, %{invalid_column: "invalid_data"})

    assert capture_log(fn ->
      assert_raise RatchetWrench.Exception.APIRequestError, fn ->
        RatchetWrench.Repo.set!(update_id_3_singer)
      end
    end) =~ "Unrecognized name: invalid_column"
  end

  test ".delete!/2" do
    assert capture_log(fn ->
      assert_raise RatchetWrench.Exception.APIRequestError, fn ->
        RatchetWrench.Repo.delete!(Singer, [3])
      end
    end) =~ "Invalid value for bind parameter singer_id:"
  end

  test "get all records from Singer" do
    {:ok, singers} = RatchetWrench.Repo.all(%Singer{})
    assert Enum.count(singers) == 2
    assert List.last(singers).__struct__ == Singer
    assert List.last(singers).first_name == "Kena"
    assert List.last(singers).singer_id == "3"
  end

  test "get all records from Data" do
    assert capture_log(fn ->
      {:ok, all_data_list} = RatchetWrench.Repo.all(%Data{})
      assert Enum.count(all_data_list) == 100
    end) =~ "Result set too large."
  end

  test "get all records and where from Singer" do
    where_sql = "singer_id = @singer_id"
    params = %{singer_id: "3"}
    {:ok, singer_list} = RatchetWrench.Repo.all(%Singer{}, where_sql, params)
    assert List.first(singer_list).singer_id == "3"
    assert List.first(singer_list).first_name == "Kena"
  end

  test "not found all records and where from Singer" do
    where_sql = "singer_id = @singer_id"
    params = %{singer_id: "not found id"}

    {:ok, singer_list} = RatchetWrench.Repo.all(%Singer{}, where_sql, params)
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

    assert capture_log(fn ->
      {:error, exception} = RatchetWrench.Repo.where(%Singer{}, where_sql, params)
      assert exception.client.status == 400
      reason = Poison.Parser.parse!(exception.client.body, %{})
      assert reason["error"]["code"] == 400
      assert reason["error"]["status"] == "INVALID_ARGUMENT"
      assert reason["error"]["message"] =~ "Syntax error: Unexpected "
    end) =~ "(RatchetWrench.Exception.APIRequestError) Request API error."
  end

  test "convert value to SQL value" do
    assert RatchetWrench.Repo.convert_value(nil) == nil
    assert RatchetWrench.Repo.convert_value(true) == true
    assert RatchetWrench.Repo.convert_value(false) == false
    assert RatchetWrench.Repo.convert_value(999) == "999"
    assert RatchetWrench.Repo.convert_value(99.9) == 99.9
    assert RatchetWrench.Repo.convert_value("string") == "string"
  end

  test "convert value type for TIMESTAMP" do
    {:ok, from_cloudspanner_dt, 0} = DateTime.from_iso8601("2020-06-30 05:11:40.02812Z")
    {:ok, elixir_dt, 0}            = DateTime.from_iso8601("2020-06-30 05:11:40.028120Z")

    converted_date_time = RatchetWrench.Repo.convert_value_type("#{from_cloudspanner_dt}", "TIMESTAMP")

    {:ok, converted_date_time} = DateTime.shift_zone(converted_date_time, "Etc/UTC", Tzdata.TimeZoneDatabase)
    assert converted_date_time == elixir_dt
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
      get_sql = RatchetWrench.Repo.get_sql(UserItem)
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
  end

  test ".convert_to_params/1" do
    date = Faker.Date.date_of_birth()

    {:ok, time_stamp, 0} = DateTime.from_iso8601("2015-01-23 23:50:07Z")

    data = %Data{data_id: "convert_to_params/1",
                 string: "string",
                 bool: true,
                 int: 999,
                 float: 99.9,
                 date: date,
                 time_stamp: time_stamp}

    params = RatchetWrench.Repo.convert_to_params(data)
    assert params.data_id == "convert_to_params/1"
    assert params.string == "string"
    assert params.bool
    assert params.int == "999"
    assert params.float == 99.9
    assert params.date == "#{date}"
    assert params.time_stamp == "2015-01-23T23:50:07Z"
  end

  test ".paramTypes/1" do
    assert RatchetWrench.Repo.param_types(Singer) == %{first_name: %{code: "STRING"},
                                                       inserted_at: %{code: "TIMESTAMP"},
                                                       last_name: %{code: "STRING"},
                                                       singer_id: %{code: "STRING"},
                                                       updated_at: %{code: "TIMESTAMP"}}
  end

  describe "type cast checks" do
    test "insert/update check type for data " do
      id = "test_data_999"

      test_data = %Data{data_id: id,
                        string: Faker.String.base64(1_024_000),
                        bool: List.first(Enum.take_random([true, false], 1)),
                        int: List.first(Enum.take_random(0..9, 1)),
                        float: 99.9,
                        date: Faker.Date.date_of_birth(),
                        time_stamp: Faker.DateTime.forward(365)}
      {:ok, data} = RatchetWrench.Repo.insert(test_data)

      assert is_binary(data.data_id)
      assert is_binary(data.string)
      assert is_boolean(data.bool)
      assert is_integer(data.int)
      assert is_float(data.float)
      assert data.date.__struct__ == Date
      assert data.time_stamp.__struct__ == DateTime

      test_data2 = %Data{data_id: id,
                        string: Faker.String.base64(1_024_000),
                        bool: List.first(Enum.take_random([true, false], 1)),
                        int: List.first(Enum.take_random(0..9, 1)),
                        float: 99.9,
                        date: Faker.Date.date_of_birth(),
                        time_stamp: Faker.DateTime.forward(365)}
      {:ok, data} = RatchetWrench.Repo.set(test_data2)

      assert is_binary(data.data_id)
      assert is_binary(data.string)
      assert is_boolean(data.bool)
      assert is_integer(data.int)
      assert is_float(data.float)
      assert data.date.__struct__ == Date
      assert data.time_stamp.__struct__ == DateTime

      RatchetWrench.Repo.delete(Data, [id])
    end

    test "get test data by .get/2" do
      test_data = RatchetWrench.Repo.get(Data, ["test_data"])
      assert is_binary(test_data.data_id)
      assert is_binary(test_data.string)
      assert is_boolean(test_data.bool)
      assert is_integer(test_data.int)
      assert is_float(test_data.float)
      assert test_data.date.__struct__ == Date
      assert test_data.time_stamp.__struct__ == DateTime
    end

    test "get test data by .where/3" do
      {:ok, test_data_list} = RatchetWrench.Repo.where(%Data{}, "data_id = @data_id", %{data_id: "test_data"})
      assert Enum.count(test_data_list) == 1

      test_data = List.first(test_data_list)

      assert is_binary(test_data.data_id)
      assert is_binary(test_data.string)
      assert is_boolean(test_data.bool)
      assert is_integer(test_data.int)
      assert is_float(test_data.float)
      assert test_data.date.__struct__ == Date
      assert test_data.time_stamp.__struct__ == DateTime
    end
  end
end
