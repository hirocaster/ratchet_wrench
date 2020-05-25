defmodule RatchetWrench.RepoTest do
  use ExUnit.Case

  setup do
    now_tz = System.get_env("TZ")
    System.put_env("TZ", "Asia/Tokyo")

    on_exit(fn ->
      if now_tz == nil do
        System.delete_env("TZ")
      else
        System.put_env("TZ", now_tz)
      end
    end)
  end

  test "get id 3" do
    id_3_singer = RatchetWrench.Repo.get(Singer, "3")

    assert id_3_singer.__struct__ == Singer
    assert id_3_singer.id == "3"
    assert id_3_singer.first_name == "Kena"
    assert id_3_singer.birth_date == ~D[1961-04-01]
  end

  test "get not found id" do
    not_exist_id = "999999999"
    assert RatchetWrench.Repo.get(Singer, not_exist_id) == nil
  end

  test "insert/get/delete new record at auto index value" do
    new_singer = %Singer{first_name: "example_first_name_auto_index"}
    now_timestamp = DateTime.utc_now

    assert new_singer.id == nil
    assert new_singer.created_at == nil
    assert new_singer.updated_at == nil

    struct = RatchetWrench.Repo.insert(new_singer)

    assert struct.id != nil
    assert byte_size(struct.id) == 36 # UUIDv4
    assert struct.first_name == "example_first_name_auto_index"

    assert now_timestamp < struct.created_at
    assert struct.created_at == struct.updated_at

    sql = "SELECT * FROM singers WHERE id = '#{struct.id}'"
    {:ok, raw_result_set} = RatchetWrench.select_execute_sql(sql)

    raw_update_at_string = raw_result_set.rows |> List.first |> List.last

    d = DateTime.utc_now
    utc_now_string = String.slice("#{d}", 11, 5) # ex) "13:23"
    raw_timestamp_string = String.slice("#{raw_update_at_string}", 11, 5)
    assert utc_now_string == raw_timestamp_string

    result = RatchetWrench.Repo.get(Singer, struct.id)
    assert result.id == struct.id
    assert result.created_at.time_zone == "Asia/Tokyo"
    assert result.updated_at.time_zone == "Asia/Tokyo"

    assert {:ok} == RatchetWrench.Repo.delete(Singer, struct.id)
    assert RatchetWrench.Repo.get(Singer, struct.id) == nil
  end

  test "update record" do
    id_3_singer = RatchetWrench.Repo.get(Singer, "3")
    update_id_3_singer = Map.merge(id_3_singer, %{first_name: "Hana"})

    assert update_id_3_singer.first_name == "Hana"

    {:ok, updated_id3_singer} = RatchetWrench.Repo.set(update_id_3_singer)
    assert id_3_singer.id == updated_id3_singer.id
    assert updated_id3_singer.first_name == "Hana"

    updated_new_id_3_singer = RatchetWrench.Repo.get(Singer, "3")
    assert updated_new_id_3_singer.first_name == "Hana"

    update_new_id_3_singer = Map.merge(updated_new_id_3_singer, %{first_name: "Kena"})

    {:ok, updated_new_id_3_singer} = RatchetWrench.Repo.set(update_new_id_3_singer)
    assert updated_new_id_3_singer.id == "3"
    assert updated_new_id_3_singer.first_name == "Kena"

    singer_kena = RatchetWrench.Repo.get(Singer, "3")
    assert singer_kena.first_name == "Kena"
  end

  test "get all records from Singer" do
    singers = RatchetWrench.Repo.all(%Singer{})
    assert Enum.count(singers) == 2
    assert List.last(singers).__struct__ == Singer
    assert List.last(singers).first_name == "Kena"
    assert List.last(singers).id == "3"
  end

  test "get all records from Data" do
    all_data_list = RatchetWrench.Repo.all(%Data{})
    assert Enum.count(all_data_list) == 146_646
    assert List.last(all_data_list).string == "D40OgpG9"
  end

  test "get all records and where from Singer" do
    singer_list = RatchetWrench.Repo.all(%Singer{}, "id = '3'")
    assert List.first(singer_list).id == "3"
    assert List.first(singer_list).first_name == "Kena"
  end

  test "not found all records and where from Singer" do
    singer_list = RatchetWrench.Repo.all(%Singer{}, "id = 'not found id'")
    assert singer_list == []
  end

  test "get where records" do
    value = "Marc"
    {:ok, singers_list} = RatchetWrench.Repo.where(%Singer{}, "first_name = '#{value}'")
    assert List.first(singers_list).id == "1"
    assert List.first(singers_list).first_name == "Marc"
  end

  test "get where records at not found record" do
    value = "not found value"
    {:ok, singers_list} = RatchetWrench.Repo.where(%Singer{}, "first_name = '#{value}'")
    assert singers_list == []
  end

  test "where DML syntax error" do
    {:error, reason} = RatchetWrench.Repo.where(%Singer{}, "first_name >< syntax error")
    assert reason["error"]["code"] == 400
    assert reason["error"]["status"] == "INVALID_ARGUMENT"
    assert reason["error"]["message"] =~ "Syntax error: Unexpected "
  end

  # test "Add fake data" do
  #   Enum.each(1..100000, fn(x) ->
  #     IO.puts x
  #     new_data = %Data{string: Faker.String.base64,
  #                      bool: List.first(Enum.take_random([True, False], 1)),
  #                      int: List.first(Enum.take_random(0..9, 1)),
  #                      float: 99.9,
  #                      date: Faker.Date.date_of_birth(),
  #                      time_stamp: Faker.DateTime.forward(365)
  #                     }
  #     struct = RatchetWrench.Repo.insert(new_data)
  #     assert struct.id != nil
  #   end)
  # end

  test "convert value to SQL value" do
    assert RatchetWrench.Repo.convert_value(nil) == "NULL"
    assert RatchetWrench.Repo.convert_value(true) == "TRUE"
    assert RatchetWrench.Repo.convert_value(false) == "FALSE"
    assert RatchetWrench.Repo.convert_value(999) == "999"
    assert RatchetWrench.Repo.convert_value(99.9) == "99.9"
  end

  test "Update records in transaction" do
    raw_data = RatchetWrench.Repo.get(Data, "00000493-55f8-42ea-9a12-fe691a86825f")
    assert raw_data.id == "00000493-55f8-42ea-9a12-fe691a86825f"
    assert raw_data.int == 9

    update_data_map = Map.merge(raw_data, %{int: raw_data.int - 1})
    update_sql = RatchetWrench.Repo.update_sql(update_data_map)

    new_singer = %Singer{id: "test in transaction", first_name: "new singer in transaction"}

    insert_sql = RatchetWrench.Repo.insert_sql(new_singer)

    transaction_sql_list = [update_sql, insert_sql]

    RatchetWrench.transaction_execute_sql(transaction_sql_list)

    get_new_singer = RatchetWrench.Repo.get(Singer, "test in transaction")
    assert get_new_singer.id == "test in transaction"
    assert get_new_singer.first_name == "new singer in transaction"
    assert RatchetWrench.Repo.get(Data, "00000493-55f8-42ea-9a12-fe691a86825f").int == 8

    RatchetWrench.Repo.set(raw_data)
    RatchetWrench.Repo.delete(Singer, "test in transaction")
  end
end
