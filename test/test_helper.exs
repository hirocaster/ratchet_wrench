ExUnit.start()
Faker.start()

defmodule Singer do
  @moduledoc """
  Sample model module for unit test.
  """
  use RatchetWrench.Model

  schema do
    pk :singer_id
    attributes singer_id: {"STRING", nil},
      first_name: {"STRING", nil},
      last_name: {"STRING", nil},
      inserted_at: {"TIMESTAMP", nil},
      updated_at: {"TIMESTAMP", nil}
  end
end

defmodule Data do
  @moduledoc """
  Support cloud spanner data type module for unit test.
  """

  use RatchetWrench.Model

  schema do
    pk :data_id
    attributes data_id: {"STRING", nil},
      string: {"STRING", ""},
      bool: {"BOOL", nil },
      int: {"INT64", nil},
      float: {"FLOAT64", nil},
      date: {"DATE", nil},
      time_stamp: {"TIMESTAMP", nil}
  end
end

defmodule UserItem do
  @moduledoc """
  Sample model module for unit test.
  """

  use RatchetWrench.Model

  schema do
    pk :user_item_id
    attributes user_item_id: {"STRING", nil},
      name: {"STRING", ""}
  end
end

defmodule TestHelper do
  def check_ready_table(struct) do
    pk_name = struct.__struct__.__pk__
    pk_value = "test_data"
    {map, _} = Code.eval_string("%{#{pk_name}: '#{pk_value}'}")
    test_data = Map.merge(struct, map)

    {:ok, _} = insert_loop(test_data)
    {:ok, _} = get_loop(test_data.__struct__, pk_value)
    {:ok, _} = delete_loop(test_data.__struct__, pk_value)
  end

  def insert_loop(struct) do
    case RatchetWrench.Repo.insert(struct) do
      {:ok, result} -> {:ok, result}
      {:error, _} ->
        Process.sleep(1000)
        insert_loop(struct)
    end
  end

  def set_loop(struct) do
    case RatchetWrench.Repo.set(struct) do
      {:ok, result} -> {:ok, result}
      {:error, _} ->
        Process.sleep(1000)
        set_loop(struct)
    end
  end

  def get_loop(module, pk_value) do
    result = RatchetWrench.Repo.get(module, pk_value)
    if result == nil do
      Process.sleep(1000)
      get_loop(module, pk_value)
    else
      {:ok, result}
    end
  end

  def delete_loop(module, pk_value) do
    case RatchetWrench.Repo.delete(module, pk_value) do
      {:ok, result} -> {:ok, result}
      {:error} ->
        Process.sleep(1000)
        delete_loop(module, pk_value)
    end
  end
end
