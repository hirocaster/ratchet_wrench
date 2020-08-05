ExUnit.start()
Faker.start()

defmodule Singer do
  @moduledoc """
  Sample model module for unit test.
  """
  use RatchetWrench.Model

  schema do
    uuid :singer_id
    pk [:singer_id]
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
    uuid :data_id
    pk [:data_id]
    attributes data_id: {"STRING", nil},
      string: {"STRING", ""},
      bool: {"BOOL", nil },
      int: {"INT64", nil},
      float: {"FLOAT64", nil},
      date: {"DATE", nil},
      time_stamp: {"TIMESTAMP", nil}
  end
end

defmodule User do
  @moduledoc """
  Sample model module for unit test.
  """

  use RatchetWrench.Model

  schema do
    uuid :user_id
    pk [:user_id]
    attributes user_id: {"STRING", nil},
      name: {"STRING", nil}
  end
end

defmodule UserItem do
  @moduledoc """
  Sample model module for unit test.
  """

  use RatchetWrench.Model

  schema do
    uuid :user_item_id
    pk [:user_id, :user_item_id]
    interleave [:user_id]
    attributes user_item_id: {"STRING", nil},
      user_id: {"STRING", nil},
      name: {"STRING", nil}
  end
end

defmodule UserLog do
  @moduledoc """
  Sample model module for unit test.
  """

  use RatchetWrench.Model

  schema do
    uuid :user_log_id
    pk [:user_id]
    interleave [:user_id]
    attributes user_log_id: {"STRING", nil},
      user_id: {"STRING", nil},
      message: {"STRING", nil}
  end
end

defmodule TestHelper do
  def check_ready_table(struct) do
    uuid_name = struct.__struct__.__uuid__
    uuid_value = ["uuid_test"]
    {map, _} = Code.eval_string("%{#{uuid_name}: '#{uuid_value}'}")
    test_data = Map.merge(struct, map)

    {:ok, _} = insert_loop(test_data)
    {:ok, _} = get_loop(test_data.__struct__, uuid_value)
    {:ok, _} = delete_loop(test_data.__struct__, uuid_value)
  end

  def insert_loop(struct) do
    case RatchetWrench.Repo.insert(struct) do
      {:ok, result} -> {:ok, result}
      {:error, _reason} ->
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

  def get_loop(module, uuid_value) do
    result = RatchetWrench.Repo.get(module, uuid_value)
    if result == nil do
      Process.sleep(1000)
      get_loop(module, uuid_value)
    else
      {:ok, result}
    end
  end

  def delete_loop(module, uuid_value) do
    case RatchetWrench.Repo.delete(module, uuid_value) do
      {:ok, result} -> {:ok, result}
      {:error} ->
        Process.sleep(1000)
        delete_loop(module, uuid_value)
    end
  end
end
