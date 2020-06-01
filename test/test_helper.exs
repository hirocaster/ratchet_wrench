ExUnit.start()
Faker.start()

defmodule Singer do
  @moduledoc """
  Sample model module for unit test.
  """
  use RatchetWrench.Model

  schema do
    attributes id: {"STRING", nil},
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
    attributes id: {"STRING", nil},
      string: {"STRING", ""},
      bool: {"BOOL", nil },
      int: {"INT64", nil},
      float: {"FLOAT64", nil},
      date: {"DATE", nil},
      time_stamp: {"TIMESTAMP", nil}
  end
end

defmodule TestHelper do
    def check_ready_table(struct) do
    test_data = Map.merge(struct, %{id: "test_data"})

    {:ok, _} = insert_loop(test_data)
    {:ok, _} = get_loop(test_data.__struct__, test_data.id)
    {:ok, _} = delete_loop(test_data.__struct__, test_data.id)
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

  def get_loop(module, id) do
    result = RatchetWrench.Repo.get(module, id)
    if result == nil do
      Process.sleep(1000)
      get_loop(module, id)
    else
      {:ok, result}
    end
  end

  def delete_loop(module, id) do
    case RatchetWrench.Repo.delete(module, id) do
      {:ok, result} -> {:ok, result}
      {:error} ->
        Process.sleep(1000)
        delete_loop(module, id)
    end
  end
end
