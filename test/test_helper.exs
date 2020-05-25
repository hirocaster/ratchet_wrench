ExUnit.start()
Faker.start()

defmodule Singer do
  @moduledoc """
  Sample model module for unit test.
  """
  use RatchetWrench.Model

  schema do
    attributes id: {"STRING", nil},
      first_name: {"STRING", ""},
      last_name: {"STRING", ""},
      created_at: {"TIMESTAMP", nil},
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
