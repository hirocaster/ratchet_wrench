defmodule RatchetWrench.Repo do
  def get(module, id) do

    struct = to_struct(module)
    table_name = to_table_name(struct)
    pk_name = module.__pk__

    sql = "SELECT * FROM #{table_name} WHERE #{pk_name} = '#{id}'"

    {:ok, result_set} = RatchetWrench.select_execute_sql(sql)

    if result_set.rows == nil do
      nil
    else
      convert_result_set_to_value_list(struct, result_set) |> List.first()
    end
  end

  def where(struct, where_string) do
    table_name = to_table_name(struct)
    sql = "SELECT * FROM #{table_name} WHERE #{where_string}"

    case RatchetWrench.select_execute_sql(sql) do
      {:ok, result_set} ->
        if result_set.rows == nil do
          {:ok, []}
        else
          {:ok, convert_result_set_to_value_list(struct, result_set)}
        end
      {:error, reason} -> {:error, reason}
    end
  end

  def insert(struct) do
    table_name = to_table_name(struct)

    now_timestamp = RatchetWrench.DateTime.now()

    map = set_pk_value(struct)
          |> set_inserted_at_value(now_timestamp)
          |> set_updated_at_value(now_timestamp)
          |> Map.from_struct

    column_list = Map.keys(map)
    column_list_string = Enum.join(column_list, ", ")

    value_list = Map.values(map)
    values_list_string = Enum.reduce(value_list, "", fn(x, acc) ->
      if acc == "" do
        convert_value(x)
      else
        acc <> ", " <> convert_value(x)
      end
    end)

    sql = "INSERT INTO #{table_name}(#{column_list_string}) VALUES(#{values_list_string})"
    case RatchetWrench.execute_sql(sql) do
      {:ok, _} -> {:ok, Map.merge(struct, map)}
      {:error, reason} -> {:error, reason}
    end
  end

  def insert_sql(struct) do
    table_name = to_table_name(struct)

    now_timestamp = RatchetWrench.DateTime.now()

    map = set_pk_value(struct)
          |> set_inserted_at_value(now_timestamp)
          |> set_updated_at_value(now_timestamp)
          |> Map.from_struct

    column_list = Map.keys(map)
    column_list_string = Enum.join(column_list, ", ")

    value_list = Map.values(map)
    values_list_string = Enum.reduce(value_list, "", fn(x, acc) ->
      if acc == "" do
        convert_value(x)
      else
        acc <> ", " <> convert_value(x)
      end
    end)

    "INSERT INTO #{table_name}(#{column_list_string}) VALUES(#{values_list_string})"
  end

  def set(struct) do
    table_name = to_table_name(struct)
    result_struct = set_updated_at_value(struct)
    map = Map.from_struct(result_struct)
    pk_name = struct.__struct__.__pk__

    {:ok, pk_value} = Map.fetch(map, pk_name)

    set_values_map = remove_pk(map, pk_name)

    set_value_list = Enum.reduce(set_values_map, [], fn({key, value}, acc) -> acc ++ ["#{key}" <> " = " <> convert_value(value)] end)
    set_value_string = Enum.join(set_value_list, ", ")

    sql = "UPDATE #{table_name} SET #{set_value_string} WHERE #{pk_name} = #{convert_value(pk_value)}"
    case RatchetWrench.execute_sql(sql) do
      {:ok, _} -> {:ok, result_struct}
      {:error, reason} -> {:error, reason}
    end
  end

  def update_sql(struct) do
    table_name = to_table_name(struct)
    result_struct = set_updated_at_value(struct)

    map = Map.from_struct(result_struct)
    pk_name = struct.__struct__.__pk__

    {:ok, pk_value} = Map.fetch(map, pk_name)

    set_values_map = remove_pk(map, pk_name)

    set_value_list = Enum.reduce(set_values_map, [], fn({key, value}, acc) -> acc ++ ["#{key}" <> " = " <> convert_value(value)] end)
    set_value_string = Enum.join(set_value_list, ", ")

    "UPDATE #{table_name} SET #{set_value_string} WHERE #{pk_name} = #{convert_value(pk_value)}"
  end

  def delete(module, pk_value) do
    struct = to_struct(module)
    table_name = to_table_name(struct)
    pk_name = module.__pk__

    sql = "DELETE FROM #{table_name} WHERE #{pk_name} = #{convert_value(pk_value)}"

    case RatchetWrench.execute_sql(sql) do
      {:ok, result_set} -> {:ok, result_set}
      {:error, reason} -> {:error, reason}
    end
  end

  def all(struct, where_string) when is_binary(where_string) do
    table_name = to_table_name(struct)
    sql = "SELECT * FROM #{table_name} WHERE #{where_string}"
    do_all(struct, sql)
  end

  def all(struct) do
    table_name = to_table_name(struct)
    sql = "SELECT * FROM #{table_name}"
    do_all(struct, sql)
  end

  def do_all(struct, sql) do
    {:ok, result_set_list} = RatchetWrench.auto_limit_offset_execute_sql(sql)
    Enum.reduce(result_set_list, [], fn(result_set, acc) ->
      acc ++ convert_result_set_to_value_list(struct, result_set)
    end)
  end

  # %{ name: type, name: type}
  def convert_metadata_rowtype_fields_to_map(fields) do
    Enum.reduce(fields, %{}, fn(field, acc) -> Map.merge(acc, %{"#{field.name}": field.type.code}) end)
  end

  def convert_result_set_to_value_list(struct, result_set) do
    converted_rows = Enum.map(result_set.rows, fn(row) ->
      converted_row_list = Enum.map(Enum.with_index(result_set.metadata.rowType.fields), fn({field, index}) ->
                               name = field.name
                               type = field.type.code
                               value =  Enum.at(row, index)
                               converted_value = convert_value_type(value, type)
                               %{"#{name}": converted_value}
                             end)

      row_map = Enum.reduce(converted_row_list, %{}, fn(x, acc) -> Map.merge(x, acc) end)
      Map.merge(struct, row_map)
    end)

    converted_rows
  end

  def remove_pk(map, pk_name) do
    Map.delete(map, pk_name)
  end

  def set_pk_value(struct) do
    pk_name = struct.__struct__.__pk__

    if Map.has_key?(struct, pk_name) do
      if Map.fetch(struct, pk_name) == {:ok, nil} || Map.fetch(struct, pk_name) == {:ok, ""} do
        {map, _} = Code.eval_string("%{#{pk_name}: UUID.uuid4()}")
        Map.merge(struct, map)
      else
        struct
      end
    else
      struct
    end
  end

  def set_inserted_at_value(struct, now_timestamp \\ RatchetWrench.DateTime.now) do
    if Map.has_key?(struct, :inserted_at) do
      Map.merge(struct, %{inserted_at: now_timestamp})
    else
      struct
    end
  end

  def set_updated_at_value(struct, now_timestamp \\ RatchetWrench.DateTime.now) do
    if Map.has_key?(struct, :updated_at) do
      Map.merge(struct, %{updated_at: now_timestamp})
    else
      struct
    end
  end

  def convert_value(value) when is_nil(value), do: "NULL"
  def convert_value(value) when is_float(value), do: Float.to_string(value)
  def convert_value(value) when is_integer(value), do: Integer.to_string(value)
  def convert_value(value) when is_boolean(value) do
    if value do
      "TRUE"
    else
      "FALSE"
    end
  end
  def convert_value(value) do
    if is_map(value) do
      if Map.has_key?(value, :__struct__) do
        if value.__struct__ == DateTime do
          {:ok, utc_datetime} = DateTime.shift_zone(value, "Etc/UTC", Tzdata.TimeZoneDatabase)
          "'#{utc_datetime}'"
        else
          "'#{value}'"
        end
      end
    else
      case value do
        "" -> "''"
        _ -> "'#{value}'"
      end
    end
  end

  def convert_value_type(value, type) do
    # https://cloud.google.com/spanner/docs/data-types
    case type do
      # Unsupport types
      # "ARRAY" -> convert_array(value)
      # "BYTES" -> convert_bytes(value)
      # "STRUCT" -> convert_struct(value)
      "STRING"    -> convert_string(value)
      "DATE"      -> convert_date(value)
      "BOOL"      -> convert_bool(value)
      "INT64"     -> convert_int64(value)
      "FLOAT64"   -> convert_float64(value)
      "TIMESTAMP" -> convert_timestamp(value)
      _           -> raise "unsupport type at def convet_value_type in RatchetWrench"
    end
  end


  def convert_string(value) when is_nil(value), do: nil
  def convert_string(value) do
    if is_binary(value) do
      value
    else
      value.to_string()
    end
  end

  def convert_date(value) do
    case value do
      nil -> nil
      _ -> {:ok, date} = Date.from_iso8601(value)
           date
    end
  end

  def convert_bool(value) when is_nil(value), do: nil
  def convert_bool(value) do
    case value do
      true -> true
      false -> false
      _ -> raise "unknown value in type bool: #{value}"
    end
  end

  def convert_int64(value) when is_nil(value), do: nil
  def convert_int64(value), do: String.to_integer(value)

  def convert_float64(value) when is_nil(value), do: nil
  def convert_float64(value) when is_float(value), do: value

  def convert_timestamp(value) when is_nil(value), do: nil
  def convert_timestamp(value) when is_binary(value) do
    {:ok, timestamp, _} = DateTime.from_iso8601(value)
    tz = System.get_env("TZ")

    if tz == nil  do
      timestamp
    else
      {:ok, datetime} = DateTime.shift_zone(timestamp, tz, Tzdata.TimeZoneDatabase)
      datetime
    end
  end

  defp to_struct(module) do
    {struct, _} = Code.eval_string("%#{module}{}")
    struct
  end

  defp to_table_name(struct) do
    table_name = struct.__struct__
      |> to_string
      |> String.split(".")
      |> List.last
      |> String.downcase
      |> Inflex.pluralize
    table_name
  end
end
