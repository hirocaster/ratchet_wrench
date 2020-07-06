defmodule RatchetWrench.Repo do
  def get(module, pk_value_list) do
    valid_pk_value_list!(module, pk_value_list)

    sql = get_sql(module)
    params = params_pk_map(module, pk_value_list)

    {:ok, result_set} = RatchetWrench.select_execute_sql(sql, params)

    if result_set.rows == nil do
      nil
    else
      struct = to_struct(module)
      convert_result_set_to_value_list(struct, result_set) |> List.first()
    end
  end

  def get_sql(module) do
    table_name = module.__table_name__
    base_sql = "SELECT * FROM #{table_name} WHERE "
    base_sql <> where_pk_sql(module)
  end

  def where_pk_sql(module) do
    pk_list = module.__pk__
    Enum.reduce(pk_list, "", fn(pk_name, acc) ->
      if acc == "" do
        "#{pk_name} = @#{pk_name}"
      else
        acc <> " AND #{pk_name} = @#{pk_name}"
      end
    end)
  end

  def params_pk_map(module, pk_value_list) do
    pk_list = module.__pk__
    Enum.with_index(pk_list)
    |> Enum.reduce(%{}, fn({pk_name, index}, acc) ->
      {value, _} = List.pop_at(pk_value_list, index)
      Map.merge(acc, Map.put(%{}, pk_name, value))
    end)
  end

  def valid_pk_value_list!(module, pk_value_list) do
    pk_count = Enum.count(module.__pk__)
    value_count = Enum.count(pk_value_list)

    unless pk_count == value_count do
      raise "Not match count for pk in list_args"
    end
  end

  def where(struct, where_string, params) do
    table_name = struct.__struct__.__table_name__
    sql = "SELECT * FROM #{table_name} WHERE #{where_string}"

    case RatchetWrench.select_execute_sql(sql, params) do
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
    struct = set_timestamps(struct)
    sql = insert_sql(struct)
    params = params_insert_values_map(struct)
    param_types = param_types(struct.__struct__)

    case RatchetWrench.execute_sql(sql, params, param_types) do
      {:ok, _} -> {:ok, struct}
      {:error, reason} -> {:error, reason}
    end
  end

  defp set_timestamps(struct) do
    now_timestamp = RatchetWrench.DateTime.now()
    set_uuid_value(struct)
    |> set_inserted_at_value(now_timestamp)
    |> set_updated_at_value(now_timestamp)
  end

  def insert_sql(struct) do
    table_name = to_table_name(struct)

    map = Map.from_struct(struct)

    column_list = Map.keys(map)
    column_list_string = Enum.join(column_list, ", ")

    values_list_string = Enum.reduce(map, [], fn({key, _value}, acc) ->
                           acc ++ ["@#{key}"]
                         end) |> Enum.join(", ")

    "INSERT INTO #{table_name}(#{column_list_string}) VALUES(#{values_list_string})"
  end

  def params_insert_values_map(struct) do
    now_timestamp = RatchetWrench.DateTime.now()

    set_uuid_value(struct)
    |> set_inserted_at_value(now_timestamp)
    |> set_updated_at_value(now_timestamp)
    |> Map.from_struct
    |> Enum.reduce(%{}, fn({key, value}, acc) ->
      Map.merge(acc, Map.put(%{}, key, convert_value(value)))
      end)
  end

  def params_update_values_map(struct) do
    now_timestamp = RatchetWrench.DateTime.now()

    set_uuid_value(struct)
    |> set_updated_at_value(now_timestamp)
    |> Map.from_struct
    |> Enum.reduce(%{}, fn({key, value}, acc) ->
      Map.merge(acc, Map.put(%{}, key, convert_value(value)))
      end)
  end

  def param_types(module) do
    module.__attributes__
    |> Enum.reduce(%{}, fn({key, {type, _default}}, acc) ->
       # Map.merge(acc, Map.put(%{}, key,  %GoogleApi.Spanner.V1.Model.Type{code: type}))
       Map.merge(acc, Map.put(%{}, key,  %{code: type}))
       end)
  end

  def set(struct) do
    struct = set_update_timestamp(struct)
    sql = update_sql(struct)
    params = params_update_values_map(struct)
    param_types = param_types(struct.__struct__)

    case RatchetWrench.execute_sql(sql, params, param_types) do
      {:ok, _} -> {:ok, struct}
      {:error, reason} -> {:error, reason}
    end
  end

  defp set_update_timestamp(struct) do
    now_timestamp = RatchetWrench.DateTime.now()
    set_updated_at_value(struct, now_timestamp)
  end


  def update_sql(struct) do
    table_name = struct.__struct__.__table_name__

    map = Map.from_struct(struct) |> remove_pk(struct)

    values_list_string = Enum.reduce(map, [], fn({key, _value}, acc) ->
                           acc ++ ["#{key} = @#{key}"]
                         end) |> Enum.join(", ")

    base_sql = "UPDATE #{table_name} SET #{values_list_string} WHERE "
    base_sql <> where_pk_sql(struct.__struct__)
  end

  def remove_pk(map, struct) do
    pk_list = struct.__struct__.__pk__
    Enum.reduce(pk_list, map, fn(pk_key, acc) ->
      Map.delete(acc, pk_key)
    end)
  end

  def delete(module, pk_value_list) do
    valid_pk_value_list!(module, pk_value_list)

    sql = delete_sql(module)
    params = params_pk_map(module, pk_value_list)
    param_types = param_types(module)

    case RatchetWrench.execute_sql(sql, params, param_types) do
      {:ok, result_set} -> {:ok, result_set}
      {:error, reason} -> {:error, reason}
    end
  end

  def delete_sql(module) do
    table_name = module.__table_name__
    base_sql = "DELETE FROM #{table_name} WHERE "
    base_sql <> where_pk_sql(module)
  end

  def all(struct, where_sql, params) when is_binary(where_sql) do
    table_name = struct.__struct__.__table_name__
    sql = "SELECT * FROM #{table_name} WHERE #{where_sql}"
    do_all(struct, sql, params)
  end

  def all(struct) do
    table_name = to_table_name(struct)
    sql = "SELECT * FROM #{table_name}"
    do_all(struct, sql, %{})
  end

  def do_all(struct, sql, params) do
    {:ok, result_set_list} = RatchetWrench.auto_limit_offset_execute_sql(sql, params)
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

  def remove_uuid(map, uuid_name) do
    Map.delete(map, uuid_name)
  end

  def set_uuid_value(struct) do
    uuid_name = struct.__struct__.__uuid__

    if Map.has_key?(struct, uuid_name) do
      if Map.fetch(struct, uuid_name) == {:ok, nil} || Map.fetch(struct, uuid_name) == {:ok, ""} do
        {map, _} = Code.eval_string("%{#{uuid_name}: UUID.uuid4()}")
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

  def convert_value(value) when is_nil(value), do: value
  def convert_value(value) when is_float(value), do: value
  def convert_value(value) when is_integer(value), do: Integer.to_string(value)
  def convert_value(value) when is_boolean(value) do
    if value do
      true
    else
      false
    end
  end
  def convert_value(value) do
    if is_map(value) do
      if Map.has_key?(value, :__struct__) do
        if value.__struct__ == DateTime do
          {:ok, utc_datetime} = DateTime.shift_zone(value, "Etc/UTC", Tzdata.TimeZoneDatabase)
          # Bug? in Google Cloud Spanner
          # https://cloud.google.com/spanner/docs/data-types?hl=ja#sql-%E3%82%AF%E3%82%A8%E3%83%AA
          # Must `T` between date to time at now
          String.replace("#{utc_datetime}", " ", "T")
        else
          "#{value}"
        end
      end
    else
      case value do
        "" -> ""
        _ -> "#{value}"
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

    datetime =
      if tz == nil  do
        timestamp
      else
        {:ok, datetime} = DateTime.shift_zone(timestamp, tz, Tzdata.TimeZoneDatabase)
        datetime
      end

    %{datetime | microsecond: {datetime.microsecond |> elem(0), 6} }
  end

  defp to_struct(module) do
    {struct, _} = Code.eval_string("%#{module}{}")
    struct
  end

  defp to_table_name(struct) do
    struct.__struct__.__table_name__
  end
end
