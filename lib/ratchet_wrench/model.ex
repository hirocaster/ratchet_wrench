defmodule RatchetWrench.Model do
  @moduledoc """
  Define struct module of record in database.

  ## Examples

    ```elixir
    defmodule Data do
      use RatchetWrench.Model

      schema do
        uuid :data_id
        attributes data_id: {"STRING", nil},
          string: {"STRING", ""},
          bool: {"BOOL", nil },
          int: {"INT64", nil},
          float: {"FLOAT64", nil},
          date: {"DATE", nil},
          time_stamp: {"TIMESTAMP", nil}
      end

    end
    ```

  """

  defmacro __using__(_) do
    quote do

      table_name = __MODULE__
      |> Atom.to_string
      |> String.split(".")
      |> List.last
      |> Macro.underscore
      |> String.downcase
      |> Inflex.pluralize

      default_table_name = "#{table_name}"

      Module.put_attribute(__MODULE__, :table_name, default_table_name)
      Module.register_attribute(__MODULE__, :uuid, accumulate: false)
      Module.register_attribute(__MODULE__, :attributes, accumulate: true)

      import RatchetWrench.Model
    end
  end

  defmacro schema([do: block]) do
    do_schema(block)
  end

  defp do_schema(block) do
    quote do
      unquote(block)

      table_name = Module.get_attribute(__ENV__.module, :table_name)
      Module.put_attribute(__ENV__.module, :table_name, table_name)

      uuid = Module.get_attribute(__ENV__.module, :uuid)
      Module.put_attribute(__ENV__.module, :uuid, uuid)

      Module.eval_quoted __ENV__, [
        RatchetWrench.Model.__defstruct__(__ENV__.module),
        RatchetWrench.Model.__valid_define_uuid__!(__ENV__.module),
        RatchetWrench.Model.__def_helper_funcs__(__ENV__.module)
      ]
    end
  end

  def __defstruct__(target) do
    quote bind_quoted: [target: target] do
      attributes = Module.get_attribute(target, :attributes)
      fields = attributes |> Enum.map(fn {name, {_type, default}} -> {name, default} end)
      defstruct fields
    end
  end

  def __valid_define_uuid__!(mod) do
    attributes = Module.get_attribute(mod, :attributes)
    uuid         = Module.get_attribute(mod, :uuid)

    result = attributes
    |> Enum.map(fn {name, {_type, _default}} -> "#{name}" == "#{uuid}" end)
    |> Enum.any?

    if result == false do
      raise "Not define uuid in #{mod} module schema"
    end
  end

  def __def_helper_funcs__(mod) do
    table_name           = Module.get_attribute(mod, :table_name)
    attributes           = Module.get_attribute(mod, :attributes)
    uuid                   = Module.get_attribute(mod, :uuid)

    quote do
      def __table_name__, do: unquote(table_name)
      def __attributes__, do: unquote(attributes)
      def __uuid__, do: unquote(uuid)
    end
  end

  defmacro table_name(table_name) do
    quote bind_quoted: [table_name: table_name] do
      RatchetWrench.Model.__table_name__(__MODULE__, table_name)
    end
  end

  def __table_name__(mod, table_name) do
    Module.put_attribute(mod, :table_name, table_name)
  end

  defmacro attributes(decl) do
    {list_of_attrs, _} = Code.eval_quoted(decl)
    for attr <- list_of_attrs do
      quote do: attribute([unquote(attr)])
    end
  end

  defmacro attribute(decl) do
    quote bind_quoted: [decl: decl] do
      {name, type, default} = case decl do
                       [{name, {type, default}}] -> {name, type, default}
                     end
      RatchetWrench.Model.__attribute__(__MODULE__, name, type, default)
    end
  end

  def __attribute__(mod, name, type, default) do
    Module.put_attribute(mod, :attributes, {name, {type, default}})
  end

  defmacro uuid(uuid) do
    quote bind_quoted: [uuid: uuid] do
      RatchetWrench.Model.__uuid__(__MODULE__, uuid)
    end
  end

  def __uuid__(mod, uuid) do
    Module.put_attribute(mod, :uuid, uuid)
  end
end
