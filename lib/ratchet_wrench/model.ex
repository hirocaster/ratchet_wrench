defmodule RatchetWrench.Model do
  @moduledoc """
  Define struct module of record in database.

  ## Examples

    ```elixir
    defmodule Data do
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
    ```

  """

  defmacro __using__(_) do
    quote do

      table_name = __MODULE__
      |> Atom.to_string
      |> String.split(".")
      |> List.last

      default_table_name = "#{table_name}"

      Module.put_attribute(__MODULE__, :table_name, default_table_name)
      Module.register_attribute(__MODULE__, :pk, accumulate: false)
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

      pk = Module.get_attribute(__ENV__.module, :pk)
      Module.put_attribute(__ENV__.module, :pk, pk)

      Module.eval_quoted __ENV__, [
        RatchetWrench.Model.__defstruct__(__ENV__.module),
        RatchetWrench.Model.__valid_define_pk__!(__ENV__.module),
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

  def __valid_define_pk__!(mod) do
    attributes = Module.get_attribute(mod, :attributes)
    pk         = Module.get_attribute(mod, :pk)

    result = attributes
    |> Enum.map(fn {name, {_type, _default}} -> "#{name}" == "#{pk}" end)
    |> Enum.any?

    if result == false do
      raise "Not define pk in #{mod} module schema"
    end
  end

  def __def_helper_funcs__(mod) do
    table_name           = Module.get_attribute(mod, :table_name)
    attributes           = Module.get_attribute(mod, :attributes)
    pk                   = Module.get_attribute(mod, :pk)

    quote do
      def __table_name__, do: unquote(table_name)
      def __attributes__, do: unquote(attributes)
      def __pk__, do: unquote(pk)
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

  defmacro pk(pk) do
    quote bind_quoted: [pk: pk] do
      RatchetWrench.Model.__pk__(__MODULE__, pk)
    end
  end

  def __pk__(mod, pk) do
    Module.put_attribute(mod, :pk, pk)
  end
end
