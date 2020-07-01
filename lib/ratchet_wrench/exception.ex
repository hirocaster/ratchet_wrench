defmodule RatchetWrench.Exception do
  defexception message: "Exception in RatchetWrench."
end
defmodule RatchetWrench.Exception.TransactionError do
  defexception message: "Raise exception in transaction."
end
