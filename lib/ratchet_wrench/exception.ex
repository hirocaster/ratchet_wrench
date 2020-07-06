defmodule RatchetWrench.Exception do
  defexception message: "Exception in RatchetWrench."
end
defmodule RatchetWrench.Exception.TransactionError do
  defexception message: "Raise exception in transaction."
end
defmodule RatchetWrench.Exception.PkCountMissMatchInListError do
  defexception message: "Pk count mismatch in args List type."
end
defmodule RatchetWrench.Exception.APIRequestError do
  defexception [message: "Request API error.", client: nil]

  @impl true
  def exception(value) do
    msg = "Request API error. client: #{inspect(value)}"
    %RatchetWrench.Exception.APIRequestError{message: msg, client: value}
  end
end
