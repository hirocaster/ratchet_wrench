defmodule RatchetWrench.Mutex do
  defmacro lock([do: block]) do
    quote do
      lock = Mutex.await(RatchetWrenchSessionPoolMutex, :pool_lock)
      result = unquote(block)
      Mutex.release(RatchetWrenchSessionPoolMutex, lock)
      result
    end
  end
end
