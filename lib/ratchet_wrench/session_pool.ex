defmodule RatchetWrench.SessionPool do
  use GenServer, shutdown: 10_000
  require Logger

  # TODO: Change interval
  # @default_interval 60 * 1000 # 60 seconds
  @default_interval 1000 # 1 seconds

  @impl true
  def init(pool) do
    Process.flag(:trap_exit, true)
    set_interval()
    {:ok, pool}
  end

  def start_link(pool \\ []) do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    pool = RatchetWrench.batch_create_session(connection, 3)
    GenServer.start_link(__MODULE__, pool, name: __MODULE__)
  end

  def checkout do
    GenServer.call(__MODULE__, :checkout)
  end

  def checkin(connection) do
    GenServer.cast(__MODULE__, {:checkin, connection})
  end

  def set_interval do
    Process.send_after(self(), :update_expire, @default_interval)
  end

  def session_all_clear do
    GenServer.call(__MODULE__, :session_all_clear)
  end

  @impl GenServer
  def handle_info(:update_expire, pool) do
    IO.inspect pool # TODO: remove
    set_interval()
    {:noreply, pool}
  end

  def kill do
    GenServer.cast(__MODULE__, :kill)
  end

  @impl GenServer
  def handle_call(:checkout, _from, pool) do
    if Enum.empty?(pool) do
      {:reply, new_session(), pool}
    else
      [session | tail] = pool
      {:reply, session, tail}
    end
  end

  defp new_session do
    RatchetWrench.token
    |> RatchetWrench.connection
    |> RatchetWrench.create_session
  end

  @impl GenServer
  def handle_call(:which_children, {pid, ref}, pool) do
    {:reply, pid, pool}
  end

  @impl GenServer
  def handle_cast({:checkin, session}, pool) do
    {:noreply, pool ++ [session]}
  end

  @impl GenServer
  def handle_info(:kill, pool) do
    {:stop, :normal, pool}
  end

  @impl GenServer
  def terminate(_reason, pool) do
    session_all_clear(pool)
    {:ok}
  end

  defp session_all_clear(pool) do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)

    Enum.each(pool, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
  end
end
