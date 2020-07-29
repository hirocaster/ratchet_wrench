defmodule RatchetWrench.SessionPool do
  use GenServer, shutdown: 10_000
  require Logger

  @session_update_boarder 60 * 55 # 55min

  @impl true
  def init(pool) do
    Process.flag(:trap_exit, true)
    set_interval()
    {:ok, pool}
  end

  def session_monitor_interval do
    env = System.get_env("RATCHET_WRENCH_SESSION_MONITOR_INTERVAL")
    if env do
      env |> String.to_integer()
    else
      60000 # 60sec
    end
  end

  def session_bust_num do
    env = System.get_env("RATCHET_WRENCH_SESSION_BUST")
    if env do
      env |> String.to_integer()
    else
      30
    end
  end

  def session_min do
    env = System.get_env("RATCHET_WRENCH_SESSION_MIN")
    if env do
      env |> String.to_integer()
    else
      10
    end
  end

  def session_max do
    System.get_env("RATCHET_WRENCH_SESSION_MAX") || 1000
  end

  def start_link(pool \\ %RatchetWrench.Pool{}) do
    if Enum.empty?(pool.idle) do
      sessions = batch_create_session(session_min())
      pool = %RatchetWrench.Pool{idle: sessions}
      GenServer.start_link(__MODULE__, pool, name: __MODULE__)
    else
      GenServer.start_link(__MODULE__, pool, name: __MODULE__)
    end
  end

  def batch_create_session(session_num) do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    RatchetWrench.batch_create_session(connection, session_num)
  end

  def checkout do
    GenServer.call(__MODULE__, :checkout, :infinity)
  end

  def checkin(session) do
    GenServer.cast(__MODULE__, {:checkin, session})
  end

  def delete_over_idle_sessions do
    GenServer.call(__MODULE__, :delete_over_idle_sessions, :infinity)
  end


  def is_safe_session?(session) do
    session_use_time = DateTime.diff(DateTime.utc_now, session.approximateLastUseTime)
    if session_use_time < @session_update_boarder do
      true
    else
      false
    end
  end

  def session_bust(pool) do
    if Enum.count(pool.idle) < (session_min() / 2) do
      sessions_list = pool
                      |> calculation_session_bust_num()
                      |> session_batch_create()


      pool = Map.merge(pool, %RatchetWrench.Pool{idle: pool.idle ++ sessions_list})
      pool
    else
      pool
    end
  end

  def calculation_session_bust_num(pool) do
    total_session_now = Enum.count(pool.idle ++ pool.checkout)
    limit_create_session_num = session_max() - total_session_now

    if session_bust_num() < limit_create_session_num do
      session_bust_num()
    else
      limit_create_session_num
    end
  end

  def session_batch_create(num) do
    RatchetWrench.token
    |> RatchetWrench.connection
    |> RatchetWrench.batch_create_session(num)
  end

  def set_interval do
    Process.send_after(self(), :session_pool_monitor, session_monitor_interval())
  end

  def pool do
    GenServer.call(__MODULE__, :pool)
  end

  def session_all_clear do
    GenServer.call(__MODULE__, :session_all_clear)
  end

  @impl GenServer
  def handle_info(:session_pool_monitor, pool) do
    # IO.inspect pool
    set_interval()
    {:noreply, session_bust(pool)}
  end

  @impl GenServer
  def handle_info(:kill, pool) do
    {:stop, :normal, pool}
  end

  def kill do
    GenServer.cast(__MODULE__, :kill)
  end

  @impl GenServer
  def handle_call(:checkout, _from, pool) do
    {session, pool} = checkout_safe_session(pool)
    {:reply, session, pool}
  end

  @impl GenServer
  def handle_call(:pool, _from, pool) do
    {:reply, pool, pool}
  end

  @impl GenServer
  def handle_call(:which_children, {pid, _ref}, pool) do
    {:reply, pid, pool}
  end

  @impl GenServer
  def handle_call({:session_bust, session_list}, _from, pool) do
    pool = Map.merge(pool, %{idle: pool.idle ++ session_list})
    {:reply, pool, pool}
  end

  @impl GenServer
  def handle_call(:delete_over_idle_sessions, _from, pool) do
    pool = do_delete_over_idle_sessions(pool)
    {:reply, pool, pool}
  end

  @impl GenServer
  def handle_call(:terminate_child, _from, pool) do
    session_all_clear(pool)
    {:stop, :shutdown, pool}
  end

  defp do_delete_over_idle_sessions(pool) do
    idle_sessions = pool.idle
    delete_session = List.last(idle_sessions)
    pool = Map.merge(pool, %{idle: List.delete_at(idle_sessions, -1)})

    if Enum.count(pool.idle) > RatchetWrench.SessionPool.session_min() do
      connection = RatchetWrench.token_data() |> RatchetWrench.connection()
      {:ok, _} = RatchetWrench.delete_session(connection, delete_session)
      do_delete_over_idle_sessions(pool)
    else
      connection = RatchetWrench.token_data() |> RatchetWrench.connection()
      {:ok, _} = RatchetWrench.delete_session(connection, delete_session)
      pool
    end
  end

  def checkout_safe_session(pool) do
    if Enum.empty?(pool.idle) do
      session = new_session()
      checkout = pool.checkout ++ [session]
      pool = Map.merge(pool, %{checkout: checkout})
      {session, pool}
    else
      [session | tail] = pool.idle
      pool = Map.merge(pool, %{idle: tail})


      if is_safe_session?(session) do
        checkout = pool.checkout ++ [session]
        pool = Map.merge(pool, %{checkout: checkout})

        {session, pool}
      else
        checkout_safe_session(pool)
      end
    end
  end

  # Sample data
  # bench mark: 100/sec 1node
  # bench mark: 1000/10sec 1node
  # bench mark: 10000/100sec 1node
  defp new_session do
    RatchetWrench.token
    |> RatchetWrench.connection
    |> RatchetWrench.create_session
  end

  @impl GenServer
  def handle_cast({:checkin, session}, pool) do
    if is_safe_session?(session) do
      pool = remove_session_in_checkout_pool(pool, session)
      pool = Map.merge(pool, %{idle: pool.idle ++ [session]})
      {:noreply, pool}
    else
      pool = remove_session_in_checkout_pool(pool, session)
      pool = Map.merge(pool, %{idle: pool.idle ++ [new_session()]})
      {:noreply, pool}
    end
  end

  defp remove_session_in_checkout_pool(pool, session) do
    checkout_sessions = Enum.reduce(pool.checkout, [], fn(checkout_session, acc) ->
                          if session.name == checkout_session.name do
                            acc
                          else
                            acc ++ [checkout_session]
                          end
                        end)
    Map.merge(pool, %{checkout: checkout_sessions})
  end

  @impl GenServer
  def terminate(_reason, pool) do
    session_all_clear(pool)
    {:ok}
  end

  def delete_session(session) do
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)
    {:ok, _} = RatchetWrench.delete_session(connection, session)
    # TODO: error case
  end

  defp session_all_clear(pool) do
    connection = RatchetWrench.token_data() |> RatchetWrench.connection()

    Enum.each(pool.idle, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
    Enum.each(pool.checkout, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
  end
end
