defmodule RatchetWrench.SessionPool do
  use GenServer, shutdown: 10_000
  require RatchetWrench.Mutex
  require Logger

  # TODO: Change interval
  # @default_interval 60 * 1000 # 60 seconds
  @default_interval 1000 # 1 seconds
  @session_update_boarder 60 * 55 # 55min

  @impl true
  def init(pool) do
    Process.flag(:trap_exit, true)
    set_interval()
    {:ok, pool}
  end

  defp startup_connection_num do
    3
  end

  def start_link(pool \\ %RatchetWrench.Pool{}) do
    if Enum.empty?(pool.idle) do
      sessions = batch_create_session(startup_connection_num())
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
    RatchetWrench.Mutex.lock do
      GenServer.call(__MODULE__, :checkout)
    end
  end

  def checkin(session) do
    if is_safe_session?(session) do
      RatchetWrench.Mutex.lock do
        GenServer.call(__MODULE__, {:checkin, session})
      end
    else
        renew_session = new_session()
        RatchetWrench.Mutex.lock do
          GenServer.call(__MODULE__, {:checkin, renew_session})
        end
      delete_session(session)
    end
  end

  def is_safe_session?(session) do
    session_use_time = DateTime.diff(DateTime.utc_now, session.approximateLastUseTime)
    if session_use_time < @session_update_boarder do
      true
    else
      false
    end
  end

  def set_interval do
    Process.send_after(self(), :update_expire, @default_interval)
  end

  def pool do
    GenServer.call(__MODULE__, :pool)
  end

  def session_all_clear do
    GenServer.call(__MODULE__, :session_all_clear)
  end

  @impl GenServer
  def handle_info(:update_expire, pool) do
    # IO.inspect pool # TODO: remove
    # expire_check(pool)
    set_interval()
    {:noreply, pool}
  end

  @impl GenServer
  def handle_info(:kill, pool) do
    {:stop, :normal, pool}
  end

  # @update_boarder_time = 60  * 10 # 10min
  @update_boarder_time 3

  def expire_check(pool) do
    Enum.map(pool, fn(session) ->
      approximate_last_use_time = session.approximateLastUseTime
      if DateTime.diff(DateTime.utc_now, approximate_last_use_time) > @update_boarder_time do
        {:ok, _} = RatchetWrench.ping(session)
        # TODO: fetch session data by API(need update approximateLastUseTime)
        session
      else
        session
      end
    end)
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
  def handle_call({:checkin, session}, _from, pool) do
    checkout_sessions = Enum.reduce(pool.checkout, [], fn(checkout_session, acc) ->
                          if session.name != checkout_session.name do
                            acc ++ [checkout_session]
                          else
                            acc
                          end
                        end)
    pool = Map.merge(pool, %{checkout: checkout_sessions})
    pool = Map.merge(pool, %{idle: pool.idle ++ [session]})
    {:reply, pool, pool}
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

  # @impl GenServer
  # def handle_cast({:checkin, session}, pool) do
  #   IO.puts "!!! debug handle_cast checkin !!!"
  #   IO.inspect session
  #   IO.inspect pool
  #   IO.inspect pool ++ [session]
  #   {:noreply, pool ++ [session]}
  # end

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
    token = RatchetWrench.token
    connection = RatchetWrench.connection(token)

    Enum.each(pool.idle, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
    Enum.each(pool.checkout, fn(session) ->
      {:ok, _} = RatchetWrench.delete_session(connection, session)
    end)
  end
end
