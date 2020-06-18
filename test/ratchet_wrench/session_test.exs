defmodule RatchetWrench.SessionTest do
  use ExUnit.Case

  test ".create/1" do
    token = RatchetWrench.token()
    connection = RatchetWrench.connection(token)
    {:ok, session} = RatchetWrench.Session.create(connection)

    assert session.__struct__ == GoogleApi.Spanner.V1.Model.Session
    {:ok, database_path} = System.fetch_env("RATCHET_WRENCH_DATABASE")
    assert session.name =~ database_path
  end

  describe "Bad database path in config" do
    setup do
      {:ok, env_database} = System.fetch_env("RATCHET_WRENCH_DATABASE")
      System.put_env("RATCHET_WRENCH_DATABASE", "bad/database/path")
      on_exit fn ->
        System.put_env("RATCHET_WRENCH_DATABASE", env_database)
      end
    end

    test "Database path config error" do
      token = RatchetWrench.token()
      connection = RatchetWrench.connection(token)

      {:error, reason} = RatchetWrench.Session.create(connection)
      assert reason =~ "Error 404 (Not Found)"
      assert reason =~ "was not found on this server."
    end
  end
end
