defmodule RatchetWrench.BackupTest do
  use ExUnit.Case
  use Timex

  setup do
    env_scope = RatchetWrench.token_scope()
    System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", "https://www.googleapis.com/auth/spanner.admin")

    on_exit fn ->
      System.put_env("RATCHET_WRENCH_TOKEN_SCOPE", env_scope)
    end
  end

  test "create/list/delete" do
    backup_id = "create_by_test"
    expire_time = DateTime.utc_now |> Timex.shift(days: +365)

    case RatchetWrench.Backup.create(backup_id, expire_time) do
      {:ok, result} ->
        assert result.__struct__ == GoogleApi.Spanner.V1.Model.Operation
      {:error, client} ->
        IO.inspect client
        assert false
    end

    case RatchetWrench.Backup.list() do
      {:ok, result} ->
        assert result.__struct__ == GoogleApi.Spanner.V1.Model.ListBackupsResponse
        assert Enum.count(result.backups) == 1
      {:error, client} ->
        IO.inspect client
        assert false
    end

    case RatchetWrench.Backup.delete(backup_id) do
      {:ok, result} ->
        assert result.__struct__ == GoogleApi.Spanner.V1.Model.Empty
      {:error, client} ->
        IO.inspect client
        assert false
    end
  end
end
