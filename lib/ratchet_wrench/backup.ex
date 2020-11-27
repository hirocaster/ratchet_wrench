defmodule RatchetWrench.Backup do
  @spec create(binary, DateTime.t()) ::
          {:ok, GoogleApi.Spanner.V1.Model.Operation.t()} | {:error, Tesla.Env.t()}
  def create(backup_id, expire_time) when is_binary(backup_id) do
    connection = RatchetWrench.token() |> RatchetWrench.connection()
    project_id = RatchetWrench.project_id()
    instance_id = RatchetWrench.instance_id()

    parent = "projects/#{project_id}/instances/#{instance_id}"
    database = RatchetWrench.database()

    backup = %GoogleApi.Spanner.V1.Model.Backup{database: database, expireTime: expire_time}

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_backups_create(
           connection,
           parent,
           [{:backupId, backup_id}, {:body, backup}]
         ) do
      {:ok, result} -> {:ok, result}
      {:error, client} -> {:error, client}
    end
  end

  @spec list() ::
          {:ok, GoogleApi.Spanner.V1.Model.ListBackupsResponse.t()} | {:error, Tesla.Env.t()}
  def list() do
    connection = RatchetWrench.token() |> RatchetWrench.connection()
    parent = "projects/#{RatchetWrench.project_id()}/instances/#{RatchetWrench.instance_id()}"

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_backups_list(
           connection,
           parent
         ) do
      {:ok, result} -> {:ok, result}
      {:error, client} -> {:error, client}
    end
  end

  @spec delete(binary) :: {:ok, GoogleApi.Spanner.V1.Model.Empty.t()} | {:error, Tesla.Env.t()}
  def delete(name) when is_binary(name) do
    connection = RatchetWrench.token() |> RatchetWrench.connection()
    project_id = RatchetWrench.project_id()
    instance_id = RatchetWrench.instance_id()

    name = "projects/#{project_id}/instances/#{instance_id}/backups/#{name}"

    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_backups_delete(
           connection,
           name
         ) do
      {:ok, result} -> {:ok, result}
      {:error, client} -> {:error, client}
    end
  end
end
