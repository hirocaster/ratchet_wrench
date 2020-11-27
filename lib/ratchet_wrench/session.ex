defmodule RatchetWrench.Session do
  def create(connection) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_create(
           connection,
           RatchetWrench.database()
         ) do
      {:ok, session} -> {:ok, session}
      {:error, reason} -> {:error, reason.body}
    end
  end

  def delete(connection, session) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_delete(
           connection,
           session.name
         ) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason.body}
    end
  end
end
