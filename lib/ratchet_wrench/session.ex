defmodule RatchetWrench.Session do
  def create(connection) do
    case GoogleApi.Spanner.V1.Api.Projects.spanner_projects_instances_databases_sessions_create(connection, RatchetWrench.database()) do
      {:ok, session} -> {:ok, session}
      {:error, reason} -> {:error, reason.body}
    end
  end
end
