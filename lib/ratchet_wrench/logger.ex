defmodule RatchetWrench.Logger do
  @moduledoc """
  Logger for RatchetWrench
  """

  require Logger

  def info(message) do
    if enable_logging() do
      Logger.info(message)
    end
  end

  def error(message) do
    if enable_logging() do
      Logger.error(message)
    end
  end

  defp enable_logging do
    if System.get_env("RATCHET_WRENCH_ENABLE_LOGGING") do
      true
    else
      if Application.fetch_env(:ratchet_wrench, :enable_logging) == :error do
        nil
      else
        true
      end
    end
  end
end
