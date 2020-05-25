defmodule RatchetWrench.DateTime do
  @moduledoc """
  Convert timestamps timezone of between local system(TZ) to Cloud Spanner(UTC).
  """

  def now() do
    tz = System.get_env("TZ")

    if tz == nil  do
      DateTime.utc_now
    else
      now(tz)
    end
  end

  def now(tz_name) do
    {:ok, datetime} = DateTime.now(tz_name, Tzdata.TimeZoneDatabase)
    datetime
  end
end
