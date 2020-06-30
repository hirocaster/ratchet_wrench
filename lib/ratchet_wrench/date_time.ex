defmodule RatchetWrench.DateTime do
  @moduledoc """
  Convert timestamps timezone of between local system(TZ) to Cloud Spanner(UTC).
  """

  def now() do
    tz = System.get_env("TZ")

    if tz == nil  do
      DateTime.utc_now |> trancate_suffix_zero()
    else
      now(tz)
    end
  end

  def now(tz) do
    {:ok, dt_zone}  =
      DateTime.utc_now
      |> trancate_suffix_zero()
      |> DateTime.shift_zone(tz, Tzdata.TimeZoneDatabase)
    dt_zone
  end

  def trancate_suffix_zero(datetime) do
    st = Regex.replace(~r/0*Z$/, "#{datetime}", "Z")
    {:ok, dt, 0} = DateTime.from_iso8601(st)
    dt
  end
end
