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

  def now(tz) do
    {:ok, dt_zone}  =
      DateTime.utc_now
      |> DateTime.shift_zone(tz, Tzdata.TimeZoneDatabase)
    dt_zone
  end

  def add_suffix_zero(datetime) do
    {microsecond, _} = datetime.microsecond
    Map.merge(datetime, %{microsecond: {microsecond, 6}})
  end
end
