ExUnit.start()
ExUnit.configure(timeout: :infinity)
Faker.start()

defmodule RatchetWrench.DateTimeTest do
  use ExUnit.Case

  test ".now/0" do
    now_tz = System.get_env("TZ")
    now_datetime = RatchetWrench.DateTime.now()
    if now_tz == nil do
      assert now_datetime.time_zone == "Etc/UTC"
    else
      assert now_datetime.time_zone == now_tz
    end
  end

  describe "Set TZ='Asia/Tokyo' environment" do
    setup do
      now_tz = System.get_env("TZ")
      System.put_env("TZ", "Asia/Tokyo")

      on_exit(fn ->
        if now_tz == nil do
          System.delete_env("TZ")
        else
          System.put_env("TZ", now_tz)
        end
      end)
    end

    test ".now/0 at set TZ environment" do
      now_datetime = RatchetWrench.DateTime.now()
      assert now_datetime.time_zone == "Asia/Tokyo"
    end
  end

  test ".now/1" do
    now_datetime = RatchetWrench.DateTime.now("Asia/Tokyo")
    assert now_datetime.time_zone == "Asia/Tokyo"
  end

  test ".trancate_suffix_zero/1" do
    {:ok, dt, 0} = DateTime.from_iso8601("2020-06-30 05:11:40.028120Z")
    assert "#{RatchetWrench.DateTime.trancate_suffix_zero(dt)}" == "2020-06-30 05:11:40.02812Z"
    {:ok, dt, 0} = DateTime.from_iso8601("2020-06-30 05:11:40.028100Z")
    assert "#{RatchetWrench.DateTime.trancate_suffix_zero(dt)}" == "2020-06-30 05:11:40.0281Z"
    {:ok, dt, 0} = DateTime.from_iso8601("2020-06-30 05:11:40.020000Z")
    assert "#{RatchetWrench.DateTime.trancate_suffix_zero(dt)}" == "2020-06-30 05:11:40.02Z"
  end
end
