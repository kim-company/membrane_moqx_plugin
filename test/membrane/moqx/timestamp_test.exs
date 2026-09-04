defmodule Membrane.MOQX.TimestampTest do
  use ExUnit.Case, async: true

  alias Membrane.MOQX.Timestamp

  test "converts track timestamps to Membrane time with nearest-integer rounding" do
    assert Timestamp.to_membrane_time(90_000, 90_000) == {:ok, 1_000_000_000}
    assert Timestamp.to_membrane_time(3_003, 90_000) == {:ok, 33_366_667}
  end

  test "converts monotonic Membrane PTS to a track timescale with nearest-integer rounding" do
    assert Timestamp.from_membrane_time(1_000_000_000, 90_000) == {:ok, 90_000}
    assert Timestamp.from_membrane_time(33_366_667, 90_000, 0) == {:ok, 3_003}
    assert Timestamp.from_membrane_time(250_000_000, 2, 0) == {:ok, 1}

    assert Timestamp.from_membrane_time(33_366_666, 90_000, 33_366_667) ==
             {:error, :non_monotonic_pts}
  end

  test "rejects invalid timescales and negative timestamps" do
    assert Timestamp.to_membrane_time(0, 0) == {:error, :invalid_timescale}
    assert Timestamp.to_membrane_time(-1, 90_000) == {:error, :invalid_timestamp}
    assert Timestamp.from_membrane_time(-1, 90_000) == {:error, :invalid_pts}
  end
end
