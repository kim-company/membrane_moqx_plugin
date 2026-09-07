defmodule Membrane.MOQX.Timestamp do
  @moduledoc """
  Converts between MOQX track timestamps and Membrane nanosecond time.

  Conversion rounds to the nearest integer, with exact half values rounded up.
  Consecutive outbound PTS values must be non-decreasing; equal PTS values and
  distinct PTS values that quantize to the same track timestamp are allowed.
  """

  @nanoseconds_per_second 1_000_000_000

  @spec to_membrane_time(non_neg_integer(), pos_integer()) ::
          {:ok, Membrane.Time.non_neg()} | {:error, :invalid_timestamp | :invalid_timescale}
  def to_membrane_time(timestamp, timescale) do
    with :ok <- validate_timescale(timescale),
         :ok <- validate_non_negative(timestamp, :invalid_timestamp) do
      {:ok, round_ratio(timestamp * @nanoseconds_per_second, timescale)}
    end
  end

  @spec from_membrane_time(Membrane.Time.non_neg(), pos_integer(), Membrane.Time.non_neg() | nil) ::
          {:ok, non_neg_integer()}
          | {:error, :invalid_pts | :invalid_timescale | :non_monotonic_pts}
  def from_membrane_time(pts, timescale, previous_pts \\ nil) do
    with :ok <- validate_timescale(timescale),
         :ok <- validate_non_negative(pts, :invalid_pts),
         :ok <- validate_previous_pts(previous_pts, pts) do
      {:ok, round_ratio(pts * timescale, @nanoseconds_per_second)}
    end
  end

  defp validate_timescale(timescale) when is_integer(timescale) and timescale > 0, do: :ok
  defp validate_timescale(_timescale), do: {:error, :invalid_timescale}

  defp validate_non_negative(value, _reason) when is_integer(value) and value >= 0, do: :ok
  defp validate_non_negative(_value, reason), do: {:error, reason}

  defp validate_previous_pts(nil, _pts), do: :ok

  defp validate_previous_pts(previous_pts, pts)
       when is_integer(previous_pts) and previous_pts >= 0 and pts >= previous_pts,
       do: :ok

  defp validate_previous_pts(_previous_pts, _pts), do: {:error, :non_monotonic_pts}

  defp round_ratio(numerator, denominator),
    do: div(numerator * 2 + denominator, denominator * 2)
end
