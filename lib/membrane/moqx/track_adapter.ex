defmodule Membrane.MOQX.TrackAdapter do
  @moduledoc """
  Adapts concrete Membrane formats to and from the canonical MOQX pad contract.

  Adapters are used by the explicit `ToTrack` and `FromTrack` filters. Core
  MOQX Sources and Sinks only exchange `Membrane.MOQX.Track` stream formats and
  buffers carrying `Membrane.MOQX.Unit` metadata.
  """

  alias Membrane.MOQX.{Track, Unit}

  @callback to_moqx_stream_format(stream_format :: struct(), options :: keyword()) ::
              {:ok, Track.t(), adapter_state :: term()} | {:error, term()}

  @callback to_moqx_buffer(Membrane.Buffer.t(), adapter_state :: term()) ::
              {:ok, Membrane.Buffer.t(), adapter_state :: term()} | {:error, term()}

  @callback from_moqx_stream_format(Track.t(), options :: keyword()) ::
              {:ok, struct(), adapter_state :: term()} | {:error, term()}

  @callback from_moqx_buffer(Membrane.Buffer.t(), adapter_state :: term()) ::
              {:ok, Membrane.Buffer.t(), adapter_state :: term()} | {:error, term()}

  @optional_callbacks to_moqx_stream_format: 2,
                      to_moqx_buffer: 2,
                      from_moqx_stream_format: 2,
                      from_moqx_buffer: 2

  @spec to_moqx_stream_format(module(), struct(), keyword()) ::
          {:ok, Track.t(), term()} | {:error, term()}
  def to_moqx_stream_format(adapter, stream_format, options \\ [])
      when is_atom(adapter) and is_struct(stream_format) do
    with :ok <- require_callbacks(adapter, [:to_moqx_stream_format, :to_moqx_buffer]),
         result <- adapter.to_moqx_stream_format(stream_format, options) do
      case result do
        {:ok, %Track{} = track, adapter_state} ->
          validate_adapted_track(track, adapter_state)

        {:ok, invalid_track, _adapter_state} ->
          {:error, {:invalid_adapted_track, invalid_track}}

        {:error, _reason} = error ->
          error

        invalid_result ->
          {:error, {:invalid_adapted_track_result, invalid_result}}
      end
    end
  end

  @spec to_moqx_buffer(module(), Membrane.Buffer.t(), term()) ::
          {:ok, Membrane.Buffer.t(), term()} | {:error, term()}
  def to_moqx_buffer(adapter, %Membrane.Buffer{} = buffer, adapter_state)
      when is_atom(adapter) do
    with :ok <- require_callbacks(adapter, [:to_moqx_stream_format, :to_moqx_buffer]),
         result <- adapter.to_moqx_buffer(buffer, adapter_state) do
      validate_adapted_buffer(result)
    end
  end

  @spec from_moqx_stream_format(module(), Track.t(), keyword()) ::
          {:ok, struct(), term()} | {:error, term()}
  def from_moqx_stream_format(adapter, %Track{} = track, options \\ []) when is_atom(adapter) do
    with :ok <- require_callbacks(adapter, [:from_moqx_stream_format, :from_moqx_buffer]),
         result <- adapter.from_moqx_stream_format(track, options) do
      case result do
        {:ok, stream_format, adapter_state} when is_struct(stream_format) ->
          {:ok, stream_format, adapter_state}

        {:ok, invalid_stream_format, _adapter_state} ->
          {:error, {:invalid_adapted_stream_format, invalid_stream_format}}

        {:error, _reason} = error ->
          error

        invalid_result ->
          {:error, {:invalid_adapted_stream_format_result, invalid_result}}
      end
    end
  end

  @spec from_moqx_buffer(module(), Membrane.Buffer.t(), term()) ::
          {:ok, Membrane.Buffer.t(), term()} | {:error, term()}
  def from_moqx_buffer(adapter, %Membrane.Buffer{} = buffer, adapter_state)
      when is_atom(adapter) do
    with :ok <- require_callbacks(adapter, [:from_moqx_stream_format, :from_moqx_buffer]),
         result <- adapter.from_moqx_buffer(buffer, adapter_state) do
      validate_adapted_buffer(result)
    end
  end

  defp validate_adapted_buffer({:ok, %Membrane.Buffer{payload: payload} = buffer, adapter_state})
       when is_binary(payload) do
    case Unit.from_buffer(buffer) do
      {:ok, %Unit{}} -> {:ok, buffer, adapter_state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_adapted_buffer({:ok, invalid_buffer, _adapter_state}),
    do: {:error, {:invalid_adapted_buffer, invalid_buffer}}

  defp validate_adapted_buffer({:error, _reason} = error), do: error

  defp validate_adapted_buffer(invalid_result),
    do: {:error, {:invalid_adapted_buffer_result, invalid_result}}

  defp validate_adapted_track(track, adapter_state) do
    case Track.validate(track) do
      :ok -> {:ok, track, adapter_state}
      {:error, _reason} -> {:error, {:invalid_adapted_track, track}}
    end
  end

  defp require_callbacks(adapter, callbacks) do
    valid? =
      Code.ensure_loaded?(adapter) and
        Enum.all?(callbacks, &function_exported?(adapter, &1, 2))

    if valid?, do: :ok, else: {:error, {:invalid_track_adapter, adapter}}
  end
end
