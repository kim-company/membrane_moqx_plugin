defmodule Membrane.MOQX.TrackAdapter do
  @moduledoc """
  Adapts concrete Membrane stream formats to the stable MOQX track contract.

  A custom adapter can be selected explicitly with the `:adapter` option.
  Without an override, the stream-format struct is resolved to a built-in
  adapter.
  """

  alias Membrane.MOQX.{PublicationUnit, TrackDescriptor}

  @type describe_result :: {:ok, TrackDescriptor.t()} | {:error, term()}

  @callback describe(stream_format :: struct(), options :: keyword()) :: describe_result()
  @callback publication_unit(
              buffer :: Membrane.Buffer.t(),
              descriptor :: TrackDescriptor.t()
            ) :: {:ok, PublicationUnit.t()} | {:error, term()}

  @spec describe(struct(), keyword()) ::
          {:ok, module(), TrackDescriptor.t()} | {:error, term()}
  def describe(stream_format, options \\ []) when is_struct(stream_format) do
    with {:ok, adapter} <- resolve(stream_format, Keyword.get(options, :adapter)) do
      case adapter.describe(stream_format, options) do
        {:ok, %TrackDescriptor{} = descriptor} -> validate_descriptor(adapter, descriptor)
        {:ok, invalid_descriptor} -> {:error, {:invalid_track_descriptor, invalid_descriptor}}
        {:error, _reason} = error -> error
        invalid_result -> {:error, {:invalid_track_descriptor_result, invalid_result}}
      end
    end
  end

  @spec publication_unit(module(), Membrane.Buffer.t(), TrackDescriptor.t()) ::
          {:ok, PublicationUnit.t()} | {:error, term()}
  def publication_unit(adapter, %Membrane.Buffer{} = buffer, %TrackDescriptor{} = descriptor)
      when is_atom(adapter) do
    case adapter.publication_unit(buffer, descriptor) do
      {:ok, %PublicationUnit{} = unit} -> validate_publication_unit(unit)
      {:ok, invalid_unit} -> {:error, {:invalid_publication_unit, invalid_unit}}
      {:error, _reason} = error -> error
      invalid_result -> {:error, {:invalid_publication_unit_result, invalid_result}}
    end
  end

  defp validate_publication_unit(%PublicationUnit{} = unit) do
    if is_binary(unit.payload) and is_boolean(unit.segment_end?) and
         (is_boolean(unit.independent?) or is_nil(unit.independent?)) and
         ((is_integer(unit.duration) and unit.duration >= 0) or is_nil(unit.duration)) do
      {:ok, unit}
    else
      {:error, {:invalid_publication_unit, unit}}
    end
  end

  defp validate_descriptor(adapter, %TrackDescriptor{} = descriptor) do
    valid? =
      valid_packaging?(descriptor.packaging) and
        valid_content_types?(descriptor.content_types) and
        valid_initialization?(descriptor.initialization) and
        valid_codecs?(descriptor.codecs) and
        valid_resolution?(descriptor.resolution) and
        valid_optional_positive_integer?(descriptor.sample_rate) and
        valid_optional_positive_integer?(descriptor.channels)

    if valid? do
      {:ok, adapter, descriptor}
    else
      {:error, {:invalid_track_descriptor, descriptor}}
    end
  end

  defp valid_packaging?(packaging), do: is_atom(packaging) and not is_nil(packaging)

  defp valid_content_types?(content_types) do
    content_types != [] and Enum.all?(content_types, &(&1 in [:audio, :video]))
  end

  defp valid_initialization?(initialization),
    do: is_binary(initialization) or is_nil(initialization)

  defp valid_codecs?(codecs), do: codecs != [] and Enum.all?(codecs, &is_binary/1)

  defp valid_resolution?(nil), do: true

  defp valid_resolution?({width, height}) do
    is_integer(width) and width >= 0 and is_integer(height) and height >= 0
  end

  defp valid_resolution?(_resolution), do: false

  defp valid_optional_positive_integer?(nil), do: true
  defp valid_optional_positive_integer?(value), do: is_integer(value) and value > 0

  defp resolve(%Membrane.CMAF.Track{}, nil), do: {:ok, Membrane.MOQX.TrackAdapter.CMAF}

  defp resolve(stream_format, nil),
    do: {:error, {:unsupported_stream_format, stream_format.__struct__}}

  defp resolve(_stream_format, adapter) when is_atom(adapter) do
    if function_exported?(adapter, :describe, 2) and
         function_exported?(adapter, :publication_unit, 2) do
      {:ok, adapter}
    else
      {:error, {:invalid_track_adapter, adapter}}
    end
  end
end
