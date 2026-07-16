defmodule Membrane.MOQX.Sink do
  @moduledoc """
  Publishes canonical `Membrane.MOQX.Track` streams through MOQX.

  Each dynamic input pad represents one already-packaged logical track.
  Format-specific filters translate concrete Membrane formats into the stable
  track and unit contract before this element; the Sink owns MOQ coordinates,
  catalog state, and the MOQX client.
  """

  use Membrane.Sink

  alias Membrane.MOQX.{Track, Unit}

  def_input_pad :input,
    availability: :on_request,
    flow_control: :auto,
    accepted_format: %Track{},
    options: [
      track_name: [spec: binary(), required: true],
      init_track_name: [spec: binary() | nil, default: nil],
      retention: [spec: :live | :latest | :all, default: :live]
    ]

  def_options endpoint: [spec: binary() | URI.t(), required: true],
              protocol: [spec: atom() | module(), required: true],
              namespace: [spec: [binary()], required: true],
              authorization: [spec: MOQX.Secret.t() | nil, default: nil],
              timeout: [spec: pos_integer(), default: 5_000],
              connect_options: [spec: keyword(), default: []],
              transport: [spec: term(), default: nil],
              catalog_track_name: [spec: binary(), default: ".catalog"],
              publisher_priority: [spec: 0..255, default: 127]

  @impl true
  def handle_init(_ctx, options) do
    state =
      options
      |> Map.from_struct()
      |> Map.merge(%{
        client: nil,
        publication: nil,
        catalog_track: nil,
        catalog_revision: 0,
        pads: %{}
      })

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    with {:ok, client} <- MOQX.connect(state.endpoint, connect_options(state)),
         {:ok, publication} <- MOQX.publish(client, state.namespace) do
      state = %{state | client: client, publication: publication}

      {[setup: :incomplete], state}
    else
      {:error, reason} -> raise "failed to set up MOQX publication: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    case validate_pad_track_names(ctx.pad_options, state) do
      :ok ->
        pad_state = %{
          options: ctx.pad_options,
          track: nil,
          init_track: nil,
          init_track_name: nil,
          media_track: nil,
          generation: 0,
          group_id: 0,
          object_id: 0,
          ended?: false
        }

        {[], put_in(state, [:pads, pad], pad_state)}

      {:error, reason} ->
        notification = {:track_rejected, pad, reason}
        {[notify_parent: notification, terminate: {:shutdown, notification}], state}
    end
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    {pad_state, pads} = Map.pop(state.pads, pad)
    state = %{state | pads: pads}

    cond do
      is_nil(pad_state) or is_nil(pad_state.track) ->
        {[], state}

      pad_state.ended? ->
        {[notify_parent: {:track_removed, pad, pad_state.options.track_name}], state}

      true ->
        case publish_catalog(state) do
          {:ok, state} ->
            actions = [notify_parent: {:track_removed, pad, pad_state.options.track_name}]
            {actions, state}

          {:error, reason} ->
            raise "failed to publish MOQX catalog after pad removal: #{inspect(reason)}"
        end
    end
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    if pad_state.track && !pad_state.ended? do
      case publish_end_of_track(state, pad_state) do
        :ok ->
          finish_track_eos(pad, pad_state, state)

        {:error, reason} ->
          raise "failed to publish MOQX end-of-track status: #{inspect(reason)}"
      end
    else
      {[], state}
    end
  end

  @impl true
  def handle_stream_format(pad, %Track{} = stream_format, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    case Track.validate(stream_format) do
      :ok ->
        cond do
          is_nil(pad_state.track) ->
            prepare_initial_track(pad, stream_format, state)

          stream_format == pad_state.track ->
            {[], state}

          true ->
            prepare_updated_track(pad, stream_format, state)
        end

      {:error, reason} ->
        raise "invalid MOQX track stream format: #{inspect(reason)}"
    end
  end

  defp prepare_initial_track(pad, track, state) do
    pad_state = Map.fetch!(state.pads, pad)
    options = pad_state.options

    with {:ok, init_track, init_name} <-
           prepare_initialization(state, init_track_name(options), track),
         {:ok, media_track} <-
           MOQX.add_track(state.client, state.publication, options.track_name,
             retention: options.retention
           ) do
      pad_state = %{
        pad_state
        | track: track,
          init_track: init_track,
          init_track_name: init_name,
          media_track: media_track
      }

      publish_prepared_track(
        pad,
        pad_state,
        {:track_ready, pad, options.track_name},
        state
      )
    else
      {:error, reason} -> raise "failed to prepare MOQX track: #{inspect(reason)}"
    end
  end

  defp prepare_updated_track(pad, track, state) do
    pad_state = Map.fetch!(state.pads, pad)
    generation = pad_state.generation + 1
    init_name = init_track_name(pad_state.options) <> ".#{generation}"

    case prepare_initialization(state, init_name, track) do
      {:ok, init_track, init_name} ->
        pad_state = %{
          pad_state
          | track: track,
            init_track: init_track,
            init_track_name: init_name,
            generation: generation
        }

        publish_prepared_track(
          pad,
          pad_state,
          {:track_updated, pad, pad_state.options.track_name, generation},
          state
        )

      {:error, reason} ->
        raise "failed to update MOQX track: #{inspect(reason)}"
    end
  end

  defp finish_track_eos(pad, pad_state, state) do
    pad_state = %{pad_state | ended?: true}
    state = put_in(state, [:pads, pad], pad_state)

    case publish_catalog(state) do
      {:ok, state} ->
        actions = [notify_parent: {:track_ended, pad, pad_state.options.track_name}]
        {actions, state}

      {:error, reason} ->
        raise "failed to publish MOQX catalog after end of stream: #{inspect(reason)}"
    end
  end

  defp publish_prepared_track(pad, pad_state, notification, state) do
    state = put_in(state, [:pads, pad], pad_state)

    case publish_catalog(state) do
      {:ok, state} -> {[notify_parent: notification], state}
      {:error, reason} -> raise "failed to publish MOQX catalog: #{inspect(reason)}"
    end
  end

  defp prepare_initialization(_state, _name, %{initialization: nil}) do
    {:ok, nil, nil}
  end

  defp prepare_initialization(state, name, track) do
    with {:ok, init_track} <-
           MOQX.add_track(state.client, state.publication, name, retention: :latest),
         :ok <- publish_initialization(state, init_track, track) do
      {:ok, init_track, name}
    end
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)

    with {:ok, unit} <- Unit.from_buffer(buffer),
         :ok <-
           MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
             group_id: pad_state.group_id,
             subgroup_id: 0,
             object_id: pad_state.object_id,
             publisher_priority: state.publisher_priority,
             payload: buffer.payload
           }) do
      pad_state = advance_coordinates(pad_state, unit.group_end?)
      {[], put_in(state, [:pads, pad], pad_state)}
    else
      {:error, reason} -> raise "failed to publish MOQX object: #{inspect(reason)}"
    end
  end

  @impl true
  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationReady{publication: publication}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    case MOQX.add_track(client, publication, state.catalog_track_name, retention: :latest) do
      {:ok, catalog_track} ->
        actions = [setup: :complete, notify_parent: {:publication_ready, state.namespace}]
        {actions, %{state | catalog_track: catalog_track}}

      {:error, reason} ->
        raise "failed to register MOQX catalog track: #{inspect(reason)}"
    end
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriberJoined{track: track, request_id: request_id}},
        _ctx,
        %{client: client} = state
      ) do
    notification = {:subscriber_joined, published_track_name(track), request_id}
    {[notify_parent: notification], state}
  end

  def handle_info(
        {:moqx, client,
         %MOQX.Event.PublicationSubscriberLeft{track: track, request_id: request_id}},
        _ctx,
        %{client: client} = state
      ) do
    notification = {:subscriber_left, published_track_name(track), request_id}
    {[notify_parent: notification], state}
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationFailed{publication: publication, error: error}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    terminate_after_event({:publication_failed, error}, state, :close)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.PublicationCancelled{publication: publication, error: error}},
        _ctx,
        %{client: client, publication: publication} = state
      ) do
    terminate_after_event({:publication_cancelled, error}, state, :close)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.ConnectionClosed{metadata: metadata}},
        _ctx,
        %{client: client} = state
      ) do
    terminate_after_event({:connection_closed, metadata}, state, :already_closed)
  end

  def handle_info(
        {:moqx, client, %MOQX.Event.ProtocolFailed{reason: reason}},
        _ctx,
        %{client: client} = state
      ) do
    terminate_after_event({:protocol_failed, reason}, state, :already_closed)
  end

  def handle_info(_message, _ctx, state), do: {[], state}

  @impl true
  def handle_terminate_request(_ctx, state) do
    finish_publication(state)
    close_client(state)

    {[terminate: :normal], state}
  end

  defp connect_options(state) do
    [
      protocol: state.protocol,
      events_to: self(),
      timeout: state.timeout,
      connect_options: state.connect_options
    ]
    |> put_if_present(:authorization, state.authorization)
    |> put_if_present(:transport, state.transport)
  end

  defp put_if_present(options, _key, nil), do: options
  defp put_if_present(options, key, value), do: Keyword.put(options, key, value)

  defp published_track_name(track) do
    track
    |> MOQX.PublishedTrack.track_ref()
    |> Map.fetch!(:track)
  end

  defp init_track_name(%{init_track_name: nil, track_name: track_name}),
    do: track_name <> ".init"

  defp init_track_name(%{init_track_name: name}), do: name

  defp validate_pad_track_names(options, state) do
    track_name = options.track_name
    init_name = init_track_name(options)

    media_names = Enum.map(state.pads, fn {_pad, pad_state} -> pad_state.options.track_name end)

    init_names =
      Enum.map(state.pads, fn {_pad, pad_state} -> init_track_name(pad_state.options) end)

    cond do
      track_name == state.catalog_track_name ->
        {:error, {:reserved_track_name, track_name}}

      track_name in media_names ->
        {:error, {:duplicate_track_name, track_name}}

      track_name in init_names ->
        {:error, {:track_name_conflicts_with_init_track, track_name}}

      init_name == track_name or init_name == state.catalog_track_name or init_name in media_names ->
        {:error, {:init_track_name_conflict, init_name}}

      init_name in init_names ->
        {:error, {:duplicate_init_track_name, init_name}}

      true ->
        :ok
    end
  end

  defp publish_initialization(state, init_track, track) do
    MOQX.publish_object(state.client, init_track, %MOQX.Object{
      group_id: 0,
      subgroup_id: 0,
      object_id: 0,
      publisher_priority: state.publisher_priority,
      payload: track.initialization
    })
  end

  defp publish_end_of_track(state, pad_state) do
    MOQX.publish_object(state.client, pad_state.media_track, %MOQX.Object{
      group_id: pad_state.group_id,
      subgroup_id: 0,
      object_id: pad_state.object_id,
      publisher_priority: state.publisher_priority,
      status: :end_of_track,
      payload: <<>>
    })
  end

  defp publish_catalog(state) do
    payload =
      JSON.encode!(%{
        "version" => 1,
        "streamingFormat" => 1,
        "streamingFormatVersion" => "0.2",
        "supportsDeltaUpdates" => false,
        "commonTrackFields" => %{
          "namespace" => Enum.join(state.namespace, "/")
        },
        "tracks" => catalog_tracks(state)
      })

    object = %MOQX.Object{
      group_id: state.catalog_revision,
      subgroup_id: 0,
      object_id: 0,
      publisher_priority: state.publisher_priority,
      payload: payload
    }

    case MOQX.publish_object(state.client, state.catalog_track, object) do
      :ok -> {:ok, %{state | catalog_revision: state.catalog_revision + 1}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp catalog_tracks(state) do
    state.pads
    |> Enum.filter(fn {_pad, pad_state} ->
      not is_nil(pad_state.track) and !pad_state.ended?
    end)
    |> Enum.sort_by(fn {_pad, pad_state} -> pad_state.options.track_name end)
    |> Enum.map(fn {_pad, pad_state} -> catalog_track(pad_state) end)
  end

  defp catalog_track(pad_state) do
    options = pad_state.options
    track = pad_state.track

    track.catalog_fields
    |> Map.put("name", options.track_name)
    |> Map.put("packaging", track.packaging)
    |> put_map_unless_empty("selectionParams", track.selection_params)
    |> put_map_if_present("initTrack", pad_state.init_track_name)
  end

  defp put_map_if_present(map, _key, nil), do: map
  defp put_map_if_present(map, key, value), do: Map.put(map, key, value)

  defp put_map_unless_empty(map, _key, value) when value == %{}, do: map
  defp put_map_unless_empty(map, key, value), do: Map.put(map, key, value)

  defp advance_coordinates(pad_state, true) do
    %{pad_state | group_id: pad_state.group_id + 1, object_id: 0}
  end

  defp advance_coordinates(pad_state, false) do
    %{pad_state | object_id: pad_state.object_id + 1}
  end

  defp finish_publication(%{client: nil}), do: :ok
  defp finish_publication(%{publication: nil}), do: :ok

  defp finish_publication(state) do
    case MOQX.finish_publication(state.client, state.publication) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to finish MOQX publication: #{inspect(reason)}")
    end
  end

  defp close_client(%{client: nil}), do: :ok

  defp close_client(state) do
    case MOQX.close(state.client) do
      :ok ->
        :ok

      {:error, reason} ->
        Membrane.Logger.warning("Failed to close MOQX client: #{inspect(reason)}")
    end
  end

  defp terminate_after_event(notification, state, :close) do
    close_client(state)
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end

  defp terminate_after_event(notification, state, :already_closed) do
    {[notify_parent: notification, terminate: {:shutdown, notification}], state}
  end
end
