defmodule Membrane.MOQX.Integration.CloudflareSourceTest do
  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.{Sink, Source, TestControlledSource, Track, Unit}
  alias Membrane.Testing

  require Pad

  @moduletag :integration
  @timeout 15_000

  test "requests an absent track, provisions it through the Sink, and receives it through Cloudflare" do
    endpoint =
      System.get_env("MOQX_ENDPOINT", "moqt://draft-14.cloudflare.mediaoverquic.com:443")

    authorization = authorization()
    namespace = ["membrane-moqx", "source-live-#{System.unique_integer([:positive])}"]
    track_ref = %MOQX.TrackRef{namespace: namespace, track: "events/on-demand"}
    track_name = track_ref.track
    stream_format = %Track{packaging: "application/example", initialization: nil}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :cloudflare_draft_14,
            namespace: namespace,
            authorization: authorization,
            timeout: @timeout,
            inbound_subscriptions: :controlled
          })
      )

    assert_pipeline_notified(publisher, :sink, {:publication_ready, ^namespace}, @timeout)

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Source{
            endpoint: endpoint,
            protocol: :cloudflare_draft_14,
            track: track_ref,
            stream_format: stream_format,
            authorization: authorization,
            timeout: @timeout
          })
          |> child(:sink, %Testing.Sink{})
      )

    assert_pipeline_notified(
      publisher,
      :sink,
      {:subscription_requested, %MOQX.PublicationSubscriptionRequest{} = request},
      @timeout
    )

    assert request.track == track_ref
    assert :ok = Testing.Pipeline.notify_child(publisher, :sink, {:accept_subscription, request})

    track_spec =
      child(:producer, %TestControlledSource{stream_format: stream_format})
      |> via_in(Pad.ref(:input, :events),
        options: [track_name: track_ref.track, retention: :live]
      )
      |> get_child(:sink)

    assert :ok = Testing.Pipeline.execute_actions(publisher, spec: track_spec)

    assert_pipeline_notified(
      publisher,
      :sink,
      {:track_ready, Pad.ref(:input, :events), ^track_name},
      @timeout
    )

    assert_pipeline_notified(subscriber, :source, {:subscription_ready, ^track_ref}, @timeout)

    assert_pipeline_notified(
      publisher,
      :sink,
      {:subscriber_joined, ^track_name, _request_id, 1},
      @timeout
    )

    assert_sink_stream_format(subscriber, :sink, ^stream_format, @timeout)

    payload = JSON.encode!(%{"kind" => "ready"})

    assert :ok =
             Testing.Pipeline.notify_child(
               publisher,
               :producer,
               {:publish, [%Buffer{payload: payload, metadata: %{moqx: %Unit{group_end?: true}}}]}
             )

    assert :ok = Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)

    assert_receive {Testing.Pipeline, ^subscriber,
                    {:handle_child_notification, {{:buffer, %Buffer{payload: ^payload}}, :sink}}},
                   @timeout

    assert :ok = Testing.Pipeline.terminate(publisher)
    assert_end_of_stream(subscriber, :sink, :input, @timeout)
    assert :ok = Testing.Pipeline.terminate(subscriber)
  end

  defp authorization do
    case System.get_env("MOQX_AUTHORIZATION_FILE") do
      nil -> nil
      path -> path |> File.read!() |> String.trim() |> MOQX.Secret.new()
    end
  end
end
