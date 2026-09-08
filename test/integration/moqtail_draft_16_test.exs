defmodule Membrane.MOQX.Integration.MoqtailDraft16Test do
  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.MOQX.{CatalogSource, Sink, TestControlledSource, Track, TrackOffer, Unit}
  alias Membrane.Testing

  require Pad

  @moduletag :integration
  @timeout 15_000

  test "discovers and attaches to Moqtail's current public CMSF publication" do
    endpoint = draft_16_endpoint()
    namespace = ["moqtail", "testsrc"]

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %CatalogSource{
            endpoint: endpoint,
            protocol: :draft_16,
            profile: :moqtail_cmsf,
            namespace: namespace,
            timeout: @timeout
          })
      )

    assert_pipeline_notified(pipeline, :source, :catalog_ready, @timeout)

    assert_receive {Testing.Pipeline, ^pipeline,
                    {:handle_child_notification,
                     {{:track_available,
                       %TrackOffer{
                         track_ref: media_ref,
                         stream_format:
                           %Track{
                             packaging: "cmaf",
                             initialization: initialization,
                             catalog_fields: %{"codec" => "avc1" <> _codec}
                           } = stream_format
                       }}, :source}}},
                   @timeout

    assert is_binary(initialization) and byte_size(initialization) > 0
    pad = Pad.ref(:output, :video)

    link =
      get_child(:source)
      |> via_out(pad,
        options: [
          track: media_ref,
          subscription_options: [start: :next_group, priority: 127]
        ]
      )
      |> child(:sink, %Testing.Sink{})

    assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)
    assert_sink_stream_format(pipeline, :sink, ^stream_format, @timeout)

    assert_receive {Testing.Pipeline, ^pipeline,
                    {:handle_child_notification, {{:buffer, %Buffer{}}, :sink}}},
                   @timeout

    assert :ok = Testing.Pipeline.terminate(pipeline)
  end

  test "accepts a namespace-routed subscription and publishes through Moqtail" do
    endpoint = draft_16_endpoint()
    namespace = ["membrane-moqx", "draft16-#{System.unique_integer([:positive])}"]
    media_name = "captions"
    payload = "WEBVTT\n\n00:00.000 --> 00:01.000\nhello from membrane_moqx_plugin\n"

    stream_format = %Track{
      packaging: "webvtt",
      initialization: nil,
      selection_params: %{"lang" => "en-US"},
      catalog_fields: %{"role" => "captions"}
    }

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :draft_16,
            profile: :moqtail_cmsf,
            namespace: namespace,
            timeout: @timeout,
            catalog_refresh_interval: 500,
            inbound_subscriptions: :controlled,
            track_demand_events: true
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace}, @timeout)

    assert {:ok, subscriber} =
             MOQX.connect(endpoint, protocol: :draft_16, timeout: @timeout)

    try do
      media_ref = %MOQX.TrackRef{namespace: namespace, track: media_name}
      subscription_task = Task.async(fn -> MOQX.subscribe(subscriber, media_ref) end)

      assert_receive {Testing.Pipeline, ^pipeline,
                      {:handle_child_notification,
                       {{:subscription_requested,
                         %MOQX.PublicationSubscriptionRequest{} = request}, :sink}}},
                     @timeout

      assert :ok =
               Testing.Pipeline.notify_child(
                 pipeline,
                 :sink,
                 {:accept_subscription, request}
               )

      link =
        child(:source, %TestControlledSource{stream_format: stream_format})
        |> via_in(Pad.ref(:input, :captions),
          options: [track_name: media_name, retention: :all]
        )
        |> get_child(:sink)

      assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)
      assert {:ok, media_subscription} = Task.await(subscription_task, @timeout)

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:track_ready, Pad.ref(:input, :captions), ^media_name},
        @timeout
      )

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:subscriber_joined, ^media_name, _identity, 1},
        @timeout
      )

      assert_pipeline_notified(
        pipeline,
        :source,
        {:track_demand, :output,
         %Membrane.MOQX.Event.TrackDemand{active?: true, subscriber_count: 1}},
        @timeout
      )

      assert :ok =
               Testing.Pipeline.notify_child(
                 pipeline,
                 :source,
                 {:publish,
                  [
                    %Buffer{
                      payload: payload,
                      metadata: %{moqx: %Unit{group_end?: true}}
                    }
                  ]}
               )

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.ObjectReceived{
                        object: %MOQX.Object{
                          subscription: ^media_subscription,
                          payload: ^payload
                        }
                      }},
                     @timeout
    after
      _result = MOQX.close(subscriber)
      _result = Testing.Pipeline.terminate(pipeline)
    end
  end

  defp draft_16_endpoint do
    System.get_env("MOQX_DRAFT16_ENDPOINT", "moqt://relay.moqtail.dev:443")
  end
end
