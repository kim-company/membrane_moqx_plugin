defmodule Membrane.MOQX.Integration.CloudflareSinkTest do
  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  alias Membrane.{Buffer, Pad}
  alias Membrane.CMAF.Track
  alias Membrane.MOQX.{Sink, TestControlledSource}
  alias Membrane.MOQX.TrackAdapter.ToTrack
  alias Membrane.Testing

  require Pad

  @moduletag :integration

  @timeout 15_000

  test "publishes real H264 CMAF through the Sink and Cloudflare relay" do
    fixture = System.fetch_env!("MOQX_CMAF_FIXTURE")
    assert {:ok, initialization, [fragment | _rest]} = MOQX.CMAF.read_fragments(fixture)

    endpoint =
      System.get_env(
        "MOQX_ENDPOINT",
        "moqt://draft-14.cloudflare.mediaoverquic.com:443"
      )

    authorization = authorization()
    namespace = ["membrane-moqx", "live-#{System.unique_integer([:positive])}"]

    stream_format = %Track{
      content_type: :video,
      header: initialization,
      resolution: {320, 180},
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "0B"}}
    }

    spec =
      child(:source, %TestControlledSource{stream_format: stream_format})
      |> child(:adapter, %ToTrack{adapter: Membrane.MOQX.TrackAdapter.CMAF})
      |> via_out(Pad.ref(:output, :video))
      |> via_in(Pad.ref(:input, :video),
        options: [
          track_name: "video.m4s",
          init_track_name: "video.init.mp4",
          retention: :live
        ]
      )
      |> child(:sink, %Sink{
        endpoint: endpoint,
        protocol: :cloudflare_draft_14,
        profile: :cloudflare_cmsf,
        namespace: namespace,
        authorization: authorization,
        timeout: @timeout
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :video), "video.m4s"},
      @timeout
    )

    assert {:ok, subscriber} =
             MOQX.connect(
               endpoint,
               connect_options(authorization)
             )

    try do
      catalog_ref = %MOQX.TrackRef{namespace: namespace, track: ".catalog"}
      assert {:ok, catalog_subscription} = MOQX.subscribe(subscriber, catalog_ref)

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.CatalogReceived{
                        catalog: %MOQX.Catalog{
                          tracks: [
                            %MOQX.Catalog.Track{
                              name: "video.m4s",
                              init_track: "video.init.mp4",
                              packaging: "cmaf",
                              codec: "avc1.42C00B"
                            }
                          ]
                        }
                      }},
                     @timeout

      assert :ok = MOQX.unsubscribe(subscriber, catalog_subscription)

      init_ref = %MOQX.TrackRef{namespace: namespace, track: "video.init.mp4"}
      assert {:ok, init_subscription} = MOQX.subscribe(subscriber, init_ref)

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.ObjectReceived{
                        object: %MOQX.Object{
                          subscription: ^init_subscription,
                          group_id: 0,
                          object_id: 0,
                          payload: ^initialization
                        }
                      }},
                     @timeout

      assert :ok = MOQX.unsubscribe(subscriber, init_subscription)

      media_ref = %MOQX.TrackRef{namespace: namespace, track: "video.m4s"}
      assert {:ok, media_subscription} = MOQX.subscribe(subscriber, media_ref)

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:subscriber_joined, "video.m4s", _request_id, 1},
        @timeout
      )

      assert :ok =
               Testing.Pipeline.notify_child(
                 pipeline,
                 :source,
                 {:publish, [%Buffer{payload: fragment}]}
               )

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.ObjectReceived{
                        object: %MOQX.Object{
                          subscription: ^media_subscription,
                          group_id: 0,
                          object_id: 0,
                          payload: ^fragment
                        }
                      }},
                     @timeout

      assert :ok = Testing.Pipeline.notify_child(pipeline, :source, :end_of_stream)

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.ObjectReceived{
                        object:
                          %MOQX.Object{
                            subscription: ^media_subscription,
                            group_id: 1,
                            object_id: 0,
                            payload: <<>>
                          } = end_object
                      }},
                     @timeout

      # The deployed relay currently forwards the terminal coordinate and empty
      # payload but may strip the draft-14 object status.
      assert end_object.status in [:end_of_track, nil]

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:track_ended, Pad.ref(:input, :video), "video.m4s"},
        @timeout
      )

      assert :ok = Testing.Pipeline.terminate(pipeline)

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.SubscriptionDone{subscription: ^media_subscription}},
                     @timeout
    after
      _result = MOQX.close(subscriber)
    end
  end

  defp authorization do
    case System.get_env("MOQX_AUTHORIZATION_FILE") do
      nil -> nil
      path -> path |> File.read!() |> String.trim() |> MOQX.Secret.new()
    end
  end

  defp connect_options(nil),
    do: [protocol: :cloudflare_draft_14, timeout: @timeout]

  defp connect_options(authorization) do
    [
      protocol: :cloudflare_draft_14,
      authorization: authorization,
      timeout: @timeout
    ]
  end
end
