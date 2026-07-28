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

  test "publishes canonical CMAF through Sink and receives it through Moqtail" do
    fixture = System.fetch_env!("MOQX_CMAF_FIXTURE")
    assert {:ok, initialization, [fragment | _rest]} = MOQX.CMAF.read_fragments(fixture)

    endpoint = draft_16_endpoint()
    namespace = ["membrane-moqx", "draft16-#{System.unique_integer([:positive])}"]
    media_name = "video"

    stream_format = %Track{
      packaging: "cmaf",
      initialization: initialization,
      selection_params: %{
        "codec" => "avc1.42C01F",
        "width" => 640,
        "height" => 360
      },
      catalog_fields: %{"role" => "video", "timescale" => 90_000}
    }

    spec =
      child(:source, %TestControlledSource{stream_format: stream_format})
      |> via_in(Pad.ref(:input, :video),
        options: [track_name: media_name, retention: :all]
      )
      |> child(:sink, %Sink{
        endpoint: endpoint,
        protocol: :draft_16,
        namespace: namespace,
        timeout: @timeout,
        catalog_refresh_interval: 500
      })

    pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_pipeline_notified(
      pipeline,
      :sink,
      {:track_ready, Pad.ref(:input, :video), ^media_name},
      @timeout
    )

    assert {:ok, subscriber} =
             MOQX.connect(endpoint, protocol: :draft_16, timeout: @timeout)

    try do
      catalog_ref = %MOQX.TrackRef{namespace: namespace, track: "catalog"}
      assert {:ok, catalog_subscription} = MOQX.subscribe(subscriber, catalog_ref)

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.CatalogReceived{
                        subscription: ^catalog_subscription,
                        catalog: %MOQX.Catalog{
                          format: :moqtail_cmsf,
                          tracks: [
                            %MOQX.Catalog.Track{
                              name: ^media_name,
                              init_data: ^initialization
                            }
                          ]
                        }
                      }},
                     @timeout

      media_ref = %MOQX.TrackRef{namespace: namespace, track: media_name}
      assert {:ok, media_subscription} = MOQX.subscribe(subscriber, media_ref)

      assert :ok =
               Testing.Pipeline.notify_child(
                 pipeline,
                 :source,
                 {:publish,
                  [
                    %Buffer{
                      payload: fragment,
                      metadata: %{moqx: %Unit{group_end?: true}}
                    }
                  ]}
               )

      assert_receive {:moqx, ^subscriber,
                      %MOQX.Event.ObjectReceived{
                        object: %MOQX.Object{
                          subscription: ^media_subscription,
                          payload: ^fragment
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
