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
    assert {:ok, initialization, [fragment | _rest]} = read_fragments(fixture)

    endpoint =
      System.get_env(
        "MOQX_ENDPOINT",
        "moqt://draft-18.cloudflare.mediaoverquic.com:443"
      )

    authorization = authorization()
    namespace = ["membrane-moqx", "live-#{System.unique_integer([:positive])}"]

    stream_format = %Track{
      content_type: :video,
      header: initialization,
      resolution: {320, 180},
      codecs: %{avc1: %{profile: "42", compatibility: "C0", level: "0B"}}
    }

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :draft_18,
            profile: :cloudflare_cmsf,
            namespace: namespace,
            authorization: authorization,
            timeout: @timeout
          })
      )

    assert_pipeline_notified(pipeline, :sink, {:publication_ready, ^namespace}, @timeout)

    assert {:ok, catalog_subscriber} = MOQX.connect(endpoint, connect_options(authorization))
    assert {:ok, init_subscriber} = MOQX.connect(endpoint, connect_options(authorization))
    assert {:ok, media_subscriber} = MOQX.connect(endpoint, connect_options(authorization))

    try do
      catalog_ref = %MOQX.TrackRef{namespace: namespace, track: ".catalog"}

      assert {:ok, _catalog_subscription} =
               MOQX.subscribe(catalog_subscriber, catalog_ref, profile: :cloudflare_cmsf)

      init_ref = %MOQX.TrackRef{namespace: namespace, track: "video.init.mp4"}
      assert {:ok, init_subscription} = MOQX.subscribe(init_subscriber, init_ref)

      media_ref = %MOQX.TrackRef{namespace: namespace, track: "video.m4s"}
      assert {:ok, media_subscription} = MOQX.subscribe(media_subscriber, media_ref)

      link =
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
        |> get_child(:sink)

      assert :ok = Testing.Pipeline.execute_actions(pipeline, spec: link)

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:track_ready, Pad.ref(:input, :video), "video.m4s"},
        @timeout
      )

      assert_receive {:moqx, ^catalog_subscriber,
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

      assert_receive {:moqx, ^init_subscriber,
                      %MOQX.Event.ObjectReceived{
                        object: %MOQX.Object{
                          subscription: ^init_subscription,
                          group_id: 0,
                          object_id: 0,
                          payload: ^initialization
                        }
                      }},
                     @timeout

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

      assert_receive {:moqx, ^media_subscriber,
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

      assert_receive {:moqx, ^media_subscriber,
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
      # payload but may strip the draft-18 object status.
      assert end_object.status in [:end_of_track, nil]

      assert_pipeline_notified(
        pipeline,
        :sink,
        {:track_ended, Pad.ref(:input, :video), "video.m4s"},
        @timeout
      )

      assert :ok = Testing.Pipeline.terminate(pipeline)

      assert_receive {:moqx, ^media_subscriber,
                      %MOQX.Event.SubscriptionDone{subscription: ^media_subscription}},
                     @timeout
    after
      _result = MOQX.close(catalog_subscriber)
      _result = MOQX.close(init_subscriber)
      _result = MOQX.close(media_subscriber)
    end
  end

  defp authorization do
    case System.get_env("MOQX_AUTHORIZATION_FILE") do
      nil -> nil
      path -> path |> File.read!() |> String.trim() |> MOQX.Secret.new()
    end
  end

  defp connect_options(nil),
    do: [protocol: :draft_18, timeout: @timeout]

  defp connect_options(authorization) do
    [
      protocol: :draft_18,
      authorization: authorization,
      timeout: @timeout
    ]
  end

  # Fixture-only ISO BMFF splitting. Container processing deliberately remains
  # outside MOQX and the plugin's transport elements.
  defp read_fragments(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, boxes} <- split_boxes(contents) do
      {initialization, media} = Enum.split_while(boxes, fn {type, _box} -> type != "moof" end)

      fragments =
        media
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.flat_map(fn
          [{"moof", moof}, {"mdat", mdat}] -> [moof <> mdat]
          _other -> []
        end)

      {:ok, initialization |> Enum.map(&elem(&1, 1)) |> IO.iodata_to_binary(), fragments}
    end
  end

  defp split_boxes(binary), do: split_boxes(binary, [])
  defp split_boxes(<<>>, boxes), do: {:ok, Enum.reverse(boxes)}

  defp split_boxes(<<size::32, type::binary-size(4), rest::binary>> = binary, boxes) do
    box_size =
      case {size, rest} do
        {0, _rest} -> byte_size(binary)
        {1, <<extended_size::64, _rest::binary>>} -> extended_size
        {1, _rest} -> 0
        {size, _rest} -> size
      end

    header_size = if size == 1, do: 16, else: 8

    if box_size >= header_size and box_size <= byte_size(binary) do
      <<box::binary-size(^box_size), tail::binary>> = binary
      split_boxes(tail, [{type, box} | boxes])
    else
      {:error, {:invalid_iso_bmff_box, type, box_size}}
    end
  end

  defp split_boxes(_incomplete_header, _boxes), do: {:error, :incomplete_iso_bmff_header}
end
