defmodule Membrane.MOQX.Integration.LiteEpochRoundtripTest do
  use ExUnit.Case, async: false
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.{Buffer, Pad, Testing}
  alias Membrane.MOQX.Event.EmptyGroup
  alias Membrane.MOQX.Hang.Legacy

  alias Membrane.MOQX.{
    CatalogSource,
    Session,
    Sink,
    TestControlledSource,
    Track,
    TrackOffer,
    Unit
  }

  require Pad

  @moduletag :integration
  @timeout 15_000

  test "HANG catalog media preserves backward epochs and the final empty group before EOS" do
    endpoint = System.fetch_env!("MOQX_LITE_ENDPOINT")

    connection = [
      verify: :verify_peer,
      cacertfile: System.get_env("MOQX_LITE_CA_FILE", "/etc/ssl/cert.pem")
    ]

    namespace = ["membrane-epochs", "run-#{System.system_time(:nanosecond)}"]

    format = %Track{
      packaging: "opus",
      initialization: nil,
      selection_params: %{"codec" => "opus", "sampleRate" => 48_000, "numberOfChannels" => 2}
    }

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:producer, %TestControlledSource{stream_format: format})
          |> child(:framing, %Legacy{direction: :encode})
          |> via_in(Pad.ref(:input, :opus), options: [track_name: "opus", timescale: 1_000_000])
          |> child(:sink, %Sink{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            connect_options: connection
          })
      )

    assert_pipeline_notified(publisher, :sink, {:track_ready, _, "opus"}, @timeout)

    session =
      start_supervised!(
        {Session, endpoint: endpoint, protocol: :moq_lite_05, connect_options: connection}
      )

    {:ok, discovery} = Session.discover(session, "")
    path = Enum.join(namespace, "/")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: ^path}},
                   @timeout

    subscriber =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:catalog, %CatalogSource{
            endpoint: endpoint,
            protocol: :moq_lite_05,
            profile: :hang,
            namespace: namespace,
            connect_options: connection
          })
      )

    assert_pipeline_notified(
      subscriber,
      :catalog,
      {:track_available,
       %TrackOffer{track_ref: ref, stream_format: %Track{packaging: "hang/legacy"}} = offer},
      @timeout
    )

    assert ref == %MOQX.TrackRef{namespace: namespace, track: "opus"}

    Testing.Pipeline.execute_actions(subscriber,
      spec:
        get_child(:catalog)
        |> via_out(Pad.ref(:output, :opus), options: [track: ref])
        |> child(:framing, %Legacy{direction: :decode})
        |> child(:consumer, Testing.Sink)
    )

    assert_pipeline_notified(publisher, :sink, {:subscriber_joined, "opus", _, 1}, @timeout)

    # A receiver observation is the phase barrier. MOQX preserves per-stream
    # arrival order, not global ordering across concurrently sent QUIC groups.
    publish(publisher, <<0xF8, 0xFF, 0xFE>>, 5_000_000)

    assert {:buffer,
            %Buffer{
              payload: <<0xF8, 0xFF, 0xFE>>,
              pts: 5_000_000,
              metadata: %{moqx: %Unit{group_id: 0}}
            }} = next_media(subscriber)

    Testing.Pipeline.notify_child(publisher, :producer, {:event, %EmptyGroup{}})
    assert {:empty, %EmptyGroup{group_id: 1}} = next_media(subscriber)

    publish(publisher, <<0xF8, 0xFF, 0xFE>>, 1_000_000)

    assert {:buffer,
            %Buffer{
              payload: <<0xF8, 0xFF, 0xFE>>,
              pts: 1_000_000,
              metadata: %{moqx: %Unit{group_id: 2}}
            }} = next_media(subscriber)

    # No observer barrier or delay between the final empty group and EOS:
    # withdrawal must drain the final group before ending the selected output.
    Testing.Pipeline.notify_child(publisher, :producer, {:event, %EmptyGroup{}})
    Testing.Pipeline.notify_child(publisher, :producer, :end_of_stream)
    assert {:empty, %EmptyGroup{group_id: 3}} = next_media(subscriber)
    assert :end_of_stream = next_media(subscriber)
    assert_pipeline_notified(subscriber, :catalog, {:track_unavailable, ^offer}, @timeout)

    Testing.Pipeline.terminate(subscriber)
    Testing.Pipeline.terminate(publisher)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastWithdrawn{discovery: ^discovery, path: ^path}},
                   @timeout

    assert :ok = Session.cancel_discovery(session, discovery)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.DiscoveryDone{discovery: ^discovery, reason: :cancelled}},
                   @timeout

    assert :ok = Session.close(session)
  end

  defp publish(publisher, payload, pts) do
    buffer = %Buffer{payload: payload, pts: pts, metadata: %{moqx: %Unit{group_end?: true}}}
    Testing.Pipeline.notify_child(publisher, :producer, {:publish, [buffer]})
  end

  defp next_media(subscriber) do
    receive do
      {Testing.Pipeline, ^subscriber,
       {:handle_child_notification, {{:buffer, buffer}, :consumer}}} ->
        {:buffer, buffer}

      {Testing.Pipeline, ^subscriber,
       {:handle_child_notification, {{:event, %EmptyGroup{} = event}, :consumer}}} ->
        {:empty, event}

      {Testing.Pipeline, ^subscriber, {:handle_element_end_of_stream, {:consumer, :input}}} ->
        :end_of_stream
    after
      @timeout ->
        flunk("selected HANG output did not deliver its next buffer, empty group or EOS")
    end
  end
end
