defmodule Membrane.MOQX.SessionTest do
  use ExUnit.Case, async: true
  alias Membrane.MOQX.{Session, TestDiscoveryPeer, TestPublisher}

  test "routes live broadcast additions and withdrawals without ending discovery" do
    peer = TestDiscoveryPeer.start("room/")

    {:ok, session} =
      Session.start_link(
        endpoint: peer.endpoint,
        protocol: :moq_lite_05,
        transport: peer.transport
      )

    {:ok, discovery} = Session.discover(session, "room/")

    assert_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryReady{discovery: ^discovery}},
                   2_000

    TestDiscoveryPeer.announce(peer, :active, "bob.hang")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: "room/bob.hang"}},
                   2_000

    TestDiscoveryPeer.announce(peer, :ended, "bob.hang")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastWithdrawn{discovery: ^discovery, path: "room/bob.hang"}},
                   2_000

    TestDiscoveryPeer.announce(peer, :active, "bob.hang")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: "room/bob.hang"}},
                   2_000

    refute_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryDone{discovery: ^discovery}}
    assert :ok = Session.cancel_discovery(session, discovery)

    assert_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryDone{discovery: ^discovery}},
                   2_000

    assert :ok = Session.close(session)
    assert :ok = Task.await(peer.task)
  end

  test "discovery owner exit cancels only its handle and survivors receive live updates" do
    peer = TestDiscoveryPeer.start(["room/", "other/"])

    {:ok, session} =
      Session.start_link(
        endpoint: peer.endpoint,
        protocol: :moq_lite_05,
        transport: peer.transport
      )

    parent = self()

    owner =
      Task.async(fn ->
        {:ok, discovery} = Session.discover(session, "room/")
        send(parent, {:owned_discovery, discovery})

        receive do
          :exit_owner -> :ok
        end
      end)

    assert_receive {:owned_discovery, foreign}
    assert {:error, :not_discovery_owner} = Session.cancel_discovery(session, foreign)
    {:ok, own} = Session.discover(session, "other/")
    assert_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryReady{discovery: ^own}}, 2_000
    send(owner.pid, :exit_owner)
    assert :ok = Task.await(owner)
    assert_receive {:discovery_cancelled, "room/"}, 2_000
    TestDiscoveryPeer.announce(peer, :active, "bob.hang")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^own, path: "other/bob.hang"}},
                   2_000

    refute_receive {:moqx_session, ^session, %{discovery: ^foreign}}
    assert :ok = Session.cancel_discovery(session, own)
    assert_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryDone{discovery: ^own}}, 2_000
    assert :ok = Session.close(session)
    assert :ok = Task.await(peer.task)
  end

  test "routes one subscription's events to its owner" do
    track = %MOQX.TrackRef{namespace: ["live", "session"], track: "data"}

    publisher =
      TestPublisher.start(track, [
        %MOQX.Object{group_id: 0, subgroup_id: 0, object_id: 0, payload: "payload"}
      ])

    on_exit(fn ->
      if Process.alive?(publisher.task.pid), do: Process.exit(publisher.task.pid, :kill)
    end)

    {:ok, session} =
      Session.start_link(
        endpoint: publisher.endpoint,
        protocol: :cloudflare_draft_14,
        transport: TestPublisher.transport(publisher)
      )

    {:ok, subscription} = Session.subscribe(session, track, delivery_timeout: 50)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.SubscriptionAccepted{subscription: ^subscription}}

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.ObjectReceived{
                      object: %MOQX.Object{subscription: ^subscription, payload: "payload"}
                    }}

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.SubscriptionDone{subscription: ^subscription}}

    assert :ok = Session.close(session)
    assert :ok = TestPublisher.await_shutdown(publisher)
  end

  test "discovers broadcasts and reports scoped cancellation through a shared Session" do
    peer = TestDiscoveryPeer.start("room/")

    {:ok, session} =
      Session.start_link(
        endpoint: peer.endpoint,
        protocol: :moq_lite_05,
        transport: peer.transport
      )

    {:ok, discovery} = Session.discover(session, "room/")

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastAvailable{discovery: ^discovery, path: "room/alice.hang"}},
                   2_000

    assert_receive {:moqx_session, ^session, %MOQX.Event.DiscoveryReady{discovery: ^discovery}},
                   2_000

    assert :ok = Session.cancel_discovery(session, discovery)

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.BroadcastWithdrawn{
                      discovery: ^discovery,
                      path: "room/alice.hang",
                      reason: :cancelled
                    }},
                   2_000

    assert_receive {:moqx_session, ^session,
                    %MOQX.Event.DiscoveryDone{discovery: ^discovery, reason: :cancelled}},
                   2_000

    assert :ok = Session.close(session)
    assert :ok = Task.await(peer.task)
  end
end
