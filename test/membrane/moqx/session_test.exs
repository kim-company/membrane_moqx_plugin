defmodule Membrane.MOQX.SessionTest do
  use ExUnit.Case, async: true

  alias Membrane.MOQX.{Session, TestPublisher}

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
end
