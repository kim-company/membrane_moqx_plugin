defmodule Membrane.MOQX.DiagnosticsTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.MOQX.Sink
  alias Membrane.Testing
  alias MOQX.Testing.Transport

  test "connection-failure pipeline diagnostics redact explicitly wrapped authorization" do
    secret = "synthetic-authorization-value-not-a-real-credential"
    {:ok, network} = Transport.start_network()

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        sink = %Sink{
          endpoint: "moql://localhost:65530",
          protocol: :moq_lite_05,
          namespace: ["diagnostics"],
          authorization: MOQX.Secret.new(secret),
          transport: {Transport, network: network, profile: :moq_lite_05}
        }

        # The isolated network has no listener. Exercise real setup failure and
        # Membrane option/error logging, without contacting an external service.
        spec = {child(:sink, sink), group: :failed_connection, crash_group_mode: :temporary}
        pipeline = Testing.Pipeline.start_link_supervised!(spec: spec)
        assert_child_terminated(pipeline, :sink)
        assert :ok = Testing.Pipeline.terminate(pipeline)
      end)

    assert log =~ "econnrefused"
    assert log =~ "#MOQX.Secret<REDACTED>"
    refute log =~ secret
  end
end
