defmodule Membrane.MOQX.MixProject do
  use Mix.Project

  def project do
    [
      app: :membrane_moqx_plugin,
      version: "0.1.0",
      description:
        "Membrane Source and Sink elements for publishing and subscribing through MOQX",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:membrane_core, "~> 1.3.4"},
      {:membrane_cmaf_format, "~> 0.7.1"},
      {:moqx, "~> 0.9.0"},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false}
    ]
  end
end
