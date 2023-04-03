defmodule Opal.MixProject do
  use Mix.Project

  def project do
    [
      app: :opal,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Opal.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:benchfella, "~> 0.3.0"},
      {:uniq, "~> 0.5.4", only: :test},
      {:stream_data, "~> 0.5", only: :test}
    ]
  end
end
