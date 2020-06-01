defmodule RatchetWrench.MixProject do
  use Mix.Project

  def project do
    [
      app: :ratchet_wrench,
      version: "0.1.0",
      elixir: "~> 1.10",
      description: "RatchetWrench is a easily use Google Cloud Spanner by Elixir.",
      package: [
        maintainers: ["hirocaster"],
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/hirocaster/ratchet_wrench"}
     ],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:google_api_spanner, "~> 0.20"},
      {:goth, "~> 1.2.0"},
      {:inflex, "~> 2.0.0" },
      {:elixir_uuid, "~> 1.2" },
      {:faker, "~> 0.13"},
      {:tzdata, "~> 1.0.3"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:credo, "~> 1.4", only: :dev, runtime: false},
    ]
  end
end
