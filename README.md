# RatchetWrench

** This library alpha level version. Not use in production. **

RatchetWrench is a easily use Google Cloud Spanner by Elixir.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ratchet_wrench` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ratchet_wrench, "~> 0.0.1"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ratchet_wrench](https://hexdocs.pm/ratchet_wrench).

## Setup

### Credentials

Add GCP credentials json file path for env `GOOGLE_APPLICATION_CREDENTIALS`.

more detail's for [goth](https://github.com/peburrows/goth).

### Set Google Cloud Spanner config

Set database.

Add env `RATCHET_WRENCH_DATABASE`.

ex) "projects/projectname-123456/instances/your-instance/databases/your-db"

``` shell
export RATCHET_WRENCH_DATABASE="projects/projectname-123456/instances/your-instance/databases/your-db"
```

or

Add config.exs

``` elixir
config :ratchet_wrench, database: "projects/projectname-123456/instances/your-instance/databases/your-db"
```

You must replace `projectname-123456`, `your-instance`, `your-db`.


### Logging

Output for Logger module.

Add env `RATCHET_WRENCH_ENABLE_LOGGING`

``` shell
export RATCHET_WRENCH_ENABLE_LOGGING=1
```

or

Add config.exs

``` elixir
config :ratchet_wrench, enable_logging: true
```

## Migration

I recommend using [wrench](https://github.com/cloudspannerecosystem/wrench).

## Usage

Sorry, will write sample more codes.

  - How to define model(Table) -> `test/test_helper.exs`
  - How to SELECT/INSERT/UPDATE -> `test/ratchet_wrench/repo_test.exs`

### Setup config

Your application.ex write to example.

``` elixir
  def start(_type, _args) do
    children = [
    ...
      {RatchetWrench.SessionPool, %RatchetWrench.Pool{}}
    ]
    ...
  end
```

### Shutdown

Cleanup session in Google Cloud Spanner at shutdown your app.

``` elixir
Process.send(RatchetWrench.SessionPool, :kill, [])
```

### Support type in Google Cloud Spanner

  - STRING
  - DATE
  - BOOL
  - INT64
  - FLOAT64
  - TIMESTAMP

Unsupport types

  - ARRAY
  - BYTES
  - STRUCT
