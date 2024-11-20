defmodule JobRunner.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: JobRunner.Worker.start_link(arg)
      # {JobRunner.Worker, arg}
      {Registry, keys: :unique, name: JobWatcherRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: JobWatcherSupervisor}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: JobRunner.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
