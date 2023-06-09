defmodule Opal.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: Opal.StreamServerSupervisor}
    ]

    opts = [strategy: :one_for_one, name: Opal.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
