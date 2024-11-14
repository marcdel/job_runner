defmodule JobWatcher do
  use GenServer
  use OpenTelemetryDecorator

  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @decorate with_span("JobWatcher.watch_job", include: [:job_id, :runner_pid])
  def watch_job(runner_pid, job_id) do
    ctx = OpenTelemetry.Ctx.get_current()
    GenServer.cast(__MODULE__, {:start, runner_pid, job_id, ctx})
  end

  def table_name, do: :job_watcher_state

  def init(_) do
    {:ok, %{}, {:continue, :load_state}}
  end

  def handle_continue(:load_state, _) do
    state = load_state()

    if state.job_id do
      schedule_check()
    end

    {:noreply, state}
  end

  def handle_cast({:start, runner_pid, job_id, ctx}, _) do
    state = %{runner_pid: runner_pid, job_id: job_id, trace_ctx: ctx}
    OpenTelemetry.Ctx.attach(ctx)
    O11y.set_attributes(state)

    save_state(state)
    schedule_check()

    {:noreply, state}
  end

  def handle_info(:check_status, state) do
    successful? = :rand.uniform() > 0.25

    if successful? do
      do_check_status(state)
    else
      {:stop, :random_error, state}
    end
  end

  @decorate with_span("JobWatcher.terminate", include: [:_reason, :state])
  def terminate(_reason, state) do
    save_state(state)
    :ok
  end

  @decorate with_span("JobWatcher.do_check_status", include: [:state])
  defp do_check_status(state) do
    case FakeK8s.job_status(state.job_id) do
      :completed ->
        O11y.set_attributes(status: :completed)
        send(state.runner_pid, :job_completed)

      :failed ->
        O11y.set_attributes(status: :failed)
        send(state.runner_pid, :job_failed)

      status ->
        O11y.set_attributes(status: status)
        schedule_check()
    end

    {:noreply, state}
  end

  @decorate with_span("JobWatcher.schedule_check")
  defp schedule_check do
    Process.send_after(self(), :check_status, :timer.seconds(1))
  end

  @decorate with_span("JobWatcher.save_state", include: [:state])
  defp save_state(state) do
    :ets.insert(table_name(), {:state, state})
    state
  end

  @decorate with_span("JobWatcher.load_state")
  defp load_state do
    case :ets.lookup(table_name(), :state) do
      [{:state, state}] ->
        OpenTelemetry.Ctx.attach(state.trace_ctx)
        O11y.set_attributes(state)

      _ ->
        O11y.set_attributes(%{
          runner_pid: nil,
          job_id: nil,
          trace_ctx: OpenTelemetry.Ctx.get_current()
        })
    end
  end
end
