defmodule JobWatcher do
  use GenServer
  use OpenTelemetryDecorator

  require Logger

  defmodule State do
    @moduledoc false

    @derive {O11y.SpanAttributes, except: [:trace_ctx, :k8s_client]}
    defstruct [
      :parent_pid,
      :trace_ctx,
      :job_run_id,
      :deadline,
      :start_time
    ]
  end

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: via(state.job_run_id))
  end

  def init(state) do
    {:ok, state, {:continue, :start_watching}}
  end

  def child_spec(state) do
    %{
      id: {__MODULE__, state.job_run_id},
      start: {__MODULE__, :start_link, [state]},
      restart: :transient
    }
  end

  def via(job_run_id) do
    {:via, Registry, {JobWatcherRegistry, job_run_id}}
  end

  @decorate with_span("JobWatcher.watch_job", include: [:state])
  def watch_job(parent_pid, job_run_id, deadline) do
    state = %State{
      parent_pid: parent_pid,
      trace_ctx: OpenTelemetry.Ctx.get_current(),
      job_run_id: job_run_id,
      deadline: deadline,
      start_time: Time.utc_now()
    }

    DynamicSupervisor.start_child(JobWatcherSupervisor, {__MODULE__, state})
  end

  @decorate with_span("JobWatcher.start_watching", include: [:state])
  def handle_continue(:start_watching, state) do
    %{
      parent_pid: parent_pid,
      trace_ctx: trace_ctx,
      job_run_id: job_run_id,
      deadline: deadline
    } = state

    O11y.set_attributes(job_run_id: job_run_id)
    OpenTelemetry.Ctx.attach(trace_ctx)
    O11y.set_attributes(job_run_id: job_run_id)

    case FakeK8s.wait_until(job_run_id, deadline) do
      {:ok, :job_successful} ->
        send(parent_pid, :job_completed)

      {:ok, :job_failed} ->
        O11y.set_error(:job_failed)
        send(parent_pid, :job_failed)

      {:error, :timeout} ->
        O11y.set_error(:job_timed_out)
        send(parent_pid, :job_timed_out)
    end

    {:stop, :normal, state}
  rescue
    error -> {:stop, error, state}
  end

  @decorate with_span("JobWatcher.terminate")
  def terminate(:finished_watching, state) do
    O11y.set_attributes(reason: :finished_watching, state: state)
    :ok
  end

  @decorate with_span("JobWatcher.terminate")
  def terminate(reason, state) do
    OpenTelemetry.Ctx.attach(state.trace_ctx)
    O11y.set_attributes(reason: reason, state: state)
    O11y.set_error(reason)
    Logger.error("JobWatcher terminated. reason: #{inspect(reason)}, state: #{inspect(state)}")

    remaining_deadline = calculate_remaining_deadline(state.deadline, state.start_time)
    O11y.set_attributes(remaining_deadline: remaining_deadline)

    send(state.parent_pid, {:job_watcher_terminated, reason, remaining_deadline})

    :ok
  end

  defp calculate_remaining_deadline(nil, _start_time), do: nil

  defp calculate_remaining_deadline(deadline, start_time) do
    deadline - Time.diff(Time.utc_now(), start_time, :second)
  end
end
