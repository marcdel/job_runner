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
      :start_time,
      :ref
    ]
  end

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: via(state.job_run_id))
  end

  def init(state) do
    {:ok, state}
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

  @decorate with_span("JobWatcher.start", include: [:state])
  def start(parent_pid, job_run_id) do
    state = %State{
      parent_pid: parent_pid,
      trace_ctx: OpenTelemetry.Ctx.get_current(),
      job_run_id: job_run_id,
      start_time: Time.utc_now()
    }

    DynamicSupervisor.start_child(JobWatcherSupervisor, {__MODULE__, state})
  end

  @decorate with_span("JobWatcher.watch_job", include: [:job_run_id])
  def watch_job(job_run_id, deadline) do
    GenServer.call(via(job_run_id), {:update_state, %{deadline: deadline, trace_ctx: OpenTelemetry.Ctx.get_current()}})
    GenServer.call(via(job_run_id), :start_watching)
  end

  def handle_call({:update_state, new_values}, _from, state) do
    new_state = %{state | start_time: Time.utc_now(), deadline: new_values.deadline, trace_ctx: new_values.trace_ctx}
    {:reply, :ok, new_state}
  end

  # In this case the task is already running, so we just return :ok.
  def handle_call(:start_watching, _from, %{ref: ref} = state)
      when is_reference(ref) do
    {:reply, :ok, state}
  end

  def handle_call(:start_watching, _from, %{ref: nil} = state) do
    task =
      Task.Supervisor.async_nolink(JobWatcherTaskSupervisor, fn ->
        OpenTelemetry.Ctx.attach(state.trace_ctx)
        FakeK8s.wait_until(state.job_run_id, state.deadline)
      end)

    new_state = %{state | ref: task.ref}

    {:reply, :ok, new_state}
  end

  # The task completed successfully
  def handle_info({ref, result}, %{ref: ref} = state) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])

    case result do
      {:ok, :job_successful} ->
        O11y.set_attributes(result: :job_successful)
        send(state.parent_pid, :job_successful)

      {:ok, :job_failed} ->
        O11y.set_error(:job_failed)
        send(state.parent_pid, :job_failed)

      {:error, :timeout} ->
        O11y.set_error(:job_timed_out)
        send(state.parent_pid, :job_timed_out)
    end

    {:noreply, %{state | ref: nil}}
  end

  # The task failed
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state) do
    # Log and possibly restart the task...
    Logger.error("JobWatcher task failed. reason: #{inspect(reason)}, state: #{inspect(state)}")
    O11y.set_error(reason)

    exponential_backoff()

    new_deadline = recalculate_deadline(state.deadline, state.start_time)

    task =
      Task.Supervisor.async_nolink(JobWatcherTaskSupervisor, fn ->
        OpenTelemetry.Ctx.attach(state.trace_ctx)
        FakeK8s.wait_until(state.job_run_id, new_deadline)
      end)

    new_state = %{state | ref: task.ref, start_time: Time.utc_now(), deadline: new_deadline}

    {:noreply, new_state}
  end

  def terminate(reason, state) do
    O11y.with_span("JobWatcher.terminate", fn ->
      OpenTelemetry.Ctx.attach(state.trace_ctx)
      O11y.set_attributes(reason: reason, state: state)
      O11y.set_error(reason)
      Logger.error("JobWatcher terminated. reason: #{inspect(reason)}, state: #{inspect(state)}")

      remaining_deadline = recalculate_deadline(state.deadline, state.start_time)
      O11y.set_attributes(remaining_deadline: remaining_deadline)

      send(state.parent_pid, {:job_watcher_terminated, reason, remaining_deadline})

      :ok
    end)
  end

  defp exponential_backoff do
    Process.sleep(:timer.seconds(3))
  end

  defp recalculate_deadline(nil, _start_time), do: nil

  @decorate with_span("JobWatcher.recalculate_deadline", include: [:deadline, :start_time, :current_time, :result])
  defp recalculate_deadline(deadline, start_time) do
    current_time = Time.utc_now()
    deadline - Time.diff(current_time, start_time, :second)
  end
end
