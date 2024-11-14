defmodule FakeK8s do
  use GenServer
  use OpenTelemetryDecorator

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{status: :idle}, name: __MODULE__)
  end

  @decorate with_span("FakeK8s.start_job", include: [:job_id, :timer])
  def start_job(job_id, timer) do
    ctx = OpenTelemetry.Ctx.get_current()
    GenServer.cast(__MODULE__, {:start_job, job_id, timer, ctx})
  end

  def init(state) do
    {:ok, state}
  end

  def handle_cast({:start_job, job_id, timer, ctx}, _state) do
    state = %{job_id: job_id, timer: timer, status: :running, trace_ctx: ctx}
    OpenTelemetry.Ctx.attach(ctx)
    O11y.set_attributes(state)
    Process.send_after(self(), {:complete_job, job_id}, timer)
    {:noreply, state}
  end

  def handle_info({:complete_job, _job_id}, state) do
    OpenTelemetry.Ctx.attach(state.trace_ctx)
    O11y.add_event("job_completed", %{job_id: state.job_id})
    new_state = Map.put(state, :status, :completed)
    {:noreply, new_state}
  end

  @decorate with_span("FakeK8s.job_status", include: [:job_id])
  def job_status(job_id) do
    ctx = OpenTelemetry.Ctx.get_current()
    GenServer.call(__MODULE__, {:status, job_id, ctx})
  end

  def handle_call({:status, _job_id, ctx}, _from, state) do
    OpenTelemetry.Ctx.attach(ctx)
    O11y.set_attributes(state)
    {:reply, state[:status], state}
  end
end
