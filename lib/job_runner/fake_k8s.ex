defmodule FakeK8s do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{status: :idle}, name: __MODULE__)
  end

  def start_job(job_id, timer) do
    GenServer.cast(__MODULE__, {:start_job, job_id, timer})
  end

  def init(state) do
    {:ok, state}
  end

  def handle_cast({:start_job, job_id, timer}, _state) do
    Process.send_after(self(), {:complete_job, job_id}, timer)
    {:noreply, %{job_id: job_id, timer: timer, status: :running}}
  end

  def handle_info({:complete_job, _job_id}, state) do
    new_state = Map.put(state, :status, :completed)
    {:noreply, new_state}
  end

  def job_status(job_id) do
    GenServer.call(__MODULE__, {:status, job_id})
  end

  def handle_call({:status, _job_id}, _from, state) do
    {:reply, state[:status], state}
  end
end
