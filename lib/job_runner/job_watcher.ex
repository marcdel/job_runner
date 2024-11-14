defmodule JobWatcher do
  use GenServer
  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def watch_job(pid, job_id) do
    GenServer.cast(__MODULE__, {:start, pid, job_id})
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

  def handle_cast({:start, pid, job_id}, _state) do
    state = %{pid: pid, job_id: job_id}
    save_state(state)
    schedule_check()
    {:noreply, state}
  end

  def handle_info(:check_status, state) do
    fifty_fifty = :rand.uniform() > 0.5

    if fifty_fifty do
      check_status(state)
    else
      {:stop, :random_error, state}
    end
  end

  def terminate(reason, state) do
    Logger.warning("JobWatcher terminated with reason: #{inspect(reason)}")
    save_state(state)
    :ok
  end

  defp check_status(state) do
    case FakeK8s.job_status(state.job_id) do
      :completed ->
        IO.puts("Job completed successfully")
        send(state.pid, :job_completed)

      :failed ->
        IO.puts("Job failed")
        send(state.pid, :job_failed)

      _ ->
        schedule_check()
    end

    {:noreply, state}
  end

  defp schedule_check do
    Process.send_after(self(), :check_status, :timer.seconds(1))
  end

  defp save_state(state) do
    :ets.insert(table_name(), {:state, state})
    state
  end

  defp load_state do
    case :ets.lookup(table_name(), :state) do
      [{:state, state}] -> state
      _ -> %{pid: nil, job_id: nil}
    end
  end
end
