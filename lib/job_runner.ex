defmodule JobRunner do
  def run_job(job_id \\ :erlang.unique_integer([:positive])) do
    :ok = FakeK8s.start_job(job_id, 5000)
    :ok = JobWatcher.watch_job(self(), job_id)

    receive do
      :job_completed ->
        System.halt(0)

      :job_failed ->
        System.halt(1)
    end
  end
end
