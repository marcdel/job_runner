defmodule JobRunner do
  use OpenTelemetryDecorator

  require Logger

  @decorate with_span("JobRunner.run_job", include: [:job_run_id])
  def run_job(job_run_id \\ :rand.uniform(100), deadline \\ :rand.uniform(10)) do
    :ok = FakeK8s.start_job(job_run_id)
    {:ok, ref} = JobWatcher.start(self(), job_run_id)

    exit_code = watch_and_wait(job_run_id, deadline, ref)

    dbg(exit_code)
  end

  @decorate with_span("JobRunner.watch_and_wait", include: [:job_run_id])
  defp watch_and_wait(job_run_id, deadline, ref) do
    :ok = JobWatcher.watch_job(job_run_id, deadline)

    receive do
      :job_successful ->
        O11y.add_event("job_successful", %{job_run_id: job_run_id})
        0

      :job_failed ->
        O11y.set_error(:job_failed)
        O11y.add_event("job_failed", %{job_run_id: job_run_id})
        1

      :job_timed_out ->
        O11y.set_error(:job_timed_out)
        O11y.add_event("job_timed_out", %{job_run_id: job_run_id})
        2

      {:DOWN, ^ref, :process, _pid, reason} ->
        Process.demonitor(ref, [:flush])
        O11y.set_error(reason)
        O11y.add_event("job_watcher_terminated", %{job_run_id: job_run_id, reason: reason})

        Logger.critical(
          "JobWatcher terminated. reason: #{inspect(reason)}, job_run_id: #{job_run_id}"
        )

        3
    end
  end
end
