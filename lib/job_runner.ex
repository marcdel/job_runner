defmodule JobRunner do
  use OpenTelemetryDecorator

  @decorate with_span("JobRunner.run_job", include: [:job_run_id])
  def run_job(job_run_id \\ :rand.uniform(100)) do
    :ok = FakeK8s.start_job(job_run_id)
    watch_and_wait(job_run_id, :rand.uniform(10))
  end

  @decorate with_span("JobRunner.watch_and_wait", include: [:job_run_id])
  defp watch_and_wait(job_run_id, deadline) do
    {:ok, _} = JobWatcher.watch_job(self(), job_run_id, deadline)

    receive do
      :job_completed ->
        O11y.add_event("job_completed", %{job_run_id: job_run_id})
        0

      :job_failed ->
        O11y.set_error(:job_failed)
        O11y.add_event("job_failed", %{job_run_id: job_run_id})
        1

      :job_timed_out ->
        O11y.set_error(:job_timed_out)
        O11y.add_event("job_timed_out", %{job_run_id: job_run_id})
        1

      {:job_watcher_terminated, reason, remaining_deadline} ->
        O11y.set_error(reason)

        O11y.add_event("job_watcher_terminated", %{
          job_run_id: job_run_id,
          reason: reason,
          remaining_deadline: remaining_deadline
        })

        watch_and_wait(job_run_id, remaining_deadline)
    end
  end
end
