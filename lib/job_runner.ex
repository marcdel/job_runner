defmodule JobRunner do
  use OpenTelemetryDecorator

  @decorate with_span("JobRunner.run_job", include: [:job_id])
  def run_job(job_id \\ :erlang.unique_integer([:positive])) do
    :ok = FakeK8s.start_job(job_id, 5000)
    :ok = JobWatcher.watch_job(self(), job_id)

    receive do
      :job_completed ->
        O11y.add_event("job_completed", %{job_id: job_id})
        0

      :job_failed ->
        O11y.add_event("job_failed", %{job_id: job_id})
        1
    end
  end
end
