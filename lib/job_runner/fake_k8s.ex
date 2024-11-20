defmodule FakeK8s do
  use OpenTelemetryDecorator

  @decorate with_span("FakeK8s.start_job", include: [:_job_run_id])
  def start_job(_job_run_id) do
    :ok
  end

  @decorate with_span("FakeK8s.wait_until", include: [:job_run_id])
  def wait_until(job_run_id, deadline) do
    Process.sleep(:timer.seconds(1))

    job_successful? = :rand.uniform() > 0.50
    watch_successful? = :rand.uniform() > 0.75

    if !watch_successful? do
      raise "watch failed"
    end

    if job_successful? do
      {:ok, :job_successful}
    else
      {:ok, :job_failed}
    end
  end
end
