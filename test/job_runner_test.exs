defmodule JobRunnerTest do
  use ExUnit.Case
  doctest JobRunner

  test "greets the world" do
    assert JobRunner.run_job(:rand.uniform(100)) == :world
  end
end
