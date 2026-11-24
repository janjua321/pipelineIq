"""Run in Airflow's isolated environment; assert scheduling and shell boundaries."""
import os
import runpy
from pathlib import Path

dag = runpy.run_path(str(Path(os.environ["PIPELINEIQ_PROJECT"]) / "orchestration/dags/retail_pipeline.py"))["dag"]
assert set(dag.task_ids) == {"process_batch", "publish_events", "verify_outcome"}
assert dag.schedule is None and dag.catchup is False and dag.max_active_runs == 1
assert dag.get_task("process_batch").retries == 1
assert dag.get_task("publish_events").retries == 2
assert dag.get_task("publish_events").trigger_rule == "all_done"
assert dag.get_task("verify_outcome").trigger_rule == "all_success"
assert dag.get_task("verify_outcome").upstream_task_ids == {"process_batch", "publish_events"}
for task in dag.tasks:
    assert "{{" not in task.bash_command  # User parameters must not become shell source.
    assert task.env["PIPELINEIQ_SOURCE"] == "{{ params.source }}"
print("DAG structure, retry policy, terminal verdict and parameter isolation verified.")
