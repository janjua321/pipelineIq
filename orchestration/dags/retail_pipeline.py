"""Manual local DAG; batch identity and input are explicit trigger parameters."""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="pipelineiq_retail", schedule=None, catchup=False, max_active_runs=1,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    params={
        "source": Param("data/source/a2-healthy-001.csv", type="string", minLength=1),
        "batch_id": Param("a2-healthy-001", type="string", pattern="^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"),
    },
    tags=["pipelineiq", "local"],
) as dag:
    environment = {
        "PIPELINEIQ_PROJECT": os.environ["PIPELINEIQ_PROJECT"],
        "PIPELINEIQ_SOURCE": "{{ params.source }}",
        "PIPELINEIQ_BATCH_ID": "{{ params.batch_id }}",
        "PIPELINEIQ_ORCHESTRATION_RUN": "{{ dag.dag_id }}:{{ run_id }}",
        "PIPELINEIQ_ATTEMPT": "{{ ti.try_number }}",
    }
    def command(task_id, action, **kwargs):
        return BashOperator(task_id=task_id,
                            bash_command=f'bash "$PIPELINEIQ_PROJECT/scripts/orchestrate.sh" {action}',
                            env=environment, append_env=True, do_xcom_push=False, skip_on_exit_code=None,
                            execution_timeout=timedelta(minutes=20), **kwargs)

    process_batch = command("process_batch", "process", retries=1, retry_delay=timedelta(seconds=5))
    publish_events = command("publish_events", "publish", trigger_rule="all_done", retries=2,
                             retry_delay=timedelta(seconds=5))
    verify_outcome = command("verify_outcome", "verify")
    process_batch >> publish_events >> verify_outcome
    process_batch >> verify_outcome
