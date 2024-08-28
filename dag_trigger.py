from airflow.models import DagBag
from airflow.utils.state import State
from airflow.utils.timezone import utcnow

# Load the DAG
dagbag = DagBag()
dag_id = 'your_dag_id'
dag = dagbag.get_dag(dag_id)

if dag is None:
    print(f"DAG with ID {dag_id} not found!")
else:
    # Trigger the DAG
    dag_run = dag.create_dagrun(
        run_id=f"manual__{utcnow()}",
        state=State.RUNNING,
        conf={"key1": "value1"},
        execution_date=utcnow(),
        external_trigger=True
    )

    print(f"Triggered DAG: {dag_id}")
    print(f"Run ID: {dag_run.run_id}")
