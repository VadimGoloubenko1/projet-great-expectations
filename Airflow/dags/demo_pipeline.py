from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import great_expectations as gx
import great_expectations.expectations as gxe

from datetime import datetime
import pandas as pd
import shutil
import os

dag = DAG(
    dag_id='demo_pipeline',
    start_date=datetime(2026, 1, 1),
)

JAVA_PROJECT_ROOT = "/opt/airflow/log_process"
JAVA_COMMAND = (
    f"cd {JAVA_PROJECT_ROOT} && mkdir -p out "
    f"&& javac -d out src/imt/ibd/lambda/*.java "
    f"&& java -cp out imt.ibd.lambda.Main"
)

run_java_task = BashOperator(
    task_id='process_logs_with_java',
    bash_command=JAVA_COMMAND,
    do_xcom_push=True,
    dag=dag,
)

GE_ROOT = "/opt/airflow/great_expectations"

def cleanup_old_docs():
    destination_path = GE_ROOT
    for filename in os.listdir(destination_path):
        file_path = os.path.join(destination_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.is_link(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def gx_validate_logs():
    context = gx.get_context(context_root_dir=GE_ROOT)

    # 1. Chargement des données de votre TP Java
    df = pd.read_csv("/opt/airflow/log_process/output_logs.csv")

    # 2. Création de la suite de validation
    suite = context.suites.add(gx.ExpectationSuite(name="logs_suite"))

    # 3. Préparation de la validation (Suite et Attentes)
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="ip", regex=r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column="status", type_="int"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="status", min_value=100, max_value=599))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="size", min_value=0))
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))

    # 4. Définition des données (Batch)
    ds = context.data_sources.add_pandas(name="ds_logs")
    asset = ds.add_dataframe_asset(name="asset_logs")
    batch_def = asset.add_batch_definition_whole_dataframe(name="batch_logs")

    # 5. Création de la Validation Definition
    # C'est cet objet qui fait le lien entre vos données et vos tests
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="logs_validation",
            data=batch_def,
            suite=suite
        )
    )

    # 6. Exécution de la validation
    val_def.run(batch_parameters={"dataframe": df})

def gx_validate_stats():
    context = gx.get_context(context_root_dir=GE_ROOT)

    df = pd.read_csv("/opt/airflow/log_process/stats.csv")

    suite = context.suites.add(gx.ExpectationSuite(name="stats_suite"))

    # Règle 1 : Vérifier que la taille moyenne (average) est entre 500 et 5000
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="average", min_value=500, max_value=5000
    ))
    # Règle 2 : S'assurer que la somme totale (sum) n'est pas nulle
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="sum", min_value=0))

    ds = context.data_sources.add_pandas(name="ds_stats")
    asset = ds.add_dataframe_asset(name="asset_stats")
    batch_def = asset.add_batch_definition_whole_dataframe(name="batch_stats")

    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="stats_validation",
            data=batch_def,
            suite=suite
        )
    )

    val_def.run(batch_parameters={"dataframe": df})

def generate_data_docs():
    context = gx.get_context(context_root_dir=GE_ROOT)
    context.build_data_docs()

cleanup_old_docs_task = PythonOperator(
    task_id='cleanup_old_docs',
    python_callable=cleanup_old_docs,
    dag=dag
)

gx_validate_logs_task = PythonOperator(
    task_id='gx_validate_logs',
    python_callable=gx_validate_logs,
    dag=dag
)

gx_validate_stats_task = PythonOperator(
    task_id='gx_validate_stats',
    python_callable=gx_validate_stats,
    dag=dag
)

generate_data_docs_task = PythonOperator(
    task_id='generate_data_docs',
    python_callable=generate_data_docs,
    dag=dag
)

echo_complete_task = BashOperator(
    task_id='echo_complete',
    bash_command='echo "Processing complete"',
    dag=dag
)

run_java_task >> cleanup_old_docs_task >> gx_validate_logs_task >> gx_validate_stats_task >> generate_data_docs_task >> echo_complete_task