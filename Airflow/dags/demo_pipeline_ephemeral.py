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
    dag_id='demo_pipeline_ephemeral',
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

def run_gx_with_full_docs():
    context = gx.get_context()

    """
    Partie 1 : Validation des données de logs
    """

    # 1. Chargement des données de votre TP Java
    df_logs = pd.read_csv("/opt/airflow/log_process/output_logs.csv")

    # 2. Création de la suite de validation
    suite = context.suites.add(gx.ExpectationSuite(name="logs_suite"))

    # 3. Préparation de la validation (suite et attentes)
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="ip", regex=r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column="status", type_="int"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="status", min_value=100, max_value=599))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="size", min_value=0))
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))

    # 4. Définition des données (Batch)
    ds = context.data_sources.add_pandas(name="java_ds")
    asset = ds.add_dataframe_asset(name="java_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(name="batch_def")

    # 5. Création de la validation definition
    # C'est cet objet qui fait le lien entre données et tests
    val_def_logs = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name="log_validation",
            data=batch_def,
            suite=suite
        )
    )

    """
    Partie 2 : Validation des statistiques
    """

    df_stats = pd.read_csv("/opt/airflow/log_process/stats.csv")

    suite_stats = context.suites.add(gx.ExpectationSuite(name="stats_suite"))

    # Règle 1 : vérifier que la taille moyenne (average) est entre 500 et 5000
    suite_stats.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="average", min_value=500, max_value=5000
    ))
    # Règle 2 : s'assurer que la somme totale (sum) n'est pas nulle
    suite_stats.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="sum", min_value=0))

    ds_stats = context.data_sources.add_pandas(name="ds_stats")
    asset_stats = ds_stats.add_dataframe_asset(name="asset_stats")
    batch_stats = asset_stats.add_batch_definition_whole_dataframe(name="batch_stats")

    val_def_stats = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="stats_validation", data=batch_stats, suite=suite_stats)
    )


    checkpoint_logs = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="logs_checkpoint",
            validation_definitions=[val_def_logs],
            actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="update_data_docs_action")],
        )
    )

    checkpoint_stats = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="stats_checkpoint",
            validation_definitions=[val_def_stats],
            actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="update_data_docs_action")],
        )
    )

    checkpoint_logs.run(batch_parameters={"dataframe": df_logs})
    checkpoint_stats.run(batch_parameters={"dataframe": df_stats})

    index_page_url = context.build_data_docs().get("local_site")

    # Transfert vers le volume Docker
    temp_docs_path = index_page_url.replace("file://", "").replace("index.html", "")
    destination_path = "/opt/airflow/great_expectations/data_docs"
    # On nettoie l'ancienne version s'il y en a une
    if os.path.exists(destination_path):
        shutil.rmtree(destination_path)
    # On copie tout le site web vers le volume
    shutil.copytree(temp_docs_path, destination_path)

    print(f"Rapport HTML complet copié vers {destination_path}")

gx_validate_task = PythonOperator(
    task_id='validate_logs_with_full_docs',
    python_callable=run_gx_with_full_docs
)

echo_complete_task = BashOperator(
    task_id='echo_complete',
    bash_command='echo "Processing complete"',
    dag=dag
)

run_java_task >> gx_validate_task >> echo_complete_task