from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuración base de la rutina
default_args = {
    'owner': 'moises',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 20),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_exel_norte_postgres',
    default_args=default_args,
    description='Actualiza código desde GitHub y ejecuta run_pipeline.py para actualizar catálogo',
    schedule_interval='*/30 * * * *', # Se ejecuta cada 30 minutos
    catchup=False,
) as dag:

    # Sincroniza el repositorio por SSH
    actualizar_repo = BashOperator(
        task_id='git_pull_repo',
        bash_command='cd /home/vboxuser/proyectos_data/Directo_b2b_Exel_del_Norte && git pull origin main',
    )

    # Entra a la carpeta, activa el entorno virtual y ejecuta el pipeline
    ejecutar_pipeline = BashOperator(
        task_id='ejecutar_run_pipeline',
        bash_command='cd /home/vboxuser/proyectos_data/Directo_b2b_Exel_del_Norte && source /home/vboxuser/airflow_workspace/airflow_venv/bin/activate && python run_pipeline.py',
    )

    # Orden de ejecución
    actualizar_repo >> ejecutar_pipeline
