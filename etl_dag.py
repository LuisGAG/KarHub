from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_handler import processar_orcamento_sp

# Definir argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 22),  # Data de início do DAG
    'retries': 1,
}

# Definir o DAG
dag = DAG(
    'etl_orcamento_sp',
    default_args=default_args,
    description='DAG para processar despesas e receitas e exportar para o BigQuery',
    schedule_interval=None,  # O DAG será executado manualmente
)

# Definir a tarefa do DAG
def run_etl():
    GDV_DESPESAS_FILE = '/app/gdvDespesasExcel.csv'
    GDV_RECEITAS_FILE = '/app/gdvReceitasExcel.csv'
    DATA_INSERT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    processar_orcamento_sp(GDV_DESPESAS_FILE, GDV_RECEITAS_FILE, '20220622', DATA_INSERT)

run_etl_task = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl,
    dag=dag,
)