from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_handler import processar_orcamento_sp

# Função para rodar o ETL
def run_etl():
    GDV_DESPESAS_FILE = '/app/gdvDespesasExcel.csv'
    GDV_RECEITAS_FILE = '/app/gdvReceitasExcel.csv'
    DATA_INSERT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    processar_orcamento_sp(GDV_DESPESAS_FILE, GDV_RECEITAS_FILE, '20220622', DATA_INSERT)

# Definição do DAG
dag = DAG(
    'etl_sp_rec_orc',
    description='DAG para processar dados e enviar para o BigQuery',
    schedule_interval='@daily',  # Ajuste o agendamento conforme necessário
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Definição do operador para executar a função ETL
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag
)

run_etl_task