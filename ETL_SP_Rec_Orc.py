from datetime import datetime
from data_handler import processar_orcamento_sp

# Definir constantes
GDV_DESPESAS_FILE = '/app/gdvDespesasExcel.csv'
GDV_RECEITAS_FILE = '/app/gdvReceitasExcel.csv'
DATA_INSERT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Rodar a função para processar os dados e exportar para o BigQuery
def run_etl():
    processar_orcamento_sp(GDV_DESPESAS_FILE, GDV_RECEITAS_FILE, '20220622', DATA_INSERT)

# Se você quiser executar o script fora do Airflow, descomente a linha abaixo:
# if __name__ == "__main__":
#     run_etl()