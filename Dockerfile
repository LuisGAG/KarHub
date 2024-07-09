# Use uma imagem base do Python com Airflow
FROM apache/airflow:2.7.0-python3.9

# Defina um diretório de trabalho
WORKDIR /app

# Copie os arquivos necessários para o contêiner
COPY data_handler.py /app/data_handler.py
COPY ETL_SP_Rec_Orc.py /app/ETL_SP_Rec_Orc.py
COPY gdvDespesasExcel.csv /app/gdvDespesasExcel.csv
COPY gdvReceitasExcel.csv /app/gdvReceitasExcel.csv
COPY service-account-file.json /app/service-account-file.json

# Instale as dependências adicionais
RUN pip install pandas requests pandas-gbq google-cloud-bigquery

# Defina a variável de ambiente para o caminho do arquivo de credenciais JSON
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account-file.json"

# Copie o arquivo de configuração do Airflow
COPY airflow.cfg /root/airflow/airflow.cfg

# Copie o DAG para o diretório de DAGs do Airflow
COPY etl_dag.py /opt/airflow/dags/etl_dag.py

# Defina o comando para rodar o Airflow
CMD ["airflow", "webserver", "--daemon"]