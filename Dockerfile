# Use uma imagem base do Python
FROM python:3.9.9-slim

# Defina um diretório de trabalho
WORKDIR /app

# Copie os arquivos necessários para o contêiner
COPY data_handler.py /app/data_handler.py
COPY ETL_SP_Rec_Orc.py /app/ETL_SP_Rec_Orc.py
COPY gdvDespesasExcel.csv /app/gdvDespesasExcel.csv
COPY gdvReceitasExcel.csv /app/gdvReceitasExcel.csv
COPY service-account-file.json /app/service-account-file.json

# Instale as dependências
RUN pip install pandas requests pandas-gbq google-cloud-bigquery

# Defina a variável de ambiente para o caminho do arquivo de credenciais JSON
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account-file.json"

# Defina o comando para rodar o script
CMD ["python", "ETL_SP_Rec_Orc.py"]