import os
import pandas as pd
import requests
from pandas import read_gbq
from pandas_gbq import to_gbq

# Função para obter cotação do dólar para o Real na data específica usando a API awesomeapi
def obter_cotacao_dolar_real(data):
    url = f'https://economia.awesomeapi.com.br/USD-BRL/10?start_date={data}&end_date={data}'
    response = requests.get(url)
    if response.status_code == 200:
        exchange = float(response.json()[0]['high'])
        return exchange
    else:
        raise ValueError(f'Erro ao obter cotação do dólar: {response.status_code}')

# Função para processar despesas e receitas
def processar_orcamento_sp(GDV_DESPESAS_FILE, GDV_RECEITAS_FILE, data, DATA_INSERT):
    # Leitura dos arquivos CSV
    df_despesas = pd.read_csv(GDV_DESPESAS_FILE, encoding='latin-1')
    df_despesas = df_despesas[['Fonte de Recursos', 'Liquidado']]
    df_receitas = pd.read_csv(GDV_RECEITAS_FILE, encoding='latin-1')
    df_receitas = df_receitas[['Fonte de Recursos', 'Arrecadado']]
    
    # Obter cotação do dólar para o Real na data específica (22/06/2022)
    cotacao_dolar_real = obter_cotacao_dolar_real(f'{data}')
    
    # Conversão dos valores dolarizados para reais
    df_despesas['total_real'] = pd.to_numeric(df_despesas['Liquidado'].str.replace('.', '').str.replace(',', '.')) * cotacao_dolar_real
    df_despesas['total_real'] = df_despesas['total_real'].round(2)
    df_receitas['total_real'] = pd.to_numeric(df_receitas['Arrecadado'].str.replace('.', '').str.replace(',', '.')) * cotacao_dolar_real
    df_receitas['total_real'] = df_receitas['total_real'].round(2)
    
    # Seleção de colunas necessárias
    df_despesas = df_despesas[['Fonte de Recursos', 'total_real']].groupby(['Fonte de Recursos'], as_index=False)['total_real'].sum().reset_index(drop=True)
    df_receitas = df_receitas[['Fonte de Recursos', 'total_real']].groupby(['Fonte de Recursos'], as_index=False)['total_real'].sum().reset_index(drop=True)
    
    # Renomear coluna para consistência
    df_despesas.rename(columns={'Fonte de Recursos': 'Fonte_Recursos', 'total_real': 'Liquidado'}, inplace=True)
    df_receitas.rename(columns={'Fonte de Recursos': 'Fonte_Recursos', 'total_real': 'Arrecadado'}, inplace=True)
    
    # Mesclar despesas e receitas
    df_dr = pd.merge(df_receitas, df_despesas, on=['Fonte_Recursos'], how='outer')
    
    # Criar ID da Fonte de Recursos e ajustar nome da fonte
    df_dr.insert(0, 'ID_Fonte_Recursos', df_dr['Fonte_Recursos'].str[:3])
    df_dr['Fonte_Recursos'] = df_dr['Fonte_Recursos'].str[6:]
    df_dr['Liquidado'] = df_dr['Liquidado'].fillna(0)
    df_dr['Arrecadado'] = df_dr['Arrecadado'].fillna(0)
    
    # Ordenar pelo ID da Fonte de Recursos
    df_dr = df_dr.sort_values(by='ID_Fonte_Recursos')
    
    # Adicionar coluna dt_insert
    df_dr['dt_insert'] = DATA_INSERT
    
    # ID do projeto no Google Cloud
    projeto_id = 'flawless-earth-428717-u5'
    
    # Defina a variável de ambiente para o caminho do arquivo de credenciais JSON
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/lgg57/OneDrive/Documentos/Github/KarHub/KarHub/service-account-file.json"

    # Exportar DataFrame para a tabela 'orcamento_sp.orcamento'
    to_gbq(df_dr, destination_table='orcamento_sp.orcamento', project_id=projeto_id, if_exists='replace')

    # Consultas SQL para responder às perguntas
    queries = {
        "5 Fontes de Recursos que mais Arrecadaram": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", "Arrecadado"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY Arrecadado DESC
            LIMIT 5
        """,
        "5 Fontes de Recursos que mais Gastaram": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", "Liquidado"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY Liquidado DESC
            LIMIT 5
        """,
        "5 Fontes de Recursos com a Melhor Margem Bruta": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", (Arrecadado - Liquidado) / Arrecadado AS "Margem Bruta"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY "Margem Bruta" DESC
            LIMIT 5
        """,
        "5 Fontes de Recursos que Menos Arrecadaram": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", "Arrecadado"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY Arrecadado ASC
            LIMIT 5
        """,
        "5 Fontes de Recursos que Menos Gastaram": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", "Liquidado"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY Liquidado ASC
            LIMIT 5
        """,
        "5 Fontes de Recursos com a Pior Margem Bruta": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", (Arrecadado - Liquidado) / Arrecadado AS "Margem Bruta"
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            ORDER BY "Margem Bruta" ASC
            LIMIT 5
        """,
        "Média de Arrecadação por Fonte de Recurso": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", AVG(Arrecadado) AS Media_Arrecadacao
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            GROUP BY "ID_Fonte_Recursos", "Fonte_Recursos"
        """,
        "Média de Gastos por Fonte de Recurso": """
            SELECT "ID_Fonte_Recursos", "Fonte_Recursos", AVG(Liquidado) AS Media_Gastos
            FROM `seu-projeto-id.orcamento_sp.orcamento`
            GROUP BY "ID_Fonte_Recursos", "Fonte_Recursos"
        """
    }

    # Executar consultas e mostrar resultados
    for descricao, query in queries.items():
        print(f"\n{descricao}:")
        df_resultado = read_gbq(query, project_id=projeto_id)
        print(df_resultado)
