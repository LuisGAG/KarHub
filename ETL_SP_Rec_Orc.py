import pandas as pd
import requests
from datetime import datetime

# Definir constantes
GDV_DESPESAS_FILE = 'gdvDespesasExcel.csv'
GDV_RECEITAS_FILE = 'gdvReceitasExcel.csv'
DATA_INSERT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Função para obter cotação do dólar para o Real na data específica usando a API awesomeapi
def obter_cotacao_dolar_real(data):
    url = f'https://economia.awesomeapi.com.br/USD-BRL/10?start_date={data}&end_date={data}'
    response = requests.get(url)
    exchange = float(response.json()[0]['high'])
    if response.status_code == 200:
        return exchange
    else:
        raise ValueError(f'Erro ao obter cotação do dólar: {response.status_code}')
    
    # Função para processar despesas e receitas
def processar_orcamento_sp():
    # Leitura dos arquivos CSV
    df_despesas = pd.read_csv(GDV_DESPESAS_FILE, encoding='latin-1')
    df_despesas = df_despesas[['Fonte de Recursos','Liquidado']]
    df_receitas = pd.read_csv(GDV_RECEITAS_FILE, encoding='latin-1')
    df_receitas = df_receitas[['Fonte de Recursos','Arrecadado']]
    
    # Obter cotação do dólar para o Real na data específica (22/06/2022)
    cotacao_dolar_real = obter_cotacao_dolar_real('20220622')
    
    # Conversão dos valores dolarizados para reais
    df_despesas['total_real'] = pd.to_numeric(df_despesas['Liquidado'].str.replace('.','').str.replace(',','.')) * cotacao_dolar_real
    df_despesas['total_real'] = df_despesas['total_real'].round(2)
    df_receitas['total_real'] = pd.to_numeric(df_receitas['Arrecadado'].str.replace('.','').str.replace(',','.')) * cotacao_dolar_real
    df_receitas['total_real'] = df_receitas['total_real'].round(2)
    
    # Seleção de colunas necessárias
    df_despesas = df_despesas[['Fonte de Recursos', 'total_real']].groupby(['Fonte de Recursos'], as_index=False)['total_real'].sum().reset_index(drop=True)
    df_receitas = df_receitas[['Fonte de Recursos', 'total_real']].groupby(['Fonte de Recursos'], as_index=False)['total_real'].sum().reset_index(drop=True)
    
    # Renomear coluna para consistência
    df_despesas.rename(columns={'total_real': 'Liquidado'}, inplace=True)
    df_receitas.rename(columns={'total_real': 'Arrecadado'}, inplace=True)
    print(df_despesas.columns)
    print(df_receitas.columns)
    
    df_dr=pd.merge( df_receitas,
                    df_despesas,
                    on=['Fonte de Recursos'], 
                    how='outer')
    
    
    df_dr.insert(0,'ID Fonte de Recursos', df_dr['Fonte de Recursos'].str[:3])
    df_dr['Fonte de Recursos'] = df_dr['Fonte de Recursos'].str[6:]
    df_dr['Liquidado'] = df_dr['Liquidado'].fillna(0)
    df_dr['Arrecadado'] = df_dr['Arrecadado'].fillna(0)
    
    # Adicionar coluna dt_insert
    df_dr['dt_insert'] = DATA_INSERT
    
    return df_dr

# Salvar dados no BigQuery (exemplo usando Google Cloud SDK)
    # df_despesas.to_gbq(destination_table='orcamento_sp.despesas', project_id='seu-projeto-id', if_exists='replace')
    # df_receitas.to_gbq(destination_table='orcamento_sp.receitas', project_id='seu-projeto-id', if_exists='replace')
    
    # Exemplo de consulta SQL para responder às perguntas
    # Exemplo: Quais são as 5 fontes de recursos que mais arrecadaram?
    # query = """
    #         SELECT id_fonte_recurso, nome_fonte_recurso, total_arrecadado
    #         FROM orcamento_sp.receitas
    #         ORDER BY total_arrecadado DESC
    #         LIMIT 5
    #         """
    # df_resultado = pd.read_gbq(query, project_id='seu-projeto-id')
    
    # Retornar DataFrame final (opcional)
    
df_receitas_despesas = processar_orcamento_sp()