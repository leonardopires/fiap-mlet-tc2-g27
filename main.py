import time
import re
from pathlib import Path

import requests
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import Index
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import os
from datetime import datetime

# Configurações AWS
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
aws_session_token = os.environ['AWS_SESSION_TOKEN']
region = os.environ['AWS_REGION']
bucket_name = os.environ['S3_BUCKET_NAME']

# Função para baixar o arquivo
def download_file(url, local_filename):
    response = requests.get(url)
    response.raise_for_status()
    with open(local_filename, 'wb') as f:
        f.write(response.content)


# Função para salvar DataFrame como Parquet
def save_to_parquet(df, output_path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)


# Função para fazer upload para S3
def upload_to_s3(local_file, bucket_name, s3_file):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token,
                      region_name=region)
    s3.upload_file(local_file, bucket_name, s3_file)


def busca_ultimo_arquivo(indice, download_dir, tempo_espera_maximo, ultimo_arquivo=None):
    tempo_inicial = time.time()
    arquivo_encontrado = False
    local_filename = None

    while time.time() - tempo_inicial < tempo_espera_maximo:
        # Verifica se o arquivo foi baixado
        arquivos = sorted([f for f in os.listdir(download_dir) if f.endswith('.csv') and f.startswith(indice)],
                          key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
        if arquivos:
            local_filename = os.path.join(download_dir, arquivos[0])

            if local_filename != ultimo_arquivo:
                arquivo_encontrado = True
                break
        elif not ultimo_arquivo:
            arquivo_encontrado = True
            local_filename = 'INICIAL'
            break

        time.sleep(1)  # Espera um segundo antes de verificar novamente

    if arquivo_encontrado:
        print(f"Arquivo encontrado: {local_filename}")
        return local_filename
    else:
        return ''


# Função para buscar e baixar o arquivo CSV da página
def busca_carteira_teorica(indice, tempo_espera_maximo=60):
    download_dir = os.path.expanduser("~\\fiap-mlet-m2")  # Diretório padrão de downloads no Windows

    url = f'https://sistemaswebb3-listados.b3.com.br/indexPage/day/{indice.upper()}?language=pt-br'
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new")
    prefs = {"download.default_directory": download_dir}
    chrome_options.add_experimental_option("prefs", prefs)
    print("Iniciando Chrome")
    wd = webdriver.Chrome(options=chrome_options)  # Inicializa o webdriver (certifique-se de ter o chromedriver instalado)
    wd.get(url)

    print("Buscando ultimo arquivo")
    local_filename = busca_ultimo_arquivo(indice, download_dir, tempo_espera_maximo)

    # Localiza e clica no botão de download
    try:
        segment_combo = wd.find_element(By.ID, 'segment')
        segment_combo = Select(segment_combo)
        wd.implicitly_wait(10)
        segment_combo.select_by_value('2')

        download_button = wd.find_element(By.LINK_TEXT, "Download")
        download_button.click()
        print("Botão de download clicado, aguardando conclusão do download...")

        local_filename = busca_ultimo_arquivo(indice, download_dir, tempo_espera_maximo, local_filename)

        if not local_filename:
            raise IOError("Não foi possível encontrar o arquivo baixado")

    except Exception as e:
        print(f"Erro ao tentar baixar o arquivo: {e}")
        raise
    finally:
        wd.quit()

    return local_filename

def convert_to_numeric(value):
    return pd.to_numeric(value.replace('.', '').replace(',', '.'))

# Processo principal
def main():
    indice = 'IBOV'
    local_csv = busca_carteira_teorica(indice)
    filename = Path(local_csv).name
    pattern = r'\d{2}-\d{2}-\d{2}'

    # Procurar por correspondências na string do nome do arquivo
    match = re.search(pattern, filename)

    if match:
        # Se encontrou a data, captura o resultado
        date_part = match.group(0)
        print(f'Data extraída do nome do arquivo: {date_part}')
        # Carrega o CSV em um DataFrame
        df = pd.read_csv(local_csv, sep=';', encoding='ISO-8859-1', skipfooter=2, engine='python', converters={'Qtde. Teórica': convert_to_numeric, 'Part. (%)': convert_to_numeric})
        df.drop(df.columns[0], axis=1, inplace=True)
        data_atual = pd.to_datetime(date_part, format='%d-%m-%y').strftime("%Y%m%d")
        df['data'] = data_atual
        print(df)

        # Caminho para salvar o arquivo Parquet localmente
        local_parquet = 'carteira_teorica.parquet'
        save_to_parquet(df, local_parquet)


        # Nome do arquivo no S3 com data incluída no nome
        s3_file = f'input/carteira_teorica_{indice}_latest.parquet'
        s3_part_file = f'parquet/carteira_teorica_{indice}_{data_atual}.parquet'

        # Faz upload para o S3
        upload_to_s3(local_parquet, bucket_name, s3_file)
        print(f"Arquivo {local_parquet} enviado para o S3 como {s3_file}")

        upload_to_s3(local_parquet, bucket_name, s3_part_file)
        print(f"Arquivo {local_parquet} enviado para o S3 como {s3_part_file}")

    else:
        print('Nenhuma data encontrada no nome do arquivo.')
        exit(0)


if __name__ == '__main__':
    main()