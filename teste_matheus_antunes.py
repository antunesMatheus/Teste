### Teste de engenharia de dados

#Bibliotecas utilizadas

import requests
import pandas as pd
import json
import boto3
import io

#Variaveis
url = "https://jsonplaceholder.typicode.com/posts"

resposta = requests.get(url)
dados = resposta.json()
s3 = boto3.client('s3', region_name='us-east-2')
bucket_name = 'pbucketaws'
file_name_json = 'posts_dados.json'
file_name_parquet = 'post_dados.parquet'
#Conectando a API
#Teste de conexão
if resposta.status_code == 200:
    print(f"Conexão bem sucedida", {resposta.status_code})
else:
    print(f"Falha na conexão", {resposta.status_code})

# Verificando os dados da API

df = pd.DataFrame(dados)

print(df.head())

# Armazenando arquivo bruto na S3


json_dados = df.to_json(orient='records')
s3.put_object(Bucket=bucket_name, Key=file_name_json, Body=json_dados)
print(f"Arquivo {file_name_json} carregado com sucesso no bucket {bucket_name}")

# Tratamento de dados

# Filtrando as colunas
df_filtrado = df[['id', 'title', 'userId']]

#Coluna derivada

df_filtrado['Tamanho_titulo'] = df_filtrado['title'].apply(len)

# Convertendo os dados para parquet e salvando no AWS S3

buffer = io.BytesIO() #Criar buffer de memória
df_filtrado.to_parquet(buffer, engine='pyarrow', index=False)#Salva no buffer

s3.put_object(Bucket=bucket_name,Key=file_name_parquet, Body=buffer.getvalue())
print(f"Arquivo {file_name_parquet} enviado para o bucket {bucket_name}")