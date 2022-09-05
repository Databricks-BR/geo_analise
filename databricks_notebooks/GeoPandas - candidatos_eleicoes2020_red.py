# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://colab.research.google.com/github/Rosangelafl/Eleicoes_Brasil/blob/master/candidatos_eleicoes2020_red.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# COMMAND ----------

# MAGIC %md
# MAGIC Análise dos dados do TSE, eleições locais 2020. 
# MAGIC 
# MAGIC Fonte:https://www.tse.jus.br/eleicoes/estatisticas/repositorio-de-dados-eleitorais-1/repositorio-de-dados-eleitorais
# MAGIC 
# MAGIC Baixados em 1 de setembro 2020
# MAGIC 
# MAGIC Planilhas csv e documentação em https://bit.ly/3jLmQt5
# MAGIC 
# MAGIC __Autor__
# MAGIC * Rosangela Lotfi  (rosangelafl.jor@gmail.com)
# MAGIC 
# MAGIC __GitHub Link__
# MAGIC * https://github.com/Rosangelafl/Eleicoes_Brasil

# COMMAND ----------

#importando as bibliotecas e definindo estilo dos gráficos

import pandas as pd
from pathlib import Path
import glob, os
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import style

#estilo 

style.use('ggplot')
plt.rcParams['figure.figsize'] = (20,10)

# COMMAND ----------

# Juntando as planilhas, uma para cada estado, apenas com a colunas para análises

path = r'/content/drive/My Drive/eleicoes2020_dados_tse/eleições2020/consulta_cand_2020' 
all_files = glob.glob(os.path.join(path, "*.csv"))

li = []

for filename in all_files:
    df = pd.read_csv(filename, encoding='latin-1', index_col=None, low_memory=False, error_bad_lines=False, sep=';', quotechar='"', 
                     usecols=['NM_UE','DS_CARGO','SQ_CANDIDATO','NM_CANDIDATO','NM_URNA_CANDIDATO', 'SG_PARTIDO',
                              'NR_IDADE_DATA_POSSE','DS_GENERO', 'DS_GRAU_INSTRUCAO','DS_COR_RACA','DS_OCUPACAO',
                              'VR_DESPESA_MAX_CAMPANHA'])
                   
    li.append(df)

df = pd.concat(li, axis=0, ignore_index=True, sort=False)

# COMMAND ----------

df.index

# COMMAND ----------

#verificando valores nulos
df.isnull().sum()

# COMMAND ----------

df.head(3).T

# COMMAND ----------

# Descobrindo quantos candidatos/valores únicos

df['SQ_CANDIDATO'].nunique()

# COMMAND ----------

# Elimando as linhas duplicadas
df.drop_duplicates(keep='last', inplace=True)

# COMMAND ----------

#Juntando os csv do TSE com os bens dos candidatos do país todo, disponível em https://bit.ly/3lnh6Ge

bens_cand = pd.read_csv('/content/drive/My Drive/eleicoes2020_dados_tse/eleições2020/bem_candidato_2020/bem_candidato_2020_BRASIL.csv', 
                        encoding='latin-1', index_col=None, low_memory=False, error_bad_lines=False, decimal=',',
                        sep=';', quotechar='"', usecols=['SG_UF', 'SG_UE','SQ_CANDIDATO','DS_BEM_CANDIDATO','VR_BEM_CANDIDATO'])

# COMMAND ----------

bens_cand.head()

# COMMAND ----------

bens_cand.columns = ['CODIGO_TSE' if x=='SG_UE' else x for x in bens_cand.columns] 

# COMMAND ----------

bens_cand.isnull().sum()

# COMMAND ----------

bens_cand.info()

# COMMAND ----------

# Somando os bens por candidato 

df_bens = pd.pivot_table(bens_cand,index=['SQ_CANDIDATO'],aggfunc={'VR_BEM_CANDIDATO':np.sum})
df_bens   

# COMMAND ----------

#Criando a planilha final

candidatos = df.merge(df_bens, on='SQ_CANDIDATO', how='left')
candidatos.head(3).T

# COMMAND ----------

candidatos.isnull().sum()

# COMMAND ----------

candidatos.info()

# COMMAND ----------

#candidatos.drop_duplicates(keep='last', inplace=True)

# COMMAND ----------

#Checando se a quantidade de candidatos não foi alterada com o merge

candidatos['SQ_CANDIDATO'].nunique()

# COMMAND ----------

candidatos.groupby(by='DS_GENERO').size()

# COMMAND ----------

#Printando a porcentagem de homens e mulheres candidatas e depois plotando

print('candidatas:', ((181848/548339) *100))
print('candidatos:', ((366491/548339) *100))

# COMMAND ----------

genero = df.groupby(by='DS_GENERO').size()
genero.plot(kind ='bar')
plt.show()

# COMMAND ----------

df.groupby(by='DS_COR_RACA').size()

# COMMAND ----------

raca = df.groupby(by='DS_COR_RACA').size()

# COMMAND ----------

print('candidatos autodeclarados de origem asiatica:', ((1946/ 548339) *100))
print('candidatos autodeclarados brancos:', ((262048/ 548339) *100))
print('candidatos autodeclarados indigenas:', ((2173/ 548339) *100))
print('candidatos autodeclarados pardos:', ((216341/ 548339) *100))
print('candidatos autodeclarados pretos:', ((57345/ 548339) *100))
print('candidatos que não declararam raça/etnia:', ((8486/ 548339) *100))

# COMMAND ----------

raca.sort_values(ascending=True).plot(kind='barh')
plt.show()

# COMMAND ----------

candidatos['DS_OCUPACAO'].value_counts()

# COMMAND ----------

candidatos.groupby(by='DS_GENERO')['DS_OCUPACAO'].value_counts()

# COMMAND ----------

candidatos.groupby(by='DS_GENERO')['DS_GRAU_INSTRUCAO'].value_counts()

# COMMAND ----------

candidatos.groupby(by='DS_GENERO')['DS_GRAU_INSTRUCAO'].value_counts().sort_values(ascending=True).plot(kind='barh')

# COMMAND ----------

profissoes = candidatos[['DS_OCUPACAO']].drop_duplicates().sort_values('DS_OCUPACAO').set_index('DS_OCUPACAO')
profissoes.head(50)

# COMMAND ----------

# Procurando candidatos que são policiais
policial = candidatos[candidatos['DS_OCUPACAO'].str.contains('POLICIAL')]
policial

# COMMAND ----------

candidatos[candidatos['DS_OCUPACAO'] == 'EMPRESÁRIO']

# COMMAND ----------

#Fazendo um recorte dos candidatos que se declaram empresários
cand_empresarios = candidatos[candidatos['DS_OCUPACAO'] == 'EMPRESÁRIO']

# COMMAND ----------

cand_empresarios['VR_BEM_CANDIDATO'].max()

# COMMAND ----------

#Identificando o candidato com maior valor de bens declarados da planilha do TSE
cand_empresarios[cand_empresarios['VR_BEM_CANDIDATO'] == 10613585493.22]

# COMMAND ----------

cand_empresarios['VR_BEM_CANDIDATO'].mean()

# COMMAND ----------

cand_empresarios.describe()

# COMMAND ----------

#Os 25% mais ricos

mais_ricos = cand_empresarios[cand_empresarios['VR_BEM_CANDIDATO'] > 4.500000e+05]

# COMMAND ----------

mais_ricos.groupby(by='DS_GENERO')['SG_PARTIDO'].value_counts(ascending=True).plot(kind='barh')

# COMMAND ----------

mais_ricos.nlargest(10, 'VR_BEM_CANDIDATO')

# COMMAND ----------

candidatos.groupby(by='DS_GENERO')['DS_CARGO'].value_counts(ascending=True).plot(kind='barh')

# COMMAND ----------

#Contando o número de candidatos por partido
candidatos['SG_PARTIDO'].value_counts()

# COMMAND ----------

candidatos['SG_PARTIDO'].value_counts(ascending=True).plot(kind='barh')

# COMMAND ----------

candidatos['VR_DESPESA_MAX_CAMPANHA'].max()

# COMMAND ----------

#Candidatos a prefeito da cidade de São Paulo têm mais dinheiro para gastar na campanha

candidatos[candidatos['VR_DESPESA_MAX_CAMPANHA'] == 51799384]

# COMMAND ----------

candidatos['VR_DESPESA_MAX_CAMPANHA'].mean()

# COMMAND ----------

candidatos['VR_DESPESA_MAX_CAMPANHA'].median()                        # -1 == não declarou

# COMMAND ----------

candidatos['VR_BEM_CANDIDATO'].median()

# COMMAND ----------

#Agrupando bens e genêro. Riqueza também é uma questão de genêro
candidatos[['DS_GENERO', 'VR_BEM_CANDIDATO']].groupby('DS_GENERO').mean().sort_values('VR_BEM_CANDIDATO', 
                                                                                             ascending=False).plot(kind='barh')

# COMMAND ----------

#Quais partidos têm mais dinheiro para gastar nas campanhas
candidatos.groupby('SG_PARTIDO')['VR_DESPESA_MAX_CAMPANHA'].sum().sort_values(ascending=False)

# COMMAND ----------

candidatos.groupby('SG_PARTIDO')['VR_DESPESA_MAX_CAMPANHA'].sum().sort_values(ascending=True).plot(kind='barh')

# COMMAND ----------

#Quais partidos têm os candidatos mais ricos
candidatos.groupby('SG_PARTIDO')['VR_BEM_CANDIDATO'].sum().sort_values(ascending=True).plot(kind='barh')

# COMMAND ----------

candidatos[candidatos['VR_DESPESA_MAX_CAMPANHA'] == candidatos['VR_DESPESA_MAX_CAMPANHA'].max()][['DS_GENERO', 'DS_COR_RACA', 'NM_CANDIDATO']]

# COMMAND ----------

candidatos[candidatos['VR_BEM_CANDIDATO'] == candidatos['VR_BEM_CANDIDATO'].max()][['DS_GENERO', 'DS_COR_RACA', 'NM_CANDIDATO']]

# COMMAND ----------

candidatos[['NM_URNA_CANDIDATO', 'DS_GENERO', 'DS_COR_RACA','DS_OCUPACAO', 'VR_DESPESA_MAX_CAMPANHA','VR_BEM_CANDIDATO']]                                

# COMMAND ----------

instrucao = candidatos[['DS_GRAU_INSTRUCAO']].value_counts()
instrucao                

# COMMAND ----------

instrucao.plot(kind='barh')

# COMMAND ----------


