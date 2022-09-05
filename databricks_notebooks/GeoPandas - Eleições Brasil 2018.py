# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://colab.research.google.com/github/Rosangelafl/Eleicoes2018_geopandas/blob/master/Aula_4_LabHackes_geopandas.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

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
# MAGIC * https://github.com/Rosangelafl/Eleicoes_Brasil/blob/master/Aula_4_LabHackes_geopandas.ipynb

# COMMAND ----------

#!pip install geopandas

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt
# MAGIC import geopandas as gpd

# COMMAND ----------

df_mapa = gpd.read_file('/content/drive/My Drive/Curso LabHacker/shapefile_ibge _aula4/BRMUE250GC_SIR.shp')

# COMMAND ----------

df_mapa.head()

# COMMAND ----------

df_mapa.info()

# COMMAND ----------

#plotando o mapa
df_mapa.plot()

# COMMAND ----------

#expandir o desenho do mapa
fig, ax = plt.subplots(1, figsize=(12,12))

#desenhar o mapa
df_mapa.plot(ax=ax)

#tirar os eixos
ax.set_axis_off()
plt.show()


# COMMAND ----------

df_votacao = pd.read_csv('/content/drive/My Drive/Curso LabHacker/shapefile_ibge _aula4/votacao_candidato_munzona_2018_BRASIL.csv', sep=';', encoding='Latin-1')
df_votacao.head()

# COMMAND ----------

df_votacao.columns


# COMMAND ----------

df_votacao.dtypes


# COMMAND ----------

df_votacao['CD_MUNICIPIO'] = df_votacao.CD_MUNICIPIO.astype(object)

# COMMAND ----------

df_votacao = df_votacao[df_votacao['DS_CARGO'] == 'Presidente']

# COMMAND ----------

#separar os dados ficando apenas com os dados de segundo turno
df_votacao = df_votacao[df_votacao['NR_TURNO'] == 2]
df_votacao.sample()


# COMMAND ----------

#Verificando a qtd de municípios com nunique()
df_votacao['CD_MUNICIPIO'].nunique()

# COMMAND ----------

#verificar cidades fora do Brasil
df_votacao[df_votacao['SG_UF'] == 'ZZ'].sample(2).T


# COMMAND ----------

df_votacao[df_votacao['SG_UF'] == 'ZZ']['CD_MUNICIPIO'].nunique()

# COMMAND ----------


df_votacao = df_votacao[df_votacao['SG_UF'] != 'ZZ']

# COMMAND ----------

df_votacao['CD_MUNICIPIO'].nunique()

# COMMAND ----------

#Quantas zonas eleitorais tem em São Paulo
#df_votacao[df_votacao['NM_MUNICIPIO'] == 'SÃO PAULO'].head(2).T

# COMMAND ----------

df_votacao = df_votacao[['SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_CANDIDATO', 'NM_URNA_CANDIDATO', 'QT_VOTOS_NOMINAIS']]
df_votacao.head()

# COMMAND ----------

#Agregar por municipios e candidatos
df_votacao.groupby(['SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_CANDIDATO', 'NM_URNA_CANDIDATO']).sum() 

# COMMAND ----------

df_votacao.groupby(['SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_CANDIDATO', 'NM_URNA_CANDIDATO']).sum().reset_index()


# COMMAND ----------

#ordenar o df pela quantidade votos nominais
df_votacao.sort_values(by='QT_VOTOS_NOMINAIS', ascending=False).head(10)

# COMMAND ----------

df_votacao.sort_values(by='QT_VOTOS_NOMINAIS', ascending=False,inplace=True)

# COMMAND ----------

# dropar as linhas duplicados, mas mantendo a primeira
df_votacao.drop_duplicates(subset='CD_MUNICIPIO', keep='first', inplace=True)
df_votacao.head(10)

# COMMAND ----------

df_votacao['RESULTADO'] = df_votacao['NR_CANDIDATO'].apply(lambda x: x== 17)

# COMMAND ----------

df_votacao.head(10)

# COMMAND ----------

#recuperar a tabela de correspondência entre código do ibge do mapa e código TSE do df


url = 'https://raw.githubusercontent.com/estadao/como-votou-sua-vizinhanca/master/data/votos/correspondencia-tse-ibge.csv'
df_equivalencia = pd.read_csv(url)
df_equivalencia.head()

# COMMAND ----------

#Vamos colocar o cód do ibge no df_votacao, usando merge
#preparando o dataframe
#vamos reindexar o df_votacao pelo código tse

df_vot_novo = df_votacao.copy()
df_vot_novo.set_index('CD_MUNICIPIO', drop=False, inplace=True)
df_vot_novo.head()

# COMMAND ----------

#reindexar o df_equivalência pelo código ibge
df_equi_novo = df_equivalencia.copy()

df_equi_novo.set_index('COD_TSE', drop=False, inplace=True)
df_equi_novo.head()


# COMMAND ----------

df_vot_novo.info()

# COMMAND ----------

df_equi_novo.info()

# COMMAND ----------

#criando um novo df com base no merge dos dois dfs anteriores, votação e equivalência
df_vot_equi = df_vot_novo.join(df_equi_novo)
df_vot_equi.head()

# COMMAND ----------

#vamos juntar o df_mapa com o df_equi_novo

df_vot_equi.info()

# COMMAND ----------

df_mapa.info()

# COMMAND ----------

# Transformar a coluna de código de IBGE do df de votação/equivalência
df_vot_equi['GEOCOD_IBGE'] = df_vot_equi['GEOCOD_IBGE'].astype(str)

df_vot_equi.info()


# COMMAND ----------

#unificando df_mapa  com o df Votação/equivalência, primeiro vamos reindexar o mapa
df_mapa.set_index('CD_GEOCMU', drop=False, inplace=True)


# COMMAND ----------

df_vot_equi.set_index('GEOCOD_IBGE', drop=False, inplace=True)

# COMMAND ----------

#vamos juntar os df
df_mapa_novo = df_mapa.join(df_vot_equi)

df_mapa_novo.head(3)

# COMMAND ----------

df_mapa_novo.info()

# COMMAND ----------

df_mapa_novo[df_mapa_novo['NR_CANDIDATO'].isnull()]

# COMMAND ----------

df_mapa_novo.drop(columns=['AJUSTE'], inplace=True)

# COMMAND ----------

#vamos eliminar info das duas lagoas do RS com dropna()
df_mapa_novo.dropna(inplace=True)



# COMMAND ----------

from matplotlib.colors import ListedColormap
cmap = ListedColormap(['red', 'green'])

#desenhando o mapa

fig, ax = plt.subplots(1, figsize=(12,12))
ax = df_mapa_novo.plot(column='RESULTADO', cmap=cmap, legend=True, linewidth=0.1, ax=ax, edgecolor='grey')

#escodendo os eixos

ax.set_axis_off()

#colocando título

ax.set_title('Resultado Eleição presidencial', fontdict={'fontsize':'25', 'fontweight': '3'})

#criando uma nota de rodape
ax.annotate('Fonte: IBGE, 2015; TSE, 2019; Estadão, 2019', xy=(0.1, 0.08), xycoords='figure fraction', horizontalalignment='left', verticalalignment='top',fontsize=10,color='#555555')

#alterando a legenda
leg= ax.get_legend()
leg.get_texts()[0].set_text('Fernando Haddad')
leg.get_texts()[1].set_text('Jair Bolsonaro')

plt.show()




