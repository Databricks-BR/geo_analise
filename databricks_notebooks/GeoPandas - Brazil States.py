# Databricks notebook source
# MAGIC %md
# MAGIC # MAPs to Brazil States
# MAGIC ### Developed by Manuel Robalinho
# MAGIC ### Set/2018
# MAGIC 
# MAGIC __referência__
# MAGIC * https://github.com/MRobalinho/GeoPandas_Brasil

# COMMAND ----------

# Map Sources:
# https://gadm.org/download_country_v3.html
# Using GeoPandas

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC 
# MAGIC import matplotlib.pyplot as plt
# MAGIC import geopandas as gpd

# COMMAND ----------

path = 'ML/GeoPandas/Brazil/'

# COMMAND ----------

# Tables Brasil and states
uf_br0 = gpd.read_file(path + 'shp/gadm36_BRA_0.shp')
uf_br1 = gpd.read_file(path + 'shp/gadm36_BRA_1.shp')

# COMMAND ----------

# Tables Brasil Distritos and municipios
uf_br2 = gpd.read_file(path + 'shp/gadm36_BRA_2.shp')
uf_br3 = gpd.read_file(path + 'shp/gadm36_BRA_3.shp')

# COMMAND ----------

# BRASIL
uf_br0

# COMMAND ----------

# Statues - UF  BRAZIL
uf_br1

# COMMAND ----------

# Municipios
uf_br2.head()

# COMMAND ----------

# Distritos
uf_br3.head()

# COMMAND ----------

uf_br1.plot(cmap='YlOrRd')

# COMMAND ----------

# List of states - UF
uf_br1x = uf_br1[['NAME_1', 'geometry']]
uf_br1x

# COMMAND ----------

# Transform POLYGON to POINT
# copy poly to new GeoDataFrame
points = uf_br1x.copy()
# change the geometry
points.geometry = points['geometry'].centroid
# same crs
#points.crs = poly.crs
points.head()

# COMMAND ----------

# PLOT State points
f, ax = plt.subplots(1, figsize=(8,10))
ax.set_axis_on()
f.suptitle('BRAZIL')
# Plot the states area
ax = uf_br1x.plot(ax=ax, facecolor='blue', alpha=1, linewidth=0, cmap='YlOrRd')

# Plot the labels 
for x, y, label in zip(points.geometry.x, points.geometry.y, points.NAME_1):
    ax.annotate(label, xy=(x, y), xytext=(3, 3), alpha=3, textcoords="offset points",color='blue')

# COMMAND ----------

# Plot Base Country ans states
ax.figsize=(8,10)
# Plot the states area
base = uf_br1x.plot( edgecolor='black', cmap='OrRd')
# Plot Base continent - Cities
ax = uf_br1x.plot(ax=base, facecolor='blue', alpha=1, linewidth=0,  cmap='OrRd')

# Plot Base continent - Cities
for x, y, label in zip(points.geometry.x, points.geometry.y, points.NAME_1):
    ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points",color='green')

# COMMAND ----------

uf_br2.head()

# COMMAND ----------

# CEARA - UF
uf_br1_ce = uf_br1[uf_br1.GID_1 == 'BRA.6_1']
uf_br1_ce

# COMMAND ----------

# MAP Of CEARA - Filter UF
# Statues - UF  BRAZIL
uf_br2_ce = uf_br2[uf_br2.GID_1 == 'BRA.6_1']

# COMMAND ----------

uf_br2_ce.head(10)

# COMMAND ----------

# List of Cities from UF
uf_br2x = uf_br2_ce[['NAME_2', 'geometry']]
uf_br2x.head()

# COMMAND ----------

# Transform POLYGON to POINT
# copy poly to new GeoDataFrame
points = uf_br2x.copy()
# change the geometry
points.geometry = points['geometry'].centroid

points.head()

# COMMAND ----------

# PLOT Municipios from UF
f, ax = plt.subplots(1, figsize=(12,14))
ax.set_axis_on()
f.suptitle('BRAZIL - CEARA')
# Plot the states area
ax = uf_br1_ce.plot(ax=ax, facecolor='blue', alpha=1, linewidth=0, cmap='YlOrRd')

# Plot the labels 
for x, y, label in zip(points.geometry.x, points.geometry.y, points.NAME_2):
    ax.annotate(label, xy=(x, y), xytext=(3, 3), alpha=3, textcoords="offset points",color='blue')

# COMMAND ----------

# PLOT Municipios from UF
f, ax = plt.subplots(1, figsize=(12,14))
ax.set_axis_on()
f.suptitle('BRAZIL - CEARA')
# Plot the states area
ax = uf_br2x.plot(ax=ax, facecolor='cy', alpha=0.5, linewidth=1)

# Plot the labels 
for x, y, label in zip(points.geometry.x, points.geometry.y, points.NAME_2):
    ax.annotate(label, xy=(x, y), xytext=(3, 3), alpha=3, textcoords="offset points",color='blue')

# COMMAND ----------

# PLOT Municipios from UF
f, ax = plt.subplots(1, figsize=(12,14))
ax.set_axis_on()
f.suptitle('BRAZIL - CEARA')
# Plot the states area
ax = uf_br2x.plot(ax=ax, facecolor='blue', alpha=0.2, linewidth=0, cmap='YlOrRd')

# Plot the labels 
for x, y, label in zip(points.geometry.x, points.geometry.y, points.NAME_2):
    ax.annotate(label, xy=(x, y), xytext=(3, 3), alpha=3, textcoords="offset points",color='blue')

# COMMAND ----------



# COMMAND ----------


