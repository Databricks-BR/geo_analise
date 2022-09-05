# Databricks notebook source
# MAGIC %md
# MAGIC # GeoPandas & PySpark Start
# MAGIC 
# MAGIC This notebook demonstrates how to use GeoPandas on Databricks to work with spatial data in GeoPackage format.
# MAGIC It builds on the [official Databricks GeoPandas notebook](https://databricks.com/notebooks/geopandas-notebook.html) but adds GeoPackage handling and explicit GeoDataFrame to Spark DataFrame conversions. 

# COMMAND ----------

# MAGIC %pip install geopandas
# MAGIC %pip install descartes

# COMMAND ----------

import urllib
import os
from matplotlib import pyplot as plt

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
import shapely.speedups
shapely.speedups.enable() # this makes some spatial queries run faster

# COMMAND ----------

# MAGIC %md
# MAGIC **Downloading GeoPackages from an online source**

# COMMAND ----------

def download_file(url):
  filename = url.split('/')[-1] 
  response = urllib.request.urlopen(url)
  content = response.read()
  with open(filename, 'wb' ) as f:
      f.write( content )  
  return filename

geolife = download_file('https://github.com/anitagraser/movingpandas/raw/master/tutorials/data/demodata_geolife.gpkg')
#grid = download_file('https://github.com/anitagraser/movingpandas/raw/master/tutorials/data/demodata_grid.gpkg')

assert(os.path.exists(geolife))
#assert(os.path.exists(grid))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading from GeoPackage**

# COMMAND ----------

geolife_gdf = gpd.read_file(geolife)
display(geolife_gdf.head())

# COMMAND ----------

plot = geolife_gdf.plot()
display(plot)

# COMMAND ----------

grid_gdf = gpd.read_file(grid)
display(grid_gdf)

# COMMAND ----------

display(grid_gdf.plot(column='id', legend=True))

# COMMAND ----------

# MAGIC %md
# MAGIC **Time to get into PySpark!**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType,DecimalType
from pyspark.sql.functions import lit, pandas_udf, PandasUDFType

# COMMAND ----------

# MAGIC %md
# MAGIC **From GeoDataframe to PySpark dataframe**

# COMMAND ----------

def geopandas_df_to_spark_df_for_points(gdf):
  gdf['lon'] = gdf['geometry'].x
  gdf['lat'] = gdf['geometry'].y
  sdf = spark.createDataFrame(pd.DataFrame(gdf).drop(['geometry'], axis=1))
  return sdf

# COMMAND ----------

sdf = geopandas_df_to_spark_df_for_points(geolife_gdf)
display(sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC **A test UDF**

# COMMAND ----------

def hello_world():
  return 'Hello world!'

hello_world_udf = udf(hello_world, StringType())

display(sdf.withColumn("test", hello_world_udf()))

# COMMAND ----------

# MAGIC %md
# MAGIC **A spatial UDF**

# COMMAND ----------

sc.broadcast(grid_gdf)

# COMMAND ----------

def find_intersection(longitude, latitude): 
  mgdf = grid_gdf.apply(lambda x: x['id'] if x['geometry'].intersects(Point(longitude, latitude)) else None, axis=1)
  idx = mgdf.first_valid_index()
  first_valid_value = mgdf.loc[idx] if idx is not None else None
  return int(first_valid_value)

find_intersection_udf = udf(find_intersection, IntegerType())

# COMMAND ----------

joined = sdf.withColumn("cell_id", find_intersection_udf(col("lon"), col("lat")))
display(joined)

# COMMAND ----------

def spark_df_to_geopandas_df_for_points(sdf):
  df = sdf.toPandas()
  gdf = gpd.GeoDataFrame(
    df.drop(['lon', 'lat'], axis=1),
    crs={'init': 'epsg:4326'},
    geometry=[Point(xy) for xy in zip(df.lon, df.lat)])
  return gdf

# COMMAND ----------

result_gdf = spark_df_to_geopandas_df_for_points(joined)
plot = grid_gdf.plot(column='id', cmap='Greys')
plot = result_gdf.plot(ax=plot, column='cell_id', legend=True)
display(plot)
