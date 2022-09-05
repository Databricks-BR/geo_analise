# Databricks notebook source
# MAGIC %md # [GeoPandas](http://geopandas.org/) Example
# MAGIC 
# MAGIC __Option-1: Using [DBUtils Library Import](https://docs.databricks.com/dev-tools/databricks-utils.html#library-utilities) within Notebook (see cell #2).__
# MAGIC 
# MAGIC __Option-2: Using [Databricks ML Runtime](https://docs.databricks.com/runtime/mlruntime.html#mlruntime) which includes Anaconda (not used).__
# MAGIC 
# MAGIC * [Install Cluster Libraries](https://docs.databricks.com/libraries.html#install-a-library-on-a-cluster):
# MAGIC  * geopandas PyPI Coordinates: `geopandas`
# MAGIC  * shapely PyPI Coordinates: `shapely`

# COMMAND ----------

dbutils.library.installPyPI("geopandas")

# COMMAND ----------

dbutils.library.installPyPI("shapely")

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely import wkb, wkt
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType,DecimalType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import shapely.speedups
shapely.speedups.enable() # this makes some spatial queries run faster

# COMMAND ----------

df_csv = pd.read_csv("/dbfs/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv")
df_csv['the_geom'] = df_csv['the_geom'].apply(wkt.loads)
gdf  = gpd.GeoDataFrame(df_csv, geometry='the_geom')
sc.broadcast(gdf)
def find_borough(latitude, longitude): 
    mgdf = gdf.apply(lambda x: x['borough'] if x['the_geom'].intersects(Point(longitude,latitude)) else None, axis=1)
    idx = mgdf.first_valid_index()
    first_valid_value = mgdf.loc[idx] if idx is not None else None
    return first_valid_value
find_borough_udf = udf(find_borough, StringType())

# COMMAND ----------

# test the function
find_borough( 40.69943618774414,-73.9920883178711)

# COMMAND ----------

df_raw = spark.read.format("delta").load("/ml/blogs/geospatial/delta/nyc-green")
df_raw_borough = df_raw.sample(False, 0.01).withColumn("pickup_borough", find_borough_udf(col("pickup_latitude"),col("pickup_longitude")))

# COMMAND ----------

display(df_raw_borough.select(["pickup_borough","pickup_datetime","pickup_latitude","pickup_longitude"]))
