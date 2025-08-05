# Databricks notebook source
base_pah = "/FileStore/rawdata"
silver_path= f"{base_pah}/silver"
gold_path= f"{base_pah}/gold"

# COMMAND ----------

#Lectura desde silver
df_gold = spark.read.format("delta").load(f"{silver_path}/valoracion_usuarios")
df_gold.display()

# COMMAND ----------

#Analisis de los pacientes por ciudad y nivel de riesgo

from pyspark.sql.functions import count
df_summary = df_gold.groupBy("nombre_ciudad","RIESGO") \
                    .agg(count("*").alias("total_pacientes")) \
                    .orderBy("nombre_ciudad","RIESGO")
df_summary.display()

# COMMAND ----------

df_summary.coalesce(1).write.mode("overwrite").format("delta").save(f"{gold_path}/resumen_riesgo_ciudad")
