# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# COMMAND ----------

base_pah = "/FileStore/rawdata"
bronce_path= f"{base_pah}/bronze"

# COMMAND ----------

valoracion_schema = StructType([
    StructField('fecha', StringType(), True),
    StructField('cedula', StringType(), True),
    StructField('TA_SIS', IntegerType(), True),
    StructField('DIABETICO', StringType(), True),
    StructField('HTA', StringType(), True),
    StructField('TABACO', StringType(), True),
    StructField('HDL', IntegerType(), True),
    StructField('CT', IntegerType(), True),
    StructField('DM', StringType(), True)
])


# COMMAND ----------

ciudad_schema = StructType([
    StructField('CODIGO',StringType(),True),
    StructField('NOMBRE',StringType(),True)
])

# COMMAND ----------

paciente_schema = StructType([
    StructField('Tipo_de_identificación_del_usuario', StringType(), True),
    StructField('Numero_de_Identificación', StringType(), True),
    StructField('Primer_apellido_del_usuario', StringType(), True),
    StructField('Segundo_apellido_del_usuario', StringType(), True),
    StructField('Primer_nombre_del_usuario', StringType(), True),
    StructField('Segundo_nombre_del_usuario', StringType(), True),
    StructField('Fecha_de_Nacimiento', StringType(), True),
    StructField('Sexo', StringType(), True),
    StructField('Edad', IntegerType(), True),
    StructField('Peso', StringType(), True),
    StructField('ciudad', StringType(), True)
])


# COMMAND ----------

# Lectura CSV -> Bronze (Delta)

df_val = spark.read.schema(valoracion_schema) \
                   .option("header", True) \
                   .option("sep", ",") \
                   .csv(f"{base_pah}/valoracion_usuarios.csv")

# Ver las primeras filas
df_val.show(5)

# Ver el esquema cargado
df_val.printSchema()


# COMMAND ----------

df_pac = spark.read.schema(paciente_schema) \
                    .option("header",True) \
                    .csv(f"{base_pah}/paciente.csv",sep=",")

df_pac.show(5)

df_pac.printSchema()

# COMMAND ----------

df_ciu = spark.read.schema(ciudad_schema) \
                    .option("header",True) \
                    .csv(f"{base_pah}/ciudad.csv",sep=",")
df_ciu.show(5)

df_ciu.printSchema()

df_ciu.display()

# COMMAND ----------

# Transformacion de la data formato delta y almacenamineto

df_val.write.mode("overwrite").format("delta").save(f"{bronce_path}/valoracion_usuarios")
df_pac.write.mode("overwrite").format("delta").save(f"{bronce_path}/paciente")
df_ciu.write.mode("overwrite").format("delta").save(f"{bronce_path}/ciudad")