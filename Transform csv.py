# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import udf,col
import pyspark.sql.functions as F


# COMMAND ----------

base_pah = "/FileStore/rawdata"
bronce_path= f"{base_pah}/bronze"
silver_path= f"{base_pah}/silver"

# COMMAND ----------

# Lectura desde Bronze 
df_val = spark.read.format("delta").load(f"{bronce_path}/valoracion_usuarios")
df_pac = spark.read.format("delta").load(f"{bronce_path}/paciente")
df_ciu = spark.read.format("delta").load(f"{bronce_path}/ciudad")


# COMMAND ----------

#creo vistas temporales (Usar SQL si se desea)
df_val.createOrReplaceTempView("valoracion_usuarios")
df_pac.createOrReplaceTempView("paciente")
df_ciu.createOrReplaceTempView("ciudad")

# COMMAND ----------

#SQL transformaciones

spark.sql("""
CREATE OR REPLACE TEMP VIEW VW_paciente AS
SELECT
    `Numero_de_Identificación` AS cedula,
    `Tipo_de_identificación_del_usuario` AS tipo_documento,
    CONCAT_WS(' ',
        Primer_nombre_del_usuario,
        Segundo_nombre_del_usuario,
        Primer_apellido_del_usuario,
        Segundo_apellido_del_usuario
    ) AS nombre_completo,
    Sexo AS sexo,
    Edad AS edad,
    Peso AS peso,
    ciudad
FROM paciente
""")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM VW_paciente
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW VW_valoracion_usuario AS
SELECT
    substr(fecha,1,10) AS fecha,
    cedula,
    TA_SIS,
    DIABETICO,
    HTA,
    TABACO,
    HDL,
    CT,
    DM
FROM valoracion_usuarios

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from valoracion_usuarios

# COMMAND ----------

spark.sql("""
create or replace temp view VW_valoracion_usuario_sabana AS 
    select
        vu.*,
        p.Tipo_documento,
        p.nombre_completo,
        p.sexo,
        p.edad,
        p.peso,
        c.NOMBRE AS nombre_ciudad
    from VW_valoracion_usuario vu
    left join VW_paciente p on trim(p.cedula) = trim(vu.cedula)
    left join ciudad c on p.ciudad = c.CODIGO

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from VW_valoracion_usuario_sabana

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     fecha,
# MAGIC     cedula,
# MAGIC     Tipo_documento,
# MAGIC     nombre_completo,
# MAGIC     sexo,
# MAGIC     edad,
# MAGIC     nombre_ciudad,
# MAGIC     CASE
# MAGIC         WHEN puntuacion < 5 THEN "BAJO"
# MAGIC         WHEN puntuacion >= 5 AND puntuacion < 10 THEN "MODERADO"
# MAGIC         WHEN puntuacion >= 10 THEN "ALTO"
# MAGIC         ELSE NULL
# MAGIC     END AS RIESGO
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT
# MAGIC         fecha,
# MAGIC         cedula,
# MAGIC         Tipo_documento,
# MAGIC         nombre_completo,
# MAGIC         sexo,
# MAGIC         edad,
# MAGIC         nombre_ciudad,
# MAGIC         ROUND(
# MAGIC             (100 * (1 - 0.88936 * EXP(cal_edad + cal_CT - cal_HDL + cal_TA + cal_TABACO + cal_DM - 23.9802)))
# MAGIC         ,2) AS puntuacion
# MAGIC     FROM
# MAGIC     (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             (LOG(edad) * 3.06117) AS cal_edad,
# MAGIC             (LOG(CT) * 1.12370) AS cal_CT,
# MAGIC             (LOG(HDL) * 0.93263) AS cal_HDL,
# MAGIC             (LOG(TA_SIS) * IF(HTA = 'SI', 1.99881, 1.93303)) AS cal_TA,
# MAGIC             (IF(TABACO = 'SI', 0.65451, 0)) AS cal_TABACO,
# MAGIC             (IF(DM = 'SI', 0.57367, 0)) AS cal_DM
# MAGIC         FROM VW_valoracion_usuario_sabana
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

query = """
SELECT
    fecha,
    cedula,
    Tipo_documento,
    nombre_completo,
    sexo,
    edad,
    nombre_ciudad,
    CASE
        WHEN puntuacion < 5 THEN "BAJO"
        WHEN puntuacion >= 5 AND puntuacion < 10 THEN "MODERADO"
        WHEN puntuacion >= 10 THEN "ALTO"
        ELSE NULL
    END AS RIESGO
FROM
(
    SELECT
        fecha,
        cedula,
        Tipo_documento,
        nombre_completo,
        sexo,
        edad,
        nombre_ciudad,
        ROUND(
            (100 * (1 - 0.88936 * EXP(cal_edad + cal_CT - cal_HDL + cal_TA + cal_TABACO + cal_DM - 23.9802)))
        ,2) AS puntuacion
    FROM
    (
        SELECT
            *,
            (LOG(edad) * 3.06117) AS cal_edad,
            (LOG(CT) * 1.12370) AS cal_CT,
            (LOG(HDL) * 0.93263) AS cal_HDL,
            (LOG(TA_SIS) * IF(HTA = 'SI', 1.99881, 1.93303)) AS cal_TA,
            (IF(TABACO = 'SI', 0.65451, 0)) AS cal_TABACO,
            (IF(DM = 'SI', 0.57367, 0)) AS cal_DM
        FROM VW_valoracion_usuario_sabana
    )
)
"""

df_silver = spark.sql(query)


# COMMAND ----------

df_silver.display()

# COMMAND ----------

#Guardar silver
df_silver.write.mode("overwrite").format('delta').save(f"{silver_path}/valoracion_usuarios")