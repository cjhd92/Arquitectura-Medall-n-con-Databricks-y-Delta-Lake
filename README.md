
# 🏥 Proyecto de Arquitectura Medallón con Databricks y Delta Lake

Este proyecto implementa la **arquitectura de medallón (Bronce–Plata–Oro)** en Databricks usando **PySpark, SQL y Delta Lake**.  
El objetivo es mostrar un flujo de trabajo de **Data Lakehouse** aplicable en entornos reales, combinando conceptos de **Data Lake** y **Data Warehouse**.

---

## 📌 Objetivos del Proyecto

- Ingerir datos crudos desde archivos CSV en un Data Lake.
- Limpiar y normalizar los datos para obtener una base confiable.
- Generar métricas de negocio y análisis agregados listos para su consumo en BI.
- Practicar el uso de Delta Lake con Databricks para garantizar transacciones ACID, versionado y eficiencia.

---

## 🏗️ Arquitectura de Medallón

### 🥉 Bronze Layer
- **Descripción**: Contiene los datos crudos tal como llegan de la fuente (CSV).
- **Acciones realizadas**:
  - Definición de esquemas con `StructType` y `StructField`.
  - Ingesta de datos con `spark.read.csv`.
  - Almacenamiento en **formato Delta** para eficiencia y confiabilidad.

🥈 Silver Layer
•	Descripción: Contiene datos limpios, estandarizados y transformados.
•	Acciones realizadas:
o	Creación de vistas temporales para ejecutar SQL.
o	Limpieza de nombres de columnas y normalización de fechas.
o	Generación de columnas derivadas (nombre_completo, puntuación de riesgo).
o	Conversión de datos categóricos a numéricos mediante expresiones SQL.

🥇 Gold Layer
•	Descripción: Contiene métricas de negocio y datos listos para consumo.
•	Acciones realizadas:
o	Cálculo de riesgo categorizado usando CASE WHEN.
o	Creación de un DataFrame con totales de pacientes por ciudad y nivel de riesgo.
o	Persistencia en formato Delta para uso en BI.
📊 Relación con Data Lake y Data Warehouse
•	Data Lake (Bronze): Almacena datos crudos de manera flexible y económica.
•	Silver (ODS / Staging): Funciona como una capa de Operational Data Store, limpiando y normalizando la información.
•	Data Warehouse (Gold): Produce métricas e indicadores listos para dashboards, modelos predictivos o reportes ejecutivos.

