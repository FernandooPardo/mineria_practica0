from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import *
from pyspark.sql.window import *

# Crear la SparkSession
spark_session = SparkSession.builder.appName("IBEX35").getOrCreate()

# Leer CSV en la misma carpeta
df = spark_session.read.option("header", True) \
                       .option("sep", ";") \
                       .option("inferSchema", True) \
                       .csv("ibex35_close-2024.csv")

# Ej1-a: Carga los datos del archivo CSV en un DataFrame de PySpark, muestra el
# esquema para comprobar el tipo de cada columna y convierte la columna “Fecha”
# de tipo “string” a tipo “date” (y cualquier otra columna si fuera necesario).
# Finalmente, verifica que la conversión se haya realizado correctamente mostrando
# de nuevo el esquema y 6 filas del DataFrame.

df.printSchema()
df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
df.printSchema()
df.show(6)

# Ej1-b: Elimina el sufijo .MC de los nombres de todas las columnas. Este sufijo solo
# indica que las acciones cotizan en la Bolsa de Madrid (Mercado Continuo). Muestra
# los 6 primeros valores del DataFrame.

new_columns= [col.replace(".MC", "") for col in df.columns]
df = df.toDF(*new_columns)
df.show(6)

# Ej1-c (opcional): Define un StructType que permita cargar los datos directamente
# con el tipo correcto para cada columna y sustituya los nombres de los tickers por el
# nombre completo/siglas de la empresa en el esquema. Muestra dicha estructura y
# las 6 primeras filas del DataFrame.

# Ej2-a: Elimina las filas o columnas duplicadas o aquellas que no aporten
# información. Muestra CUANTAS filas se han eliminado. ¿De cuántas empresas hay
# información disponible en el DataFrame resultante? Muéstralo.

antes = df.count()
df = df.dropDuplicates()
despues = df.count()
print(f"Filas eliminadas: {antes - despues}")

num_empresas = len(df.columns) - 1
print(f"Número de empresas: {num_empresas}")
print("Empresas disponibles:")
print(df.columns[1:])

# Ej2-b: Determina cuál es el periodo temporal que cubren los datos (fecha inicial y
# fecha final) y muestra de cuantos días hay información disponible. Comenta si el
# resultado es coherente con lo esperado y comenta si consideras necesario buscar
# datos adicionales para completar el periodo y porqué (2 líneas máximo).

df.select(min("Fecha")).show()
df.select(max("Fecha")).show()
df.select(countDistinct("Fecha")).show()

"""Es correcto, el periodo cubre desde el 2 de enero de 2024 hasta el 30 de diciembre de 2024, y da un total de 255 fechas distintas, 
lo cual es coherente si quitamos los días festivos."""

# Ej3: Estudia los datos y modifica de acuerdo con lo siguiente:
# Renombra la columna “Fecha”. a “Dia”. Muestra 10 filas del DataFrame.

df = df.withColumnRenamed("Fecha", "Dia")
df.show(10)

# Calcula y muestra para cada empresa la media anual (Media anual), el valor
# máximo anual (Max anual) y el valor mínimo anual (Min anual) dentro del
# periodo de datos disponible