from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.functions import col, to_date, when

from src.Conexion.conexion_spark import *
from src.Conexion.conexion_mysql import Conexion

# Conexión a con SPARK y MySQL

spark = get_spark_session("IBEX35")
df = cargar_csv(spark, "ibex35_close-2024.csv")
conexion_mysql = Conexion()
cursor = conexion_mysql.getCursor()

db_name = conexion_mysql._database
table_name = "ibex35_viejo"

cursor.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = ?
    AND table_name = ?
""", (db_name, table_name)) 

existe = cursor.fetchone()[0] > 0

if not existe:
    print(f"Creando tabla {table_name} en MySQL...")

    df.write.format("jdbc") \
        .option("url", f"jdbc:mysql://localhost:3306/{db_name}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ibex35_viejo") \
        .option("user", "root") \
        .option("password", "changeme") \
        .mode("overwrite") \
        .save()

    print(f"Tabla {table_name} creada en {db_name}.")
else:
    print(f"La tabla {table_name} ya existe, no se creó nada.")


cursor.close()
conexion_mysql.closeConnection()


# ============================================================================
# Ej1-a: 
# Carga los datos del archivo CSV en un DataFrame de PySpark, muestra el
# esquema para comprobar el tipo de cada columna y convierte la columna “Fecha”
# de tipo “string” a tipo “date” (y cualquier otra columna si fuera necesario).
# Finalmente, verifica que la conversión se haya realizado correctamente mostrando
# de nuevo el esquema y 6 filas del DataFrame.
# ============================================================================
df.printSchema()
df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
df.printSchema()
df.show(6)

# ============================================================================
# Ej1-b: 
# Elimina el sufijo .MC de los nombres de todas las columnas. 
# Este sufijo solo indica que las acciones cotizan en la Bolsa de Madrid (Mercado Continuo). 
# Muestra los 6 primeros valores del DataFrame.
# ============================================================================
new_columns = [col_name.replace(".MC", "") for col_name in df.columns]
df = df.toDF(*new_columns)
df.show(6)


# ============================================================================
# Ej2-a: 
# Elimina las filas o columnas duplicadas o aquellas que no aporten
# información. 
# Muestra CUÁNTAS filas se han eliminado. 
# ¿De cuántas empresas hay información disponible en el DataFrame resultante? Muéstralo.
# ============================================================================
antes = df.count()
df = df.dropDuplicates()
despues = df.count()
print(f"Filas eliminadas: {antes - despues}")

num_empresas = len(df.columns) - 1  # excluimos la columna Fecha
print(f"Número de empresas: {num_empresas}")
print("Empresas disponibles:")
print(df.columns[1:])

# ============================================================================
# Ej2-b: 
# Determina cuál es el periodo temporal que cubren los datos (fecha inicial y
# fecha final) y muestra de cuántos días hay información disponible. 
# Comenta si el resultado es coherente con lo esperado y comenta si consideras necesario 
# buscar datos adicionales para completar el periodo y por qué (2 líneas máximo).
# ============================================================================
df.select(min("Fecha")).show()
df.select(max("Fecha")).show()
df.select(countDistinct("Fecha")).show()


print("Es correcto, el periodo cubre desde el 2 de enero de 2024 hasta el 30 de diciembre de 2024, y da un total de 255 fechas distintas, lo cual es coherente si quitamos los días festivos.")

# ============================================================================
# Ej3: 
# Estudia los datos y modifica de acuerdo con lo siguiente:
# - Renombra la columna “Fecha” a “Dia”. 
# - Muestra 10 filas del DataFrame.
# - Calcula y muestra para cada empresa la media anual (Media anual), 
#   el valor máximo anual (Max anual) y el valor mínimo anual (Min anual) 
#   dentro del periodo de datos disponible.
# ============================================================================
df = df.withColumnRenamed("Fecha", "Dia")
df.show(10)

df = df.withColumn("Año", year(col("Dia")))

empresas = [c for c in df.columns if c not in ["Dia", "Año"]]

aggs = []
for c in empresas:
    aggs.append(mean(col(c)).alias(f"{c}_Media_anual"))
    aggs.append(max(col(c)).alias(f"{c}_Max_anual"))
    aggs.append(min(col(c)).alias(f"{c}_Min_anual"))

resumen = df.groupBy("Año").agg(*aggs)

resumen.show(truncate=False)

# ============================================================================
# Ej3b: En la bolsa de EE. UU. existen requisitos mínimos de precio para una acción.
# Si cierra por debajo de 1 USD durante 30 días consecutivos, la empresa
# recibe un "Deficiency Notice". Con el objetivo de hacer un seguimiento de
# este suceso, crea una nueva columna booleana llamada “Deficiency Notice
# UNI” que sea “True” cuando el precio de cierre de la acción de la empresa
# UNI.MC esté por debajo de 1 €, y “False” en caso contrario, evaluando esta
# condición para cada día de forma independiente. Muestra 100 filas del
# DataFrame.
# ============================================================================
df = df.withColumn(
    "Deficiency_Notice_UNI",
    when(col("UNI") < 1, True).otherwise(False)
)

df.show(100, truncate=False)

# ============================================================================
# Ej4: Calcula la “Variación Anual” ((diferencia / inicial) * 100) que ha sufrido cada
# empresa, de forma que las clasifique en “Bajada Fuerte”, “Bajada”, “Neutra”,
# “Subida” y “Subida Fuerte” en función del valor que tenían al comienzo del periodo
# y el que conservan al final (de todas las que puedas). Una variación fuerte es de
# igual o más de un 15% y una Neutra es sobre un 1%. Muestra el resultado.
# ============================================================================
from pyspark.sql import Window
from pyspark.sql.functions import first, last, col, when

w = Window.orderBy("Dia")
empresas = [c for c in df.columns if c not in ["Dia", "Año", "Deficiency_Notice_UNI"]]
resultados = []

for c in empresas:
    primer_valor = df.orderBy("Dia").select(first(col(c), ignorenulls=True)).first()[0]
    ultimo_valor = df.orderBy("Dia").select(last(col(c), ignorenulls=True)).first()[0]
    
    if primer_valor is not None and ultimo_valor is not None:
        variacion = ((ultimo_valor - primer_valor) / primer_valor) * 100
        
        if variacion <= -15:
            clasificacion = "Bajada Fuerte"
        elif variacion < -1:
            clasificacion = "Bajada"
        elif -1 <= variacion <= 1:
            clasificacion = "Neutra"
        elif variacion < 15:
            clasificacion = "Subida"
        else:
            clasificacion = "Subida Fuerte"
        
        resultados.append((c, primer_valor, ultimo_valor, variacion, clasificacion))

resumen_variacion = spark.createDataFrame(resultados, ["Empresa", "Valor_Inicial", "Valor_Final", "Variacion_%", "Clasificacion"])
resumen_variacion.orderBy("Variacion_%").show(truncate=False)

# ============================================================================
# Ej5: Con el objetivo de hacer un seguimiento el año que viene, vamos a calcular
# para cada empresa, cuál es su distribución en ese periodo. Calcula la distribución
# de precios de cada empresa en el periodo disponible y, para cada sesión, añade
# una columna llamada “NombreEmpresaCuartil” que indique si el valor de ese día
# se encuentra en el q1, q2, q3 o q4. Muestra la primera fila del dataframe y Muestra
# las columnas completas correspondientes a las empresas AENA y BBVA del
# dataframe.
# ============================================================================

for empresa in empresas:
    q1, q2, q3 = df.approxQuantile(empresa, [0.25, 0.5, 0.75], 0.01)
    df = df.withColumn(
        f"{empresa}_Cuartil",
        when(col(empresa) <= q1, "q1")
        .when((col(empresa) > q1) & (col(empresa) <= q2), "q2")
        .when((col(empresa) > q2) & (col(empresa) <= q3), "q3")
        .otherwise("q4")
    )
    
df.show(1, truncate=False)
df.select("Dia", "AENA", "AENA_Cuartil", "BBVA", "BBVA_Cuartil").show(truncate=False)

# ============================================================================
# Añadir la tabla final a MySQL
# ============================================================================
cursor = conexion_mysql.getCursor()

db_name = conexion_mysql._database
table_name = "ibex35_modificado"

cursor.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = ?
    AND table_name = ?
""", (db_name, table_name)) 

existe = cursor.fetchone()[0] > 0

if not existe:
    print(f"Creando tabla {table_name} en MySQL...")

    df.write.format("jdbc") \
        .option("url", f"jdbc:mysql://localhost:3306/{db_name}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "ibex35_modificado") \
        .option("user", "root") \
        .option("password", "changeme") \
        .mode("overwrite") \
        .save()

    print(f"Tabla {table_name} creada en {db_name}.")
else:
    print(f"La tabla {table_name} ya existe, no se creó nada.")


cursor.close()
conexion_mysql.closeConnection()