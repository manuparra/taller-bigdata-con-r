# Taller Big Data con Apache Spark + R desde Databricks cloud


### Inicio del entorno de trabajo

Antes de iniciar el trabajo con Spark y R, necesitamos cargar la bilioteca de SparkR

```
# Añadimos la bibioteca
library(SparkR)
```

### Primera prueba de con datos simples

Con SparkR podemos crear un DataFrame de Spark desde un data.frame habitual usado en R.

Un DataFrame es una colección distribuida de datos organizada en columnas.

Los dataframes son conceptualmente equivalentes a bases de datos relacionales o a data.frames en R o Python, pero con una ventaja: son mucho más eficientes para el trabajo con grandes volumenes de datos. Los DataFramespueden ser creados desde una amplio surtido de fuentes muy diferentes. Es decir, casi cualquier cosa puede ser un dataframe, por ejemplo ficheros estructurados, tablas en HIVE, bases de datos externas o RDDs.

Los RDDs son la principal abstracción de datos en Spark. Un RDD es una colección resilente y distribuida de registros. Esta es una de las claves de Spark y es uno de los componentes fundamentales del core de Spark.

```
# Vamos a usar un dataset sencillo integrado en R
# El dataset contiene el tiempo de espera entre erupciones y duración 
# de la erupción de un geiser de Yellowstone
class(faithful)

# Convertimos un dataframe de R en un DataFrame de Spark, que llamaremos SparkDataFrame
df_faithful <- createDataFrame(faithful)

# Vemos el tipo de dataset nuevo
class(df_faithful)

# Visualizamos de forma rápida el contenido
head(df_faithful)

# Usamos la función printSchema de SparkR para 'deducir' el esquema de datos (la estructura)
printSchema(df_faithful)
```
