# Taller Big Data con Apache Spark + R desde Databricks cloud


### Inicio del entorno de trabajo

Antes de iniciar el trabajo con Spark y R, necesitamos cargar la bilioteca de SparkR

```
# Añadimos la bibioteca
library(SparkR)
```

### Inicio de la sesión con Spark y R

Este bloque no es necesario ya que Databricks ya lo habilita por defecto al iniciar el cluster. Sí es necesario cuando usas un cluster comn Spark genérico.
```
sparkR.session(appName="Primeros_Pasos", master = "local[*]", sparkConfig = list(spark.driver.memory = "1g"))
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

Un SparkDataFrame puede ser registrado como una vista temporal en SparkSQL y que permite ejecutar sentencias SQL sobre los datos. La funcionalidad de SQL permite a las aplicaciones y flujos de trabajo ejecutar consultas SQL de forma programatica, devolviendo el resultado también como SparkDataFrame.

Esto es importante, ya que todas las transformaciones a los conjuntos de datos que están en formato SparkDataFrames, siguen siendo SparkDataFrames, lo que hace que toda su manipulación corra por parte de Spark con todas las ventajas que eso tiene:

- Volumen masivo de datos
- Almacenamiento distribuido
- Resilencia

###  Operaciones sencillas con SparkR sobre SparkDataFrames

En estos ejemplos vamos a tratar de ver una parte muy muy simple sobre la manipulación de los datos en el formato que entiende SparkR.

```
# Contamos los elementos a partir de un filtro normal
count(filter(df_faithful,"eruptions>3.0"))

# Convertimos a vista temporal de datos en SparkSQL y le damos el nombre faithful a la 'tabla'
createOrReplaceTempView(df_faithful,"faithful")

# Usamos SparkSQL para hacer consultas a los datos.
eruptions_sql <- sql("SELECT eruptions FROM faithful WHERE eruptions >= 3.0")

# Contamos el resultado
count(eruptions_sql)

# Mostramos un resumen
head(eruptions_sql)
```

### Fuentes de datos para Big Data


Hoy en día el trabajo con BigData parece que siempre está asociado al ecosistema HADOOP. Hace unos años esto significaba que si también eras un buen programador en JAVA, trabajar en tal entorno, incluso un simple programa para hacer un WORDCOUNT, implicaba varias docenas de líneas de código.

Pero hace 6-7 años la cuestión ha cambiado gracias a Apache Spark con su API de estilo funcional. Está escrito en SCALA pero también puede ser utilizado desde Python, JAVA y como estais viendo por este Taller: también en R

Dentro de una sesión de Spark, las aplicaciones pueden crear SparkDataFrames desde variadas fuentes de datos, como por ejemplo: un fichero local (data.frame), desde HDFS (hdfs:///), desde tablas en HIVE o desde otras múltiples fuentes de datos (AmazonS3, etc).

Concretamente las principales fuentes u orígenes de datos desde las que cargar datos son los siguientes:
- Ficheros locales
- Ficheros en sistemas distribuidos de almacenamiento Hadoop HDFS
- Sistemas de almacenamiento de datos tipo HIVE
- Desde bases de datos relacionales a través de JDBC
- ...

Una cosa son las fuentes de datos y otras cosa son los tipo de fuentes de datos. El tipo de fuente de datos puede ser visto como el formato de los datos.

Los conjuntos de datos pueden están almacenados en diferentes formatos, los más utilizados para SparkR (y Spark):

- Ficheros planos y CSV
- Ficheros JSON
- Ficheros de tipo avro (row-based)
- Ficheros de tipo parquet (column-based)
- Ficheros de tipo orc (column-based)

### Trabajo con ficheros en formato CSV

Para la lectura de datos con SparkR usamos la función read.df( )

```
# Sólo indicamos un fichero concreto .... No hay problema Spark es muy listo ! ;)
df <- read.df("/root/TallerSparkR/datasets/csv/buy_costumers_amazon01.csv", "csv", header = "true", inferSchema = "true")
printSchema(df)
count(df)
head(df)
```

```
# Sólo indicamos un fichero concreto .... No hay problema Spark es muy listo ! ;)
df <- read.df("/root/TallerSparkR/datasets/csv/buy_costumers_amazon01.csv", "csv", header = "true", inferSchema = "true")
print("Estructura sin parsear:")
printSchema(df)

# Creamos un esquema para definir cual será la estructura del fichero a leer.
schema_amazon <- structType(structField("id", "integer"),
                     structField("first_name", "string"),
                     structField("last_name", "string"),
                     structField("buy_hours", "string"),
                     structField("amount", "double"),
                     structField("credit_card", "string"))

df <- read.df("/root/TallerSparkR/datasets/csv/buy_costumers_amazon01.csv", "csv", header = "true", schema=schema_amazon)
print("Estructura parseada:")
printSchema(df)
head(df)

```



Si queremos leer todos los ficheros de un directorio sin entrar en los subdirectorios:

```
# Esto leería todos los ficheros de la carpeta pero no entraría a cada subdirectorio... Spark no eres muy listo !
df <- read.df("/ruta_de_los_datos/", "csv", header = "true", inferSchema = "true", schema=schema_amazon)
count(df)
```

### Escritura de datos o resultados

Una vez que hemos realizado transformaciones con los datos del SparkDataFrame, podemos dejarlo en memoria o bien pasarlo a DISCO (local) o HDFS (distribuido).

La API de fuentes de datos puede también ser usada para guardar y almacenar SparkDataFrames en múltiples formatos. Por ejemplo podemos almacenar el SparkDataDrame desde/hacia otros formatos como CSV, PARQUET usando la función write.df.

Esto da mucha versatilidad, ya que independiente del tipo de fuente, podemos almacenarlo y leerlo desde cualquiera otra fuente. Como no podía ser de otra forma.

```
# Escritura desde CSV a CSV:
write.df(df_full, path = "resultado_df_full.csv", source = "csv", mode = "overwrite")

# Escritura desde CSV a PARQUET
write.df(df_full, path = "resultado_df_full.parquet", source = "parquet", mode = "overwrite")
```
En mode podemos usar 'append', 'overwrite', 'error', 'ignore'.

### Trabajo con datos en formato PARQUE -> Igual que CSV u otros ficheros

Parquet es un formato de almacenamiento en columnas disponible para cualquier proyecto dentro del ecosistema de Hadoop, enfocado en la mejora del procesamiento de datos, modelado de datos y programación.

Parquet está diseñado para soportar esquemas de compresión y codificación muy eficientes. Múltiples proyectos han demostrado el impacto en el rendimiento de aplicar el correcto sistema de compresión y codificación a los datos. Parquet permite que los esquemas de compresión se especifiquen a nivel de columna.

Es un formato bien estructurado para ser usado para problemas de BigData.

La estructura del fichero se segmenta en N columnas partidas en M grupos de filas:


```
# Leemos un dataset que contiene los datos en formato Parquet
df_parquet <- read.df("/root/TallerSparkR/datasets/parquet/", "parquet")
```


```
# Vemos la estructura del fichero y sus atributos
printSchema(df_parquet)

```

 Vemos un resumen de los datos del fichero ...

```
head(df_parquet)
```

```
# Hacemos un pequeño cambio en el nombre de las columnas del SparkDataFrame.
colnames(df_parquet) <- c("user_id","cat","R1","R2","R3")
```

Vemos de nuevo el cambio de las columnas:

```
head(df_parquet)
# Contamos los registros del dataset ... es pequeño, no es BigData...
count(df_parquet)
```

Aplicamos unas transformaciones sencillas al SparkDataFrame, copiando la tabla en una Vista Temporal para poder trabajar con ella en SQL.

```
# Creamos una vista SparkDataFrame con el nombre "tmp_parquet".
# Este nombre tmp_parquet es el nombre que se usará ahora.
createOrReplaceTempView(df_parquet,"tmp_parquet")
```


Una vista temporal, permite trabajar con una copia temporal de los datos.

Contamos el número de registros:


```
# Usamos SparkSQL para hacer consultas a los datos.
count_rows <- sql("SELECT user_id,count(user_id) as registers FROM tmp_parquet group by user_id")
# Cuidado como obtener las cosas en SparkR: ---> Nooooooooo !!!! ;)
# print(collect(count_rows))
```

Compara el tiempo la opción anterior y la siguiente:

```
head(count_rows)
```



Si usamos una vista temporal, está estará disponible durante toda la sesión a no ser que se elimine la vista temporal con unpersist(....)

Probamos con otro ejemplo, para saber las categorías que hay:

```
# createOrReplaceTempView(df_parquet,"tmp_parquet") --> No volvermos a cargarla!
# Usamos SparkSQL para hacer consultas a los datos.
categories <- sql("SELECT cat FROM tmp_parquet group by cat")
head(categories)
```

#### Ejercicio básico

¿Cómo se calcularía el número de elementos de cada categoría?

```
# createOrReplaceTempView(df_parquet,"tmp_parquet")   --> No volvemos a cargarla !
# Usamos SparkSQL para hacer consultas a los datos.
categories_list <- sql("SELECT cat,count(user_id) as num_users FROM tmp_parquet group by cat")
```

```
head(categories_list)
```

¿Cuando usuarios distintos hay y que suma total tienen por usuario?

```
# createOrReplaceTempView(df_parquet,"tmp_parquet") --> No volvemos a cargarla!
# Usamos SparkSQL para hacer consultas a los datos.
table_summary <- sql("SELECT user_id,SUM(R1) as sum_index FROM tmp_parquet group by user_id")
```
```
count(table_summary)
head(table_summary)
```

```
unpersist(table_summary)
```


**Escritura de los datos** 

Al igual que con los otros formatos, se pueden exportar a cualquier otro.

```
# Escritura del fichero de formato parquet a formato parquet
write.df(finals, path = "resultsfinals.parquet", source = "parquet", mode = "overwrite")

# Escritura del fichero de formato csv a formato a CSV
write.df(finals, path = "resultsfinals.csv", source = "csv", mode = "overwrite")
```



