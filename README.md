# Taller Big Data con Apache Spark + R desde Databricks cloud

Procesamiento de datos Big Data con Spark + R sobre DataBricks

Seminarios de Especialización “Empresa Digital” - Análisis de datos para la toma de decisiones
Universidad de Castilla-LaMancha - España

Taller impartido por: Manuel Jesús Parra Royón


**Contenido:**

- [Taller Big Data con Apache Spark + R desde Databricks cloud](#taller-big-data-con-apache-spark---r-desde-databricks-cloud)
    + [R + Spark + Big Data](#r---spark---big-data)
      - [Introducción](#introducci-n)
      - [R](#r)
      - [Entonces, ¿cuál es el problema? ¿Por qué no se usa para Datos Masivos?](#entonces---cu-l-es-el-problema---por-qu--no-se-usa-para-datos-masivos-)
      - [Spark](#spark)
      - [¿Por qué Spark? ¿Qué ventajas tiene?](#-por-qu--spark---qu--ventajas-tiene-)
      - [SparkR](#sparkr)
    + [Inicio del entorno de trabajo](#inicio-del-entorno-de-trabajo)
    + [Inicio de la sesión con Spark y R](#inicio-de-la-sesi-n-con-spark-y-r)
    + [Primera prueba de con datos simples](#primera-prueba-de-con-datos-simples)
    + [Operaciones sencillas con SparkR sobre SparkDataFrames](#operaciones-sencillas-con-sparkr-sobre-sparkdataframes)
    + [Fuentes de datos para Big Data](#fuentes-de-datos-para-big-data)
    + [Trabajo con ficheros en formato CSV](#trabajo-con-ficheros-en-formato-csv)
    + [Escritura de datos o resultados](#escritura-de-datos-o-resultados)
    + [Trabajo con datos en formato PARQUE -> Igual que CSV u otros ficheros](#trabajo-con-datos-en-formato-parque----igual-que-csv-u-otros-ficheros)
      - [Ejercicio básico](#ejercicio-b-sico)
    + [Operaciones con SparkDataFrames](#operaciones-con-sparkdataframes)
      - [Selección de instancias y columnas](#selecci-n-de-instancias-y-columnas)
      - [Uso de agrupación y agregación](#uso-de-agrupaci-n-y-agregaci-n)
      - [Operaciones con columnas](#operaciones-con-columnas)
      - [Añadir columnas](#a-adir-columnas)
      - [Operando con SparkSQL sobre conjuntos masivos de datos](#operando-con-sparksql-sobre-conjuntos-masivos-de-datos)
    + [Uso de pipes con magittr](#uso-de-pipes-con-magittr)
    + [Minería de datos](#miner-a-de-datos)
      - [Algoritmos](#algoritmos)
      - [Generalized Linear Model](#generalized-linear-model)
        * [K-MEANS](#k-means)
      - [Creación de Conjuntos de entrenamiento y prueba](#creaci-n-de-conjuntos-de-entrenamiento-y-prueba)
      - [Persistencia de los MODELOS generados](#persistencia-de-los-modelos-generados)

### R + Spark + Big Data

![alt text](https://camo.githubusercontent.com/c5bfd73908238855af39f3c8536dfcc6169db4c6/68747470733a2f2f73697465732e676f6f676c652e636f6d2f736974652f6d616e7570617272612f686f6d652f537061726b526c6f676f2e706e67)

#### Introducción

La tendencia más reciente en el análisis de grandes conjuntos de datos indica la necesidad de un análisis INTERACTIVO de grandes conjuntos de datos.

Debido a ello se han desarrollado herramientas academicas y comerciales para trabajar de este modo con los datos.

Está claro que cada vez más el uso de herramientas como R para el análisis de datos, ya que permiten una mejora sustancial y más avanzada para el estudio de los datos. R, como sabemos, es ya hoy en día bastante popular y provee de soporte para procesamiento de datos estructurados usando dataframes y lo mejor de todo es que incluye en CRAN un número muy elevado de paquetes para análisis estadístico y visualización.

- R es la herramienta de facto para el análisis de datos
- R soporta procesamiento de datos estrcuturados dataframes
- R tiene miles de paquetes para todo tipo de tareas: analíticas, visualización, minería de datos, ...

#### R

El proyecto R, o el lenguaje R es un lenguaje de programación, un entorno de desarrollo interactivo, y un conjunto de bibliotecas de computación estadistica.

R es un lenguaje interpretado y provee de soporte para ejecución condicional, bucles, etc. R también incluye caracteristicas para computación nmérica, con tipos de datos vectores, matrices, etc. Otra característica fundamental es el uso de dataframes: estructuras de datos en tablas donde cada columna está formada por elementos con su tipo de dato. Estos dataframes pueden ser manipulados para filtrado, agregación, etc.

#### Entonces, ¿cuál es el problema? ¿Por qué no se usa para Datos Masivos?

El análisis de datos usando R está limitado por la cantidad de memoria disponible en una sola máquina y además R trabaja en un sólo hilo/proceso (parallel), por lo que es poco práctico usar R para grandes conjuntos de datos.

#### Spark

Algunas de estas limitaciones se han abordado, a través de un mejor soporte de Entrada/Salida, la integración con Hadoop y el diseño de aplicaciones R distribuidas que se pueden integrar con los motores de Bases de Datos más extendidos

Por tanto, analizamos cómo podemos escalar los programas R al tiempo que facilitamos su uso y despliegue a través de varias cargas de trabajo.

Hay una serie de beneficios para el diseño de un frontend R que está integrado con Spark:

- Bibliotecas de soporte: Spark, contiene bilbiotecas para trabajar con fuentes de datos y SQL, Machine Learning Distrbuido, análisis de grafos, etc.
- Fuentes de datos: Desde bases de datos, HDFS, HBase, Cassandra, JSON, CSV, Parquet, ...
- Rendimiento: Planificación de tareas, generación de código, gestión de memoria, ...

Apache Spark es un motor de proposito general para procesamiento de conjuntos de datos masivos. El proyecto inicialmete introdujo el RDD, Resilient Distributed Datasets, una API para computación tolerante a fallos en un entorno de cluster.

Más recientemte muchas más APIs de alto nivel están siendo desarrolladas en Spark. Estás son las siguientes:

- Machine Learning Library (MLLib) una biblioteca para machine learning a gran escala.
- GraphX, una biblioteca para procesamiento de grafos a gran escala
- SparkSQL, una API para consultas analíticas en SQL

Las bibliotecas están integradas en el CORE de Spark, por lo que Spark, puede permitir flujos de trabajos complejos, donde consultas SQL pueden ser usadas para preprocesar posteriormente los datos y analizarlos de forma avanzada con la parte de Machine Learning

#### ¿Por qué Spark? ¿Qué ventajas tiene?

Desde el inicio de Hadoop hace más de 10 años, poco a poco han aparecido nuevos problemas que ya no se podían resolver usando el paradigma de MapReduce. Esto ha echo que hayan aparecido nuevos sistemas para afrontar estos problemas, apareciendo :

![alt text](https://camo.githubusercontent.com/0edf6ca76b4be5bc81bc3c7ba05b633ce9abe07a/68747470733a2f2f73697465732e676f6f676c652e636f6d2f736974652f6d616e7570617272612f686f6d652f666c6f72612e706e67)

Una característica importante es que muchas de las aplicaciones ya existentes se han hecho compatibles con Spark y que estén surgiendo nuevas enfocadas en trabajar con Spark en áreas especificas de procesamiento de datos masivos.

![alt text](https://camo.githubusercontent.com/ed13af052590448deb8f371fd38eed392bd34bed/68747470733a2f2f73697465732e676f6f676c652e636f6d2f736974652f6d616e7570617272612f686f6d652f6661756e612e706e67)


#### SparkR

SparkR está construido como un paquete en R, luego no es necesarios hacer cosas especiales para que funcione.

El componente principal de SparkR es un DDF, Distributed Data Frame, que permite procesamiento de datos estructurados utilizando la sintaxis familiar para los usuarios acostumbrados a R.

Para mejorar el rendimiento, sobre grandes conjuntos de datos, SparkR provee de "evaluación perezosa" para operaciones sobre dataframes y utiliza además un optimizador de consultas relacionales para optimiar la ejecución.

![alt text](https://camo.githubusercontent.com/0efc69b82a0a47197e9390910e55c4f41247812c/68747470733a2f2f73697465732e676f6f676c652e636f6d2f736974652f6d616e7570617272612f686f6d652f72737061726b5f617263682e706e67)



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
df <- read.df("dbfs:/FileStore/shared_uploads/TU_USUARIO/buy_costumers_amazon01.csv", "csv", header = "true", inferSchema = "true")
printSchema(df)
count(df)
head(df)
```

```
# Sólo indicamos un fichero concreto .... No hay problema Spark es muy listo ! ;)
df <- read.df("dbfs:/FileStore/shared_uploads/TU_USUARIO/buy_costumers_amazon01.csv", "csv", header = "true", inferSchema = "true")
print("Estructura sin parsear:")
printSchema(df)

# Creamos un esquema para definir cual será la estructura del fichero a leer.
schema_amazon <- structType(structField("id", "integer"),
                     structField("first_name", "string"),
                     structField("last_name", "string"),
                     structField("buy_hours", "string"),
                     structField("amount", "double"),
                     structField("credit_card", "string"))

df <- read.df("dbfs:/FileStore/shared_uploads/TU_USUARIO/buy_costumers_amazon01.csv", "csv", header = "true", schema=schema_amazon)
print("Estructura parseada:")
printSchema(df)
head(df)

```



Si queremos leer todos los ficheros de un directorio sin entrar en los subdirectorios:

```
# Esto leería todos los ficheros de la carpeta pero no entraría a cada subdirectorio... Spark no eres muy listo !
df <- read.df("dbfs:/FileStore/shared_uploads/TU_USUARIO/directorio/", "csv", header = "true", inferSchema = "true", schema=schema_amazon)
count(df)
```

### Escritura de datos o resultados

Una vez que hemos realizado transformaciones con los datos del SparkDataFrame, podemos dejarlo en memoria o bien pasarlo a DISCO (local) o HDFS (distribuido).

La API de fuentes de datos puede también ser usada para guardar y almacenar SparkDataFrames en múltiples formatos. Por ejemplo podemos almacenar el SparkDataDrame desde/hacia otros formatos como CSV, PARQUET usando la función write.df.

Esto da mucha versatilidad, ya que independiente del tipo de fuente, podemos almacenarlo y leerlo desde cualquiera otra fuente. Como no podía ser de otra forma.

```
# Escritura desde CSV a CSV:
write.df(df, path = "resultado_df_full.csv", source = "csv", mode = "overwrite")

# Escritura desde CSV a PARQUET
write.df(df, path = "resultado_df_full.parquet", source = "parquet", mode = "overwrite")
```
En mode podemos usar 'append', 'overwrite', 'error', 'ignore'.

### Trabajo con datos en formato PARQUE -> Igual que CSV u otros ficheros

Parquet es un formato de almacenamiento en columnas disponible para cualquier proyecto dentro del ecosistema de Hadoop, enfocado en la mejora del procesamiento de datos, modelado de datos y programación.

Parquet está diseñado para soportar esquemas de compresión y codificación muy eficientes. Múltiples proyectos han demostrado el impacto en el rendimiento de aplicar el correcto sistema de compresión y codificación a los datos. Parquet permite que los esquemas de compresión se especifiquen a nivel de columna.

Es un formato bien estructurado para ser usado para problemas de BigData.

La estructura del fichero se segmenta en N columnas partidas en M grupos de filas:


```
# Leemos un dataset que contiene los datos en formato Parquet
df_parquet <- read.df("dbfs:/FileStore/shared_uploads/TU_USUARIO/parquet/", "parquet")
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

### Operaciones con SparkDataFrames

Cargamos un conjunto de datos **masivo** desde el repositirio de datasets.

El dataset que vamos a usar para el procesamiento de dato masivos, corresponde con un conjunto de datos de los registros de viaje en TAXI, donde se capturan las fechas y horas de recogida y devolución de pasajeros, lugares de recogida y entrega (coordenadas), distancias de viaje, tarifas detalladas, tipos de tarifas, tipos de pago y conteos de pasajeros que van en el taxi.

El dataset tiene MUCHAS posibilidades de procesamiento y también extracción de conocimiento.

Estos conjuntos de datos adjuntos fueron recopilados y proporcionados por la Comisión de Taxisde Nueva York (TLC) http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Características del conjunto de datos original:

- El conjunto de datos NYCTaxiTrips en total tiene sobre 267GB, que pueden ser manejados sin problema por SparkR (en un cluster real, no sobre una máquina virtual sencilla).
- En total contiene 1100 millones de registros.
- Más información de como se gestionan 1100 millones de instancias en la siguiente web y se soluciona este problema problema real: http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/

Más datasets masivos de NYCTaxiTrips en: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Primero revisamos los distintos dataset que se han preparado en: 
- yellow_tripdata_2016-02_small2.csv
- yellow_tripdata_2016-02_small3.csv

```
# Cargamos una versión reducida de los datos en CSV
df_nyctrips <- read.df("dbfs:/FileStore/shared_uploads/manuparr.a@gmail.com/yellow_tripdata_2016-02_small3.csv", "csv", header = "true", inferSchema = "true")
```

Estudiamos de manera superficial el dataset

```
# Comprobamos los campos del dataset
printSchema(df_nyctrips)

# Comprobamos como son los datos:
head(df_nyctrips)

# Contamos el total del registros:
count(df_nyctrips)
```


#### Selección de instancias y columnas

Para la selección de columnas y filas, usamos select y filter.

Todas las operaciones se pueden combinar para producir un nuevo dataset o SparkDataFrame. Son equivalentes a usar SPARKSQL .

Estas operaciones son esenciales si queremos transformar el dataset en otra versión preprocesada del mismo.

```
# Seleccionamos sólo la columna longitud, por el id de la columna
# Por ID de columna 
head(select(df_nyctrips,df_nyctrips$pickup_longitude))
```

```
# Seleccionamos sólo la columna longitud, por el nombre de la columna.
# Por nombre de columna del dataset
head(select(df_nyctrips,"pickup_longitude"))
```



Para aplicar filtros de para las filas usamos filter que admite expresiones con operadores condicionales:

```< = > ! & | ...```

```
# Aplicamos un filtro para ver los viajes aquellos viajes de taxi de más de 10 millas.
head(filter(df_nyctrips, df_nyctrips$trip_distance > 10 & df_nyctrips$total_amount> 20 ))
```
```
# Aplicamos un filtro para ver los viajes aquellos viajes de taxi de más de 10 millas y el importe mayor de $ 20
head(filter(df_nyctrips, df_nyctrips$trip_distance > 10 & df_nyctrips$total_amount> 20 ))
```
```
# Aplicamos un filtro para ver el viaje más caro en Taxi que se ha hecho:
head( agg(df_nyctrips ,max = max(df_nyctrips$total_amount)))
```
```
# Aplicamos un filtro para ver el viaje menos caro en Taxi que se ha hecho:
head(agg(df_nyctrips, min = min(df_nyctrips$total_amount)))
```

#### Uso de agrupación y agregación

Los SparkDataFrames soportan funciones de agregado despues de agrupar.

Por ejemplo podemos:

```
# Agrupamos por Vendedor y mostramos el número de viajes.
head(summarize(groupBy(df_nyctrips, df_nyctrips$VendorID), count = n(df_nyctrips$VendorID)))
```
```
# Agrupamos por Vendedor y mostramos el número de viajes.
head(summarize(groupBy(df_nyctrips, df_nyctrips$VendorID), max = max(df_nyctrips$total_amount)))
```

```
# Agrupamos y ordenamos
numsum <- summarize(groupBy(df_nyctrips, df_nyctrips$VendorID), num = n(df_nyctrips$VendorID))
head(arrange(numsum,asc(numsum$num)))
```

```
# Agrupamos por numero de pasajeros y mostramos el numero de viajes
trips_passenger <- summarize(groupBy(df_nyctrips, df_nyctrips$passenger_count), count = n(df_nyctrips$passenger_count))
```

```
# Cuidado con el COLLECT !
trips_df <- head(collect(trips_passenger))
```

```
head(trips_df)
```

#### Operaciones con columnas

Otras operaciones muy familiares en R, corresponden con la manipulación o transformación de valores en los registros de un dataset. En este caso la manipulación es muy sencilla:

```
# Convertimos la columna de millas a kilómetros, igual que en R.
df_nyctrips$trip_distance <- df_nyctrips$trip_distance*1.6
```

```
head(df_nyctrips)
```

#### Añadir columnas

```
# Usamos mutate para añadir columnas que operan con elementos de las demás columnas.

# mutate(sql_nyc,  uniform = rand(10),  normal  = randn(27))

head(mutate(df_nyctrips,  uniform = rand(10),  normal  = randn(27)))
head(mutate(df_nyctrips,  uniform =df_nyctrips$total_amount*1.1355,  normal  = randn(27)))
```

```
# Otro modo de hacerlo es:
head(withColumn(df_nyctrips,"uniform",rand(20)))
```

#### Operando con SparkSQL sobre conjuntos masivos de datos



Todas las funciones de manejo de datos que se han usado con SparkR, pueden hacerse de una forma sencilla e intuitiva con SparkSQL

```
# sql_nyc es nuestro DataFrameSpark de SQL
createOrReplaceTempView(sql_nyc,"slqdf_filtered_nyc")

# Hacemos una consulta para extraer el viaje de mayor distancia de cada venderor.
results <- sql("select VendorID, MAX(trip_distance) from slqdf_filtered_nyc GROUP BY VendorID ")
```


```
# Vemos el resultado.
head(results)
```




Buscamos el total de kilómetros recorridos por cada vendedor:
```
results <- sql("select VendorID, SUM(trip_distance) from slqdf_filtered_nyc GROUP BY VendorID ")

# Vemos el resultado
head(results)
```



Calculamos el tiempo en segundos
```
results <- sql("select VendorID, SUM(trip_time) from slqdf_filtered_nyc GROUP BY VendorID ")

# Vemos los resultados
head(results)
```



Calculamos el tiempo en minutos
```
results <- sql("select VendorID, SUM(trip_time)/60.0 as min_trip from slqdf_filtered_nyc GROUP BY VendorID ")

# Vemos los resultados
head(results)
```



Buscamos la ganacia total cada vendedor:

```
results <- sql("select VendorID, SUM(total_amount)*1.10373 as Total_Amount_Euro from slqdf_filtered_nyc GROUP BY VendorID ")

# Vemos el resultado
head(results)
```



Calculamos la media y la desviación típica del tiempo de recorrido y ganancia por numero de personas:

```
results <- sql("select passenger_count, AVG(trip_time), AVG(total_amount) ,AVG(trip_distance)   
                from slqdf_filtered_nyc 
                GROUP BY passenger_count 
                order by passenger_count ASC ")
head(results)
```



Coeficiente de correlación


```
results <- sql("select corr(total_amount,trip_distance) as correlation_coef
                from 
                slqdf_filtered_nyc")
# Ver resultados
head(results)
```


```
results <- sql("select corr(total_amount,trip_time) as correlation_coef
                from 
                slqdf_filtered_nyc")
head(results
```

```
results <- sql("select corr(trip_time,trip_distance) as correlation_coef
                from 
                slqdf_filtered_nyc")
head(results)
```


### Uso de pipes con magittr



El paquete magrittr permite:

- mejorar el tiempo de desarrollo y
- mejorar enormemente la legibilidad y mantenibilidad del código.

Para usarlo hay que importar la biblioteca magrittr dentro del proyecto y apartir de ese momentos podemos utilizar el operador

```%>%``` para concaternar operaciones y poder trabajar con flujos de datos y pipelines.

Provee de un operador que sirve para hacer pipes con el cual se puede encauzar un valor hacia adelante dentro de una expresión o llamada a función.

Veamos todas las operaciones que hemos realizado sobre los datos y su equivalente con pipes.

```
# Hacemos una copia del SparkDataFrame para usarla en una vista temporal en SQL
createOrReplaceTempView(df_nyctrips,"slqdf_filtered_nyc")

# Hacemos una selección de los registros, donde calculamos el tiempo del viaje de cada viaje
sql_nyc <- sql("select VendorID,INT(unix_timestamp(tpep_dropoff_datetime)- unix_timestamp(tpep_pickup_datetime)) AS trip_time,passenger_count,trip_distance,total_amount from slqdf_filtered_nyc")

head(sql_nyc)
```

```
# Usamos magrittr
library(magrittr)

# results <- sql("select VendorID, MAX(trip_distance) from slqdf_filtered_nyc GROUP BY VendorID ")
#summarize(groupBy(df_nyctrips, df_nyctrips$passenger_count), count = n(df_nyctrips$passenger_count))

df_nyctrips %>% 
        groupBy( df_nyctrips$passenger_count) %>%
        summarize(count = n(df_nyctrips$passenger_count)) %>%
        head()
```

```
df_nyctrips %>% 
        groupBy( df_nyctrips$passenger_count) %>%
        summarize( avg_total_amount=avg(df_nyctrips$total_amount) ,avg_trip_distance=avg(df_nyctrips$trip_distance)) %>%
        head()
```

```
df_nyctrips %>% 
         groupBy( df_nyctrips$passenger_count) %>%
        summarize(min = min(df_nyctrips$trip_distance),max = max(df_nyctrips$trip_distance)) %>%
        head()
```

```
df_nyctrips %>% 
         groupBy( df_nyctrips$passenger_count, hour(df_nyctrips$tpep_pickup_datetime)) %>%
        summarize(total_pickup = n(df_nyctrips$tpep_pickup_datetime)) %>%
        head()
```

```
count(sql_nyc)
num_regs <- as.integer(count(sql_nyc))

# Mostramos el número de registros
print(num_regs)
```

### Minería de datos 

La biblioteca de SparkR actualmente soporta los siguientes algoritmos de aprendizaje automático :
- modelo lineal generalizado,
- modelo de regresión de supervivencia con tiempo de fallo acelerado (AFT),
- modelo Bayes Naive y
- modelo KMeans.

SparkR utiliza MLlib para entrenar el modelo. Por tanto se puede analizar el resumen del modelo ajustado, predecir y hacer predicciones sobre nuevos datos y escribir/leer el modelo para guardar / cargar los modelos ajustados.

Además de ello, al igual que ocurre cuando usamos cualquier funcion en R, SparkR soporta el uso de fómulas, lo cual mejora bastante la adopción de SparkR para análisis de datos másivos. SparkR soporta un subconjunto de los operadores de fórmula R disponibles para el ajuste del modelo, incluyendo '~', '.', ':', '+' y '-'.

Dado que parte de SparkR está modelado en el paquete dplyr, ciertas funciones de SparkR comparten los mismos nombres con los de dplyr. Dependiendo del orden de carga de los dos paquetes, algunas funciones del paquete cargado primero son enmascaradas por las del paquete cargado después.

```
cov in package:stats
filter in package:stats
sample in package:base
```

Por tanto hay siempre que usar el paquete que queramos usar al final de la importación de las bibliotecas para que se haga efectiva la función que queremos para SparkR.

#### Algoritmos

El paquete SparkR soporta las siguientes funcionalidades de Machine Learning y Data mining


#### Generalized Linear Model

Usamos la función de R

```
gaussianDF <- iris
gaussianTestDF <- iris
gaussianGLM <- glm(data = gaussianDF, Sepal.Length ~ Sepal.Width + Species, family = "gaussian")

summary(gaussianGLM)
```



Usamos la función para glm de SparkR


```
irisDF <- suppressWarnings(createDataFrame(iris))
# Fit a generalized linear model of family "gaussian" with spark.glm
gaussianDF <- irisDF
gaussianTestDF <- irisDF
gaussianGLM <- spark.glm(gaussianDF, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")

# Model summary
summary(gaussianGLM)
```

**¿Qué diferencias hay entre ambos?**

Calculamos el modelo

```
# Calculamos la predicción
gaussianPredictions <- predict(gaussianGLM, gaussianTestDF)
# Mostramos las predicciones
showDF(gaussianPredictions)

# Usamos la función de R de glm con la familia gaussian
gaussianGLM2 <- glm(Sepal_Length ~ Sepal_Width + Species, gaussianDF, family = "gaussian")
summary(gaussianGLM2)

# Ahora usamos la función de glm de spark para la familia binomial.
binomialDF <- filter(irisDF, irisDF$Species != "setosa")
binomialTestDF <- binomialDF
binomialGLM <- spark.glm(binomialDF, Species ~ Sepal_Length + Sepal_Width, family = "binomial")

# Imprimimos el modelo
summary(binomialGLM)

# Obtenemos la predicción
binomialPredictions <- predict(binomialGLM, binomialTestDF)
showDF(binomialPredictions)
```

##### K-MEANS


```
library(ggplot2)
ggplot(iris, aes(Petal.Length, Petal.Width, color = Species)) + geom_point()

set.seed(20)
irisCluster <- kmeans(iris[, 3:4], 3, nstart = 20)
irisCluster
```

```
# Ajustamos un modelo k-medias. 

irisDF <- suppressWarnings(createDataFrame(iris))
kmeansDF <- irisDF
kmeansTestDF <- irisDF
kmeansModel <- spark.kmeans(kmeansDF, ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width,
                            k = 3)

# Vemos el resumen
summary(kmeansModel)

# Vemos los resultados del ajuste
showDF(fitted(kmeansModel))

# y vemos la predicción
kmeansPredictions <- predict(kmeansModel, kmeansTestDF)
showDF(kmeansPredictions)

# Mostramos la información de los grupos.
table(kmeansModel$cluster, iris$Species)
```


#### Creación de Conjuntos de entrenamiento y prueba

Existen varias formas de hacer los conjuntos de prueba y test. Se pueden usar las funciones de muestreo (sample) que trabajan sobre los SparkDataFrames.

```
train_df <- sample(df_training, withReplacement=FALSE, fraction=0.85, seed=42)
test_df <- except(df_training, train_df)

count(train_df)
count(test_df)
```

#### Persistencia de los MODELOS generados

Si necesitamos almacenar el modelo que hemos ajustado podemo hacerlo mediante el uso de la funcion write.ml. Al igual que luego podemos recuperarlo con read.ml.

```
modelPath <- tempfile(pattern = "ml", fileext = ".tmp")
write.ml(gaussianGLM, modelPath)
gaussianGLM2 <- read.ml(modelPat
```



