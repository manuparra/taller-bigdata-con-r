# Taller Big Data con Apache Spark + R desde Databricks cloud


Antes de iniciar el trabajo con Spark y R, necesitamos cargar la bilioteca de SparkR

```
# Añadimos la bibioteca
library(SparkR)
```


Con SparkR podemos crear un DataFrame de Spark desde un data.frame habitual usado en R.

Un DataFrame es una colección distribuida de datos organizada en columnas.

Los dataframes son conceptualmente equivalentes a bases de datos relacionales o a data.frames en R o Python, pero con una ventaja: son mucho más eficientes para el trabajo con grandes volumenes de datos. Los DataFramespueden ser creados desde una amplio surtido de fuentes muy diferentes. Es decir, casi cualquier cosa puede ser un dataframe, por ejemplo ficheros estructurados, tablas en HIVE, bases de datos externas o RDDs.

Los RDDs son la principal abstracción de datos en Spark. Un RDD es una colección resilente y distribuida de registros. Esta es una de las claves de Spark y es uno de los componentes fundamentales del core de Spark.
