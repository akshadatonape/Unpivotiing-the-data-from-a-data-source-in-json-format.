import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
# SparkSession setup
spark = (SparkSession
            .builder
            .appName("abc")
            .getOrCreate())

spark = SparkSession.builder.appName('abc').getOrCreate()
df = spark.read.json('s3://aws-poc-serverless-analytics/data/run-1617909458268-part-r-00000')
df.createOrReplaceTempView("row_ex")

columnsdf = spark.sql("select categories.* ,  country ,  createdAt ,  gopayId ,  gsi1SortKey ,  id ,  includedInPrice ,  name ,  state ,  type ,  updatedAt  from row_ex")

df1 = columnsdf.select([col(column).cast('string') for column in columnsdf.columns])
df_spark = df1.select([F.col(col).alias(col.replace('-', '_')) for col in df.columns])


subset=df.select("categories.*")
df2=subset.select([col(column).cast('string') for column in subset.columns])
df_subset = df2.select([F.col(col).alias(col.replace('-', '_')) for col in df2.columns])

cols_to_stack = ", ".join(['\'{c}\', {c}'.format(c=c) for c in df_subset.columns]) 

stack_expression = "stack({}, {}) as (keys, values)".format(n, cols_to_stack)

new_df=df.drop("categories")
sql = "select {}, {} from features_to_check".format(",".join(new_df.columns),stack_expression)
print ("Stacking sql:", sql)
df_spark.createOrReplaceTempView("features_to_check")

final=spark.sql(sql)

filtered=final.filter("values is not null")
filtered.select("values").show()

spark.stop()

