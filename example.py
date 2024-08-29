from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import date
from pyspark.sql import Row

# Create Dataframe - a list of rows
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=['2024-08-28 09:00:00']),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=['2024-08-28 10:00:00']),
    Row(a=3, b=4., c='string3', d=date(2000, 3, 1), e=['2024-08-28 11:00:00'])
])

"""
# explicit schema
df = spark.createDataFrame([
	(1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'stirng2', date(2000, 2, 1), datetime(2000, 1, 1, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
    ], schema='a long, b double, c string, d date, e timestamp')

# a pandas Dataframe
pandas_df = pd.DataFrame({
	'a' : [1, 2, 3],
    'b' : [2., 3., 4.],
    'c' :  ['string1', 'string2', 'string3'],
    'd' : [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e' : [date(2000, 1, 1, 12, 0), date(2000, 1, 2, 12, 0), date(2000, 1, 3, 12, 0)]
})
df = spark.creatDataFrame(pandas_df)

# an RDD consisting of a list of tuples
rdd = spark.sparkContext.paralleize([
	(1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
    ])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd'. 'e'])
"""

# Viewing Data
df.show()
df.show(1, vertical=True)
df.printSchema()
df.select("a","b").describe().show()

"""
[Error]
TypeError: Casting to unit-less dtype 'datetime64' is not supported. Pass e.g. 'datetime64[ns]' instead.

[Solution]
As is : e=datatime(2000,1,1,12,0)
To be : e=['2024-08-28 09:00:00']
"""

# Selecting and Accessing data
df.select(df.a).show()
df.withColumn('upper_c', upper(df.c)).show()
df.filter(df.a == 1).show()

"""
# Applying a Function
pandas_udf 는 pyarrow==0.15.1 을 필요로 하는데,
ERROR: Could not build wheels for pyarrow, which is required to install pyproject.toml-based projects
"""

# Grouping data
df_grouping = spark.createDataFrame([
	['red', 'banana', 1, 10],
    ['blue', 'banana', 2, 20],
    ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40],
    ['red', 'carrot', 5, 50],
    ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70],
    ['red', 'grape', 8, 80]]
    , schema=['color','fruit','v1','v2']
)
df_grouping.show()
df_grouping.groupby('color').avg().show()

# Working with SQL
df.createOrReplaceTempView('tableA')
spark.sql("SELECT count(*) FROM tableA").show()

from pyspark.sql.functions import expr
df.select(expr('count(*)') > 0).show()