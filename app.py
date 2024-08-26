from pyspark.sql import SparkSession

# 스파크 세션 생성
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 예제
data = [('001','Smith','M',40,'DA',4000),
        ('002','Rose','M',35,'DA',3000),
        ('003','Williams','M',30,'DE',2500),
        ('004','Anne','F',30,'DE',3000),
        ('005','Mary','F',35,'BE',4000),
        ('006','James','M',30,'FE',3500)]

columns = ["cd","name","gender","age","div","salary"]
df = spark.createDataFrame(data = data, schema = columns)

df.printSchema()
df.show()

df.select('*').show()
df.createOrReplaceTempView('EMP_INFO')
df2 = spark.sql("SELECT * FROM EMP_INFO")
df2.show()

df.filter("gender == 'F'").show()
df2 = spark.sql("SELECT * FROM EMP_INFO WHERE GENDER == 'F'")
df2.show()

from pyspark.sql import functions as Fun
test = df.filter(
    (Fun.col("div") == "DA") &
    (Fun.col("salary") > 3500)
).count()
print(test)

df.groupby("div").count().sort("count", ascending=True).show()