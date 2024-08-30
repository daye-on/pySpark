from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("dataframe").getOrCreate()

# data
stocks = [
    ('Google', 'GOOGL', 'USA', 2984, 'USD'),
    ('Netflix', 'NFLX', 'USA', 645, 'USD'),
    ('Amazon', 'AMZN', 'USA', 3518, 'USD'),
    ('Tesla', 'TSLA', 'USA', 1222, 'USD'),
    ('Tencent', '0700', 'Hong Kong', 483, 'HKD'),
    ('Toyota', '7203', 'Japan', 2006, 'JPY'),
    ('Samsung', '005930', 'Korea', 70600, 'KRW'),
    ('Kakao', '035720', 'Korea', 125000, 'KRW'),
]

# schema
stockSchema = ['name', 'ticker', 'country', 'price', 'currency']

# create dataframe
df = spark.createDataFrame(stocks, stockSchema)
df.createOrReplaceTempView("stocks")

# udf 등록
from pyspark.sql.types import LongType
def squared(n):
    return n * n

spark.udf.register("squared", squared, LongType())

# 가격을 제곱
spark.sql("SELECT name, squared(price) FROM stocks").show()

# 조건 추가
spark.sql("SELECT name, squared(price) FROM stocks WHERE squared(price) < 1000000").show()

# 동화를 한글로 변환
def currency_ko(n):
    if n == 'USD':
        return '달러'
    elif n == 'KRW':
        return '원'
    elif n == 'JPY':
        return '엔'
    else:
        return '위안'

spark.udf.register("currency_ko", currency_ko)
spark.sql("SELECT name, CONCAT(price, currency_ko(currency)) as price FROM stocks").show()