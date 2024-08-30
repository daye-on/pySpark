# Apache Spark
대용량의 데이터를 고속으로, 효율적으로 처리하는 빅데이터 분산처리 플랫폼. 하드디스크가 아닌 메모리의 캐시로 저장하는 인-메모리 컴퓨팅 상에서 데이터를 처리한다. 반복 처리가 필요한 작업에서 hadoop과 비교했을 때, 연산 속도가 10~10,000배 빠르다.
<br/>PySpark는 파이썬 API를 활용한 빅데이터 분산처리 플랫폼이다.
<br/><br/>
# References
* [PySpark 기본 예제](https://dslyh01.tistory.com/5)
* [PySpark 개념 및 주요 기능](https://heytech.tistory.com/304)
<br/><br/>
## Spark UDF
row마다 적용되어 새로운 column을 만들 수 있는 사용자 정의 함수이다. RDBMS의 User Defined Functions와 유사하게 작동하며, scala에서는 udf()로 함수를 wrapping하여 사용할 수 있다.
<br/>[SparkSQL UDF example](https://mengu.tistory.com/48)
