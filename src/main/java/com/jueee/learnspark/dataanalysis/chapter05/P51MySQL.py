from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext('local')
print('sc:',sc)

MYSQL_DRIVER = "com.mysql.jdbc.Driver";
MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/test";
MYSQL_CONNECTION_NAME = "root";
MYSQL_CONNECTION_PASSWORD = "jue";
MYSQL_CONNECTION_TABLE = "stat";

sqlContext = SQLContext(sc)
dataframe_mysql = sqlContext.read.format("jdbc").options(
    url=MYSQL_CONNECTION_URL,
    driver = MYSQL_DRIVER,
    dbtable = MYSQL_CONNECTION_TABLE,
    user=MYSQL_CONNECTION_NAME,
    password=MYSQL_CONNECTION_PASSWORD
).load()
dataframe_mysql.show()