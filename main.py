from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,ArrayType,BooleanType,DateType


spark = SparkSession.builder.appName("my-spark-proj").getOrCreate()

title_scheme = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", ArrayType(StringType()), True),
    StructField("attributes", ArrayType(StringType()), True),
    StructField("isOriginalTitle", BooleanType(), True),
])

title_basics_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", DateType(), True),
    StructField("endYear", DateType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", ArrayType(StringType()), True)
])

crew_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", ArrayType(), True),
    StructField("writers", ArrayType(), True)
])

episode_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("eposideNumber", IntegerType(), True)
])

principals_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True),
])

ratings_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructType("ratings", IntegerType(), True),
    StructType("numVotes", IntegerType(), True)
])

name_basics_scheme = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", DateType(), True),
    StructField("deathYear", DateType(), True),
    StructField("primaryProfession", ArrayType(), True),
    StructField("knownForTitles", ArrayType(), True)
])


