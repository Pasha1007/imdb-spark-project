# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, DateType

# spark = SparkSession.builder.appName("my-spark-proj").getOrCreate()

# title_scheme = StructType([
#     StructField("titleId", StringType(), True),
#     StructField("ordering", IntegerType(), True),
#     StructField("title", StringType(), True),
#     StructField("region", StringType(), True),
#     StructField("language", StringType(), True),
#     StructField("types", ArrayType(StringType()), True),
#     StructField("attributes", ArrayType(StringType()), True),
#     StructField("isOriginalTitle", BooleanType(), True),
# ])

# title_basics_scheme = StructType([
#     StructField("tconst", StringType(), True),
#     StructField("titleType", StringType(), True),
#     StructField("primaryTitle", StringType(), True),
#     StructField("originalTitle", StringType(), True),
#     StructField("isAdult", BooleanType(), True),
#     StructField("startYear", IntegerType(), True),  # Change DateType() to IntegerType()
#     StructField("endYear", IntegerType(), True),    # Change DateType() to IntegerType()
#     StructField("runtimeMinutes", IntegerType(), True),
#     StructField("genres", ArrayType(StringType()), True)
# ])

# crew_scheme = StructType([
#     StructField("tconst", StringType(), True),
#     StructField("directors", StringType(), True),  # Змінити на StringType()
#     StructField("writers", StringType(), True)      # Змінити на StringType()
# ])


# episode_scheme = StructType([
#     StructField("tconst", StringType(), True),
#     StructField("parentTconst", StringType(), True),
#     StructField("seasonNumber", IntegerType(), True),
#     StructField("episodeNumber", IntegerType(), True)  # Fix the column name
# ])

# principals_scheme = StructType([
#     StructField("tconst", StringType(), True),
#     StructField("ordering", IntegerType(), True),
#     StructField("nconst", StringType(), True),
#     StructField("category", StringType(), True),
#     StructField("job", StringType(), True),
#     StructField("characters", StringType(), True),
# ])

# name_basics_scheme = StructType([
#     StructField("nconst", StringType(), True),
#     StructField("primaryName", StringType(), True),
#     StructField("birthYear", IntegerType(), True),  # Change DateType() to IntegerType()
#     StructField("deathYear", IntegerType(), True),  # Change DateType() to IntegerType()
#     StructField("primaryProfession", ArrayType(StringType()), True),
#     StructField("knownForTitles", ArrayType(StringType()), True)
# ])

# crew_file_path = "title.basics.tsv.gz"

# crew_df = spark.read.option("delimiter", "\t").csv(crew_file_path, header=True, schema=title_basics_scheme)

# crew_df.show()

# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder.appName("my-spark-proj").getOrCreate()

name_basics = "name.basics.tsv.gz"
akas = "title.akas.tsv.gz"
basics = "title.basics.tsv.gz"
crew = "title.crew.tsv.gz"
episode = "title.episode.tsv.gz"
principals = "title.principals.tsv.gz"
ratings = "title.ratings.tsv.gz"

akas_scheme = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),  
    StructField("attributes", StringType(), True),  
    StructField("isOriginalTitle", BooleanType(), True),
])
title_basics_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", StringType(), True),
    StructField("endYear", StringType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True),  
])
crew_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])

episode_scheme = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("episodeNumber", IntegerType(), True)
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
    StructField("averageRating", DoubleType(),True),
    StructField("numVotes", IntegerType(),True)
])

name_basics_scheme = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", StringType(), True),
    StructField("deathYear", StringType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])

name_basics_df = spark.read.option("delimiter", "\t").csv(name_basics, header=True, schema=name_basics_scheme)
title_akas_df = spark.read.option("delimiter", "\t").csv(akas, header=True, schema=akas_scheme)
title_basics_df = spark.read.option("delimiter", "\t").csv(basics, header=True, schema=title_basics_scheme)
title_crew_df = spark.read.option("delimiter", "\t").csv(crew, header=True,schema=crew_scheme)
title_episode_df = spark.read.option("delimiter", "\t").csv(episode,header=True,schema = episode_scheme)
title_principlas_df = spark.read.option("delimiter", "\t").csv(principals, header = True, schema = principals_scheme)
title_ratings_df = spark.read.option("delimiter", "\t").csv(ratings, header = True, schema = ratings_scheme)



name_basics_df.show()
title_basics_df.show()
title_akas_df.show()
title_basics_df.show()
title_crew_df.show()
title_episode_df.show()
title_principlas_df.show()
title_ratings_df.show()


spark.stop()
