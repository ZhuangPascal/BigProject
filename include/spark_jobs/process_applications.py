from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col

spark = SparkSession.builder.appName("ProcessApplications").getOrCreate()

applications = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "applications") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Ajout d'un score de pertinence basé sur la longueur du résumé
applications = applications.withColumn("relevance_score", length(col("resume_summary")) / 10)

applications.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "applications") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()