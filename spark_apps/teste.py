from pyspark.sql import SparkSession


def main():
    # Cria uma sessão Spark
    spark = SparkSession.builder.appName("Analyzing the vocabulary of Pride and Prejudice.").getOrCreate()

    # Cria um RDD a partir de uma lista
    data = ["Hello World", "Hello Spark", "Hello Python"]
    rdd = spark.sparkContext.parallelize(data)

    # Divide as linhas em palavras
    words = rdd.flatMap(lambda line: line.split(" "))

    # Conta as ocorrências de cada palavra
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # Coleta e imprime os resultados
    for word, count in word_counts.collect():
        print(f"{word}: {count}")

    # Para a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
