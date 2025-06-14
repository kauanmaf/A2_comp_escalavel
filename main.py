# main.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Inicializa a SparkSession
    spark = SparkSession.builder \
        .appName("MeuPrimeiroJobSpark") \
        .getOrCreate()

    print("SparkSession criada com sucesso!")

    # Exemplo simples: criar um DataFrame e mostrar
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "ID"])

    print("DataFrame criado:")
    df.show()

    # Parar a SparkSession
    spark.stop()
    print("SparkSession encerrada.")