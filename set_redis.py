import redis
import uuid
from datetime import datetime

# função vai ser chamada duas vezes, uma para o dataframe de voos e outra para o dataframe de hoteis 
def createRedisSet(spark_df, id_column_name, redis_conn):

    # verifica se a coluna existe no dataframe
    if id_column_name not in spark_df.columns:
        print(f"Coluna '{id_column_name}' não encontrada no DataFrame.")
        return

    # selecionando todos os ids disponíveis no dataframe spark
    ids_to_add = [str(row[id_column_name]) for row in spark_df.select(id_column_name).collect()]
    if not ids_to_add:
        print("Nenhum ID encontrado no DataFrame Spark para adicionar ao Redis.")
        return

    # criando um novo set e sua chave
    redis_set_key = f"set:{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    print(f"Criando novo set Redis com chave '{redis_set_key}'...")

    # adicionando os elementos ao set 
    try:
        redis_conn.sadd(redis_set_key, *ids_to_add)
        print(f"{len(ids_to_add)} IDs adicionados com sucesso ao set '{redis_set_key}'!")
        print(f"Número total de membros no set: {redis_conn.scard(redis_set_key)}")
    except Exception as e:
        print(f"Erro ao adicionar IDs ao Redis: {e}")
        return

    return redis_set_key


def postgresByRedisSet(redis_set_key, tabela, coluna_id, spark, redis_conn, pg_conn):

    # pega a lista de ids
    try:
        ids = list(redis_conn.smembers(redis_set_key))
        if not ids:
            print(f"O set '{redis_set_key}' está vazio ou não existe.")
            return

        print(f"{len(ids)} IDs recuperados do set '{redis_set_key}'")

    except redis.exceptions.ConnectionError as e:
        print(f"Erro ao conectar ao Redis: {e}")
        return

    # vai se conectar no container postgres
    try:
        cur = pg_conn.cursor()

        # faz a consulta sql com os ids que recebeu
        sql = f"SELECT * FROM {tabela} WHERE {coluna_id} = ANY(%s);"
        cur.execute(sql, (ids,))

        resultados = cur.fetchall()
        colunas = [desc[0] for desc in cur.description]

        print(f"{len(resultados)} registros encontrados na tabela '{tabela}' com base nos IDs.")

        cur.close()

        df = spark.createDataFrame(resultados, schema=colunas)
        return df

    except Exception as e:
        print(f"Erro ao consultar o PostgreSQL: {e}")
        return
