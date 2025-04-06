# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, upper
import traceback

# Caminhos relativos ou simples como o usuário forneceria
input_data_file = "dados_entrada_raw.parquet" # Será um diretório criado pelo script principal
processed_stage1_dir = "intermediate_processed"
final_output_location = "resultados/finais.parquet" # Spark criará 'resultados' se não existir

print(f"PySpark Original: Input='{input_data_file}', Intermediate='{processed_stage1_dir}', Output='{final_output_location}'")

spark = None
try:
    print("PySpark: Iniciando SparkSession...")
    spark = SparkSession.builder \\
        .appName("LlamaIndex_PySpark_AST_Rewrite") \\
        .config("spark.driver.memory", "512m") \\
        .master("local[*]") \\
        .getOrCreate()
    print(f"PySpark: SparkSession iniciada.")

    # --- Leitura 1 ---
    # A chamada spark.read.parquet(input_data_file) será reescrita pelo AST
    print(f"PySpark: Tentando ler de: {input_data_file}")
    df_raw = spark.read.parquet(input_data_file)
    print("PySpark: Schema Raw (Lido com sucesso):")
    df_raw.printSchema()
    df_raw.show(3, truncate=False)

    # --- Processamento e Escrita 1 ---
    print(f"PySpark: Processando (adicionando ano)...")
    df_processed1 = df_raw.withColumn("ano_evento", year(col("data_evento")))

    print(f"PySpark: Tentando escrever intermediário em: {processed_stage1_dir}")
    # A chamada df.write...parquet(processed_stage1_dir) será reescrita
    df_processed1.write.mode('overwrite').parquet(processed_stage1_dir)
    print("PySpark: Intermediário salvo com sucesso.")

    # --- Leitura 2 ---
    print(f"PySpark: Tentando ler intermediário de: {processed_stage1_dir}")
    # A chamada spark.read.parquet(processed_stage1_dir) será reescrita
    df_read_processed1 = spark.read.parquet(processed_stage1_dir)
    print("PySpark: Schema Intermediário Lido com sucesso:")
    df_read_processed1.printSchema()
    df_read_processed1.show(3, truncate=False)

    # --- Processamento e Escrita 2 ---
    print(f"PySpark: Processamento final (upper)...")
    df_final = df_read_processed1.withColumn("coluna_b_upper", upper(col("coluna_b"))) \\
                                 .select("coluna_a", "ano_evento", "coluna_b_upper")

    print(f"PySpark: Tentando escrever final em: {final_output_location}")
    # A chamada df.write...parquet(final_output_location) será reescrita
    df_final.write.mode("overwrite").parquet(final_output_location)
    print("PySpark: Resultado final salvo com sucesso.")

    result_message = "Execução do código PySpark (Reescrito por AST) concluída com SUCESSO."

except Exception as e:
    print(f"PySpark: ERRO FATAL durante a execução.")
    print(traceback.format_exc()) # Imprime todo o stack trace para depuração
    result_message = f"Execução do código PySpark (Reescrito por AST) falhou: {{str(e)}}"

finally:
    if spark:
        print("PySpark: Parando SparkSession...")
        spark.stop()
        print("PySpark: SparkSession parada.")

# Retorna a mensagem final
result_message