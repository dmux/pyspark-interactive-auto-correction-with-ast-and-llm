# -*- coding: utf-8 -*-
"""
Script de Exemplo ETL PySpark com Geração de Dados de Entrada

Este script demonstra um processo básico de Extração, Transformação e Carga (ETL) usando PySpark.
Ele é projetado para ser executado no ambiente da aplicação Streamlit, que lida automaticamente
com a reescrita de caminhos Parquet relativos para um diretório temporário.

**Novidade:** Este script AGORA GERA seus próprios dados de entrada ('dados_entrada_raw.parquet')
usando PySpark antes de iniciar o ETL.

Entrada Gerada Internamente:
- Um diretório Parquet chamado 'dados_entrada_raw.parquet' é criado pelo próprio script.
- Schema dos dados gerados:
    - id_registro (inteiro)
    - nome_cliente (string)
    - data_transacao (data)
    - valor_transacao (double)

Transformações ETL (Após geração de dados):
1. Enriquecer dados: Adicionar ano e mês da transação, converter nome para maiúsculas.
2. Filtrar dados: Manter apenas transações de um ano específico (ex: 2023).
3. Escrita Intermediária: Salvar os dados filtrados em um local Parquet intermediário.
4. Leitura Intermediária: Carregar os dados de volta do local intermediário.
5. Transformação Final: Selecionar e renomear colunas para a saída final.

Saída:
- Parquet de Entrada Gerado: Diretório chamado 'dados_entrada_raw.parquet'
- Parquet Intermediário: Diretório chamado 'intermediate_data/transactions_filtered.parquet'
- Parquet Final: Diretório chamado 'output_data/final_report.parquet'
Todos serão criados dentro do diretório de execução temporário gerenciado pelo Streamlit.
"""

import os
import traceback
from datetime import date # Importa date para os dados de exemplo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, upper, lit, month
# Importa tipos para definir o schema explicitamente
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# Os caminhos relativos são usados diretamente nas chamadas .parquet().
# O reescritor AST da aplicação Streamlit modificará esses caminhos literais.

# --- Inicialização da Spark Session ---
spark = None
result_message = "Processo ETL não iniciado." # Mensagem padrão

try:
    print("ETL Script: Iniciando SparkSession...")
    spark = SparkSession.builder \
        .appName("Streamlit_PySpark_ETL_SelfGen") \
        .config("spark.driver.memory", "512m") \
        .config("spark.sql.adaptive.enabled", "false") \
        .master("local[*]") \
        .getOrCreate()
    print(f"ETL Script: SparkSession iniciada. Versão: {spark.version}")
    print("-" * 30)

    # --- GERAÇÃO DE DADOS DE ENTRADA ---
    print("ETL Script: [Geração] Iniciando criação dos dados de entrada...")

    # Define os dados de exemplo
    input_data_for_generation = [
        (1, "Alice Silva", date(2023, 5, 15), 150.75),
        (2, "Bruno Costa", date(2022, 11, 20), 89.90),
        (3, "Carla Dias", date(2023, 1, 5), 320.00),
        (4, "Daniel Souza", date(2023, 8, 22), 45.50),
        (5, "Elisa Santos", date(2024, 2, 10), 199.99),
        (6, "Fernando Lima", date(2023, 5, 18), 75.00),
        (7, "Gabriela Melo", date(2022, 12, 30), 500.25)
    ]

    # Define o schema para os dados de entrada
    input_schema = StructType([
        StructField("id_registro", IntegerType(), True),
        StructField("nome_cliente", StringType(), True),
        StructField("data_transacao", DateType(), True),
        StructField("valor_transacao", DoubleType(), True)
    ])

    # Cria o DataFrame Spark com os dados e schema definidos
    df_input_generation = spark.createDataFrame(data=input_data_for_generation, schema=input_schema)

    print("ETL Script: [Geração] Dados de exemplo a serem salvos:")
    df_input_generation.show(truncate=False)

    # Escreve o DataFrame como Parquet. O caminho "dados_entrada_raw.parquet"
    # será reescrito pelo reescritor AST do Streamlit para o diretório temporário.
    print("ETL Script: [Geração] Salvando dados de entrada em 'dados_entrada_raw.parquet'...")
    df_input_generation.write.mode("overwrite").parquet("dados_entrada_raw.parquet") # <--- Escrita dos dados gerados
    print("ETL Script: [Geração] Dados de entrada gerados e salvos com sucesso.")
    print("-" * 30)

    # --- EXTRAÇÃO (Agora lê os dados que acabou de gerar) ---
    print(f"ETL Script: [E] Lendo dados de entrada gerados de: 'dados_entrada_raw.parquet'")
    df_raw = spark.read.parquet("dados_entrada_raw.parquet") # <--- Leitura dos dados gerados (caminho reescrito)

    print("ETL Script: Schema dos dados lidos (após geração):")
    df_raw.printSchema()
    print("ETL Script: Amostra dos dados lidos (até 5 linhas):")
    df_raw.show(5, truncate=False)
    print("-" * 30)

    # --- TRANSFORMAÇÃO (Passo 1: Enriquecimento) ---
    print("ETL Script: [T1] Iniciando transformações - Adicionando ano, mês, nome_upper...")
    # Ajustado para usar os nomes das colunas geradas (data_transacao, nome_cliente)
    df_enriched = df_raw.withColumn("ano_transacao", year(col("data_transacao"))) \
                        .withColumn("mes_transacao", month(col("data_transacao"))) \
                        .withColumn("nome_cliente_upper", upper(col("nome_cliente"))) \
                        .withColumn("source_system", lit("Sistema_B")) \
                        .select("id_registro", "nome_cliente_upper", "data_transacao", "valor_transacao", \
                                "ano_transacao", "mes_transacao", "source_system") # Reordena/seleciona

    print("ETL Script: Schema após Enriquecimento (T1):")
    df_enriched.printSchema()
    print("ETL Script: Amostra dos dados enriquecidos (T1):")
    df_enriched.show(5, truncate=False)
    print("-" * 30)

    # --- TRANSFORMAÇÃO (Passo 2: Filtragem) ---
    filter_year = 2023 # Ano de filtro exemplo
    print(f"ETL Script: [T2] Filtrando dados para ano_transacao = {filter_year}...")
    df_filtered = df_enriched.filter(col("ano_transacao") == filter_year)

    print(f"ETL Script: Contagem de registros após filtro (ano = {filter_year}): {df_filtered.count()}")
    print("ETL Script: Amostra dos dados filtrados (T2):")
    df_filtered.show(5, truncate=False)
    print("-" * 30)

    # --- CARGA (Intermediária) ---
    # Salva os dados filtrados. Caminho será reescrito.
    print(f"ETL Script: [L-Intermediário] Escrevendo dados intermediários filtrados em: 'intermediate_data/transactions_filtered.parquet'")
    df_filtered.write.mode('overwrite').parquet("intermediate_data/transactions_filtered.parquet") # <--- Caminho literal
    print("ETL Script: Dados intermediários escritos com sucesso.")
    print("-" * 30)

    # --- EXTRAÇÃO (Intermediária) ---
    # Lê os dados intermediários. Caminho será reescrito.
    print(f"ETL Script: [E-Intermediário] Lendo dados do diretório intermediário: 'intermediate_data/transactions_filtered.parquet'")
    df_intermediate_read = spark.read.parquet("intermediate_data/transactions_filtered.parquet") # <--- Caminho literal

    print("ETL Script: Schema dos dados intermediários lidos:")
    df_intermediate_read.printSchema()
    print("ETL Script: Amostra dos dados intermediários lidos:")
    df_intermediate_read.show(5, truncate=False)
    print("-" * 30)

    # --- TRANSFORMAÇÃO (Passo 3: Seleção Final e Renomeação) ---
    print("ETL Script: [T3] Aplicando transformação final - Selecionando e renomeando colunas...")
    df_final = df_intermediate_read.select(
        col("id_registro"),
        col("nome_cliente_upper").alias("cliente_final"),
        col("data_transacao"),
        col("valor_transacao").alias("valor_final"),
        col("ano_transacao"),
        col("mes_transacao"),
        col("source_system")
    )

    print("ETL Script: Schema final antes da escrita:")
    df_final.printSchema()
    print("ETL Script: Amostra dos dados finais:")
    df_final.show(5, truncate=False)
    print("-" * 30)

    # --- CARGA (Final) ---
    # Escreve os dados finais. Caminho será reescrito.
    print(f"ETL Script: [L-Final] Escrevendo resultado final em: 'output_data/final_report.parquet'")
    df_final.write.mode("overwrite").parquet("output_data/final_report.parquet") # <--- Caminho literal
    print("ETL Script: Resultado final escrito com sucesso.")
    print("-" * 30)

    result_message = "Execução do script ETL PySpark (com auto-geração de dados) concluída com SUCESSO."

except Exception as e:
    print(f"ETL Script: ERRO FATAL durante a execução.")
    # Imprime o traceback completo para stderr
    print(traceback.format_exc())
    # Mensagem de erro amigável
    error_type = type(e).__name__
    result_message = f"Execução do script ETL PySpark (com auto-geração de dados) FALHOU: {error_type} - {e}"

finally:
    # --- Parar a Spark Session ---
    if spark:
        print("ETL Script: Parando SparkSession...")
        spark.stop()
        print("ETL Script: SparkSession parada.")

# --- Mensagem de Saída Final ---
print("\n" + "="*40)
print(f"Mensagem Final: {result_message}")
print("="*40)