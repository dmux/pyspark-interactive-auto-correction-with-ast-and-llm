"""
Script de Exemplo ETL PySpark com Geração de Dados e ERROS INTRODUZIDOS

Este script demonstra um processo básico de ETL usando PySpark, mas com
ERROS INTENCIONAIS para testar a funcionalidade de correção.

Ele gera seus próprios dados de entrada ('dados_entrada_raw.parquet')
antes de tentar iniciar o ETL.

**AVISO:** Este script contém erros propositais e NÃO funcionará como está.
         Use-o para testar a detecção e correção de erros.

Erros Introduzidos:
1. SyntaxError: Falta uma vírgula na definição da lista de dados de entrada.
2. IndentationError: Indentação incorreta dentro do bloco 'except'.
3. NameError (Import): A função 'lit' é usada, mas sua importação foi removida.
4. NameError: Um nome de DataFrame está escrito incorretamente ('df_raww' em vez de 'df_raw').
5. SyntaxError: Falta ':' no final da declaração 'finally'.

Entrada Gerada Internamente:
- Tentativa de criar um diretório Parquet chamado 'dados_entrada_raw.parquet'.
- Schema dos dados gerados:
    - id_registro (inteiro)
    - nome_cliente (string)
    - data_transacao (data)
    - valor_transacao (double)

Transformações ETL (Tentativas, provavelmente falharão):
- Tentativas de enriquecer, filtrar, salvar e ler dados.

Saída:
- Provavelmente não produzirá saídas Parquet devido aos erros.
"""
import os
import traceback
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, upper, month, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
spark = None
result_message = 'Processo ETL iniciado, mas contém erros e não deve ter sucesso.'
try:
    print('ETL Script (com erros): Iniciando SparkSession...')
    spark = SparkSession.builder.appName('Streamlit_PySpark_ETL_WithErrors').config('spark.driver.memory', '512m').config('spark.sql.adaptive.enabled', 'false').master('local[*]').getOrCreate()
    print(f'ETL Script (com erros): SparkSession iniciada. Versão: {spark.version}')
    print('-' * 30)
    print('ETL Script (com erros): [Geração] Iniciando criação dos dados de entrada...')
    input_data_for_generation = [(1, 'Alice Silva', date(2023, 5, 15), 150.75), (2, 'Bruno Costa', date(2022, 11, 20), 89.9), (3, 'Carla Dias', date(2023, 1, 5), 320.0), (4, 'Daniel Souza', date(2023, 8, 22), 45.5), (5, 'Elisa Santos', date(2024, 2, 10), 199.99), (6, 'Fernando Lima', date(2023, 5, 18), 75.0), (7, 'Gabriela Melo', date(2022, 12, 30), 500.25)]
    input_schema = StructType([StructField('id_registro', IntegerType(), True), StructField('nome_cliente', StringType(), True), StructField('data_transacao', DateType(), True), StructField('valor_transacao', DoubleType(), True)])
    df_input_generation = spark.createDataFrame(data=input_data_for_generation, schema=input_schema)
    print('ETL Script (com erros): [Geração] Dados de exemplo a serem salvos (se a criação funcionar):')
    df_input_generation.show(truncate=False)
    print("ETL Script (com erros): [Geração] Salvando dados de entrada em 'dados_entrada_raw.parquet'...")
    df_input_generation.write.mode('overwrite').parquet('/var/folders/z4/zjs9h8rx6gj_2xc97nfw1f240000gn/T/temp_pyspark_st_run_50aa5f9f_a8d4c8be/dados_entrada_raw.parquet')
    print('ETL Script (com erros): [Geração] Dados de entrada (tentativa) salvos.')
    print('-' * 30)
    print(f"ETL Script (com erros): [E] Lendo dados de entrada gerados de: 'dados_entrada_raw.parquet'")
    df_raw = spark.read.parquet('/var/folders/z4/zjs9h8rx6gj_2xc97nfw1f240000gn/T/temp_pyspark_st_run_50aa5f9f_a8d4c8be/dados_entrada_raw.parquet')
    print('ETL Script (com erros): Schema dos dados lidos:')
    df_raw.printSchema()
    print('ETL Script (com erros): Amostra dos dados lidos:')
    df_raw.show(5, truncate=False)
    print('-' * 30)
    print('ETL Script (com erros): [T1] Iniciando transformações...')
    df_enriched = df_raw.withColumn('ano_transacao', year(col('data_transacao'))).withColumn('mes_transacao', month(col('data_transacao'))).withColumn('nome_cliente_upper', upper(col('nome_cliente'))).withColumn('source_system', lit('Sistema_B')).select('id_registro', 'nome_cliente_upper', 'data_transacao', 'valor_transacao', 'ano_transacao', 'mes_transacao', 'source_system')
    print('ETL Script (com erros): Schema após Enriquecimento (T1):')
    df_enriched.printSchema()
    print('ETL Script (com erros): Amostra dos dados enriquecidos (T1):')
    df_enriched.show(5, truncate=False)
    print('-' * 30)
    filter_year = 2023
    print(f'ETL Script (com erros): [T2] Filtrando dados para ano_transacao = {filter_year}...')
    df_filtered = df_enriched.filter(col('ano_transacao') == filter_year)
    print(f'ETL Script (com erros): Contagem de registros após filtro (ano = {filter_year}): {df_filtered.count()}')
    print('ETL Script (com erros): Amostra dos dados filtrados (T2):')
    df_filtered.show(5, truncate=False)
    print('-' * 30)
    print(f"ETL Script (com erros): [L-Intermediário] Escrevendo dados intermediários em: 'intermediate_data/transactions_filtered.parquet'")
    df_filtered.write.mode('overwrite').parquet('/var/folders/z4/zjs9h8rx6gj_2xc97nfw1f240000gn/T/temp_pyspark_st_run_50aa5f9f_a8d4c8be/intermediate_data/transactions_filtered.parquet')
    print('ETL Script (com erros): Dados intermediários (tentativa) escritos.')
    print('-' * 30)
    print(f"ETL Script (com erros): [E-Intermediário] Lendo dados do diretório intermediário: 'intermediate_data/transactions_filtered.parquet'")
    df_intermediate_read = spark.read.parquet('/var/folders/z4/zjs9h8rx6gj_2xc97nfw1f240000gn/T/temp_pyspark_st_run_50aa5f9f_a8d4c8be/intermediate_data/transactions_filtered.parquet')
    print('ETL Script (com erros): Schema dos dados intermediários lidos:')
    df_intermediate_read.printSchema()
    print('ETL Script (com erros): Amostra dos dados intermediários lidos:')
    df_intermediate_read.show(5, truncate=False)
    print('-' * 30)
    print('ETL Script (com erros): [T3] Aplicando transformação final...')
    df_final = df_intermediate_read.select(col('id_registro'), col('nome_cliente_upper').alias('cliente_final'), col('data_transacao'), col('valor_transacao').alias('valor_final'), col('ano_transacao'), col('mes_transacao'), col('source_system'))
    print('ETL Script (com erros): Schema final antes da escrita:')
    df_final.printSchema()
    print('ETL Script (com erros): Amostra dos dados finais:')
    df_final.show(5, truncate=False)
    print('-' * 30)
    print(f"ETL Script (com erros): [L-Final] Escrevendo resultado final em: 'output_data/final_report.parquet'")
    df_final.write.mode('overwrite').parquet('/var/folders/z4/zjs9h8rx6gj_2xc97nfw1f240000gn/T/temp_pyspark_st_run_50aa5f9f_a8d4c8be/output_data/final_report.parquet')
    print('ETL Script (com erros): Resultado final (tentativa) escrito.')
    print('-' * 30)
    result_message = 'Execução do script ETL PySpark (com erros) CONCLUÍDA (INESPERADO!).'
except Exception as e:
    print(f'ETL Script (com erros): ERRO CAPTURADO (esperado).')
    print(traceback.format_exc())
    error_type = type(e).__name__
    result_message = f'Execução do script ETL PySpark (com erros) FALHOU (esperado): {error_type} - {e}'
finally:
    if spark:
        print('ETL Script (com erros): Parando SparkSession...')
        spark.stop()
        print('ETL Script (com erros): SparkSession parada.')
print('\n' + '=' * 40)
print(f'Mensagem Final: {result_message}')
print('=' * 40)