# -*- coding: utf-8 -*-
#import os
#import sys
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper # Erro 5: Importação comentada (opcional)
from pyspark.sql.functions import col, upper # Importação correta

# Configuração do PySpark (ajuste conforme necessário para seu ambiente)
# Tenta encontrar o Spark e Py4j automaticamente
try:
    # Define PYSPARK_PYTHON e PYSPARK_DRIVER_PYTHON para o executável Python atual
    # Isso ajuda a evitar conflitos de versão do Python entre o driver e os workers (em modo local)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    print(f"PYSPARK_PYTHON set to: {{sys.executable}}")
except Exception as e:
    print(f"Warning: Could not set PYSPARK_PYTHON environment variables: {{e}}")

print("Iniciando SparkSession...")
try:
    spark = SparkSession.builder \\
        .appName("ErrorCorrectionDemo") \\
        .master("local[*]") \\
        .config("spark.driver.memory", "2g") \\
        .config("spark.sql.adaptive.enabled", "false") # Desabilitar AQE para logs mais previsíveis em debug
        .getOrCreate()

    print("SparkSession iniciada com sucesso.")
    print(f"Versão do Spark: {{spark.version}}")

    sc = spark.sparkContext
    # print(f"Spark Context Web UI: {{sc.uiWebUrl}}") # Descomente se quiser ver a URL do Web UI

#except Exception as e:
    print(f"Erro CRÍTICO ao iniciar SparkSession: {{e}}")
    print("Verifique a instalação do Java, Spark e configurações de ambiente (SPARK_HOME, HADOOP_HOME se aplicável).")
    sys.exit(1) # Encerra o script se o Spark não puder iniciar


# Caminho para o arquivo Parquet
# Usar string bruta (r"...") é geralmente mais seguro para caminhos, especialmente no Windows
parquet_file = r"{safe_parquet_path}"
print(f"Tentando ler arquivo Parquet de: {{parquet_file}}")

# Leitura do Parquet com ERRO 1 (Typo)
try:
    # df = spark.read.parquet(parquet_file) # Linha correta
    df = spark.read.parquett(parquet_file) # ERRO 1: Typo em 'parquet'
    print("Arquivo Parquet lido com sucesso.")
except Exception as e:
    print(f"Erro ao ler o arquivo Parquet: {{e}}")
    # Em um cenário real, poderíamos tentar lidar com o erro aqui ou deixar o script falhar
    raise # Propaga o erro para ser pego pelo corretor

print("Schema original:")
df.printSchema()

print("Dados originais (5 primeiras linhas):")
df.show(5, truncate=False)

# Transformações com ERROS
print("Aplicando transformações...")

# ERRO 2: Erro de digitação no nome da coluna 'nome' vs 'name'
try:
    # df_transformed = df.withColumn("NAME_UPPER", upper(col("name"))) # Linha correta
    df_transformed = df.withColumn("NAME_UPPER", upper(col("nome"))) # ERRO 2: Coluna 'nome' não existe
except Exception as e:
    print(f"Erro na transformação (withColumn): {{e}}")
    raise

# Filtragem (idade > 30)
try:
    df_filtered = df_transformed.filter(col("age") > 30)
except Exception as e:
    print(f"Erro na transformação (filter): {{e}}")
    raise

print("Schema transformado:")
# ERRO 3: Parêntese faltando
try:
    # df_filtered.printSchema() # Linha correta
    df_filtered.printSchema( # ERRO 3: Parêntese final ausente
except Exception as e:
    print(f"Erro ao imprimir schema transformado: {{e}}")
    # Neste caso específico, um erro de sintaxe não será pego por try/except
    # Ele será pego pelo interpretador Python antes da execução.
    # O try/except está aqui mais para erros de runtime que poderiam ocorrer.
    pass # Deixa o erro de sintaxe ser pego

print("Dados transformados (filtrados por idade > 30):")
# ERRO 4: Erro de digitação no método 'show'
try:
    # df_filtered.show(truncate=False) # Linha correta
    df_filtered.shw(truncate=False) # ERRO 4: 'shw' em vez de 'show'
except Exception as e:
    print(f"Erro ao mostrar dados transformados: {{e}}")
    raise

# ERRO 6 (Opcional): Erro lógico ou variável não definida
# print(minha_variavel_inexistente)

# Contagem final
    try:
    count = df_filtered.count()
    print(f"Contagem final de registros (idade > 30): {{count}}")
except Exception as e:
    print(f"Erro ao contar registros finais: {{e}}")
    raise

print("Parando SparkSession...")
spark.stop()
print("Script concluído com sucesso.")

# Sinaliza sucesso para o processo pai (opcional)
# print("RESULT_SUCCESS")