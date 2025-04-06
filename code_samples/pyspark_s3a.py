import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- 1. Configuração das Credenciais AWS (Escolha UMA das opções) ---

# Opção A: Via Variáveis de Ambiente (Recomendado para segurança)
# Antes de rodar o script, defina no seu terminal:
# export AWS_ACCESS_KEY_ID='SUA_ACCESS_KEY_ID'
# export AWS_SECRET_ACCESS_KEY='SUA_SECRET_ACCESS_KEY'
# export AWS_REGION='sua-regiao-aws' # Ex: us-east-1 (Opcional, mas bom ter)

# Opção B: Diretamente na configuração do Spark (Menos seguro, OK para testes locais)
# Descomente as linhas abaixo e substitua com suas credenciais se escolher esta opção
# aws_access_key_id = "SUA_ACCESS_KEY_ID"
# aws_secret_access_key = "SUA_SECRET_ACCESS_KEY"
# aws_region = "sua-regiao-aws" # ex: us-east-1

# --- 2. Configuração do SparkSession ---

# Defina as versões dos pacotes (verifique compatibilidade com sua versão do Spark/Hadoop)
# Hadoop 3.3.x é comum com Spark 3.x
hadoop_aws_version = "3.3.4"
aws_sdk_version = "1.12.262" # aws-java-sdk-bundle

spark_builder = SparkSession.builder \
    .appName("SparkS3LocalWriteExample") \
    .master("local[*]") \
    .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}")

# Configuração do S3A FileSystem (Necessário para s3a://)
spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Configuração das Credenciais (Se estiver usando a Opção B acima)
# Descomente se estiver usando a Opção B
# spark_builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
# spark_builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
# spark_builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") # Importante para algumas regiões/configurações

# --- 3. Inicializar SparkSession ---
spark = spark_builder.getOrCreate()

print("SparkSession Iniciada com Sucesso!")
print(f"Usando hadoop-aws: {hadoop_aws_version}, aws-java-sdk-bundle: {aws_sdk_version}")