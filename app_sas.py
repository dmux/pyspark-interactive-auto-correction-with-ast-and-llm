# -----------------------------------------------------------------------------
# SAS to PySpark Transpiler with Multi-Agent Assistance & findspark
# (Versão com extração de amostra de dados e injeção na documentação)
#
# Funcionalidades:
# 1. Extração de Amostra: Tenta extrair dados de DATALINES/CARDS.
# 2. Agente 1 (LLM): Interpreta SAS -> Documentação Markdown (inclui amostra).
# 3. Editor Integrado: Edição da documentação.
# 4. Agente 2 (LLM): Transpila SAS -> PySpark (usa documentação c/ amostra).
# 5. findspark: Tenta localizar SPARK_HOME automaticamente.
# 6. Agente 3 (Subprocess): Tenta executar PySpark via 'spark-submit'.
#
# Pré-requisitos e Avisos: Mantidos como na versão anterior.
# -----------------------------------------------------------------------------

import streamlit as st
import os
import subprocess
import tempfile
import platform
import traceback
import findspark # Para localizar Spark
import locale    # Para encoding
import re        # Para parsing SAS (Regex)
from openai import OpenAI
from dotenv import load_dotenv # Descomente se usar .env localmente

load_dotenv() # Descomente se usar .env localmente
api_key = os.environ.get("OPENAI_API_KEY")

# --- Funções de Inicialização e LLM (get_llm_client, initialize_spark_environment) - SEM ALTERAÇÕES ---
# (Cole as funções initialize_spark_environment e get_llm_client aqui como estavam antes)
def initialize_spark_environment():
    """
    Tenta encontrar SPARK_HOME usando findspark ou a variável de ambiente.
    Atualiza o estado da sessão com o resultado.
    Retorna True se encontrado, False caso contrário.
    """
    if st.session_state.get('spark_found', False): return True
    spark_home_env = os.environ.get("SPARK_HOME")
    spark_path_found = None; found_method = None
    st.write("Inicializando descoberta do Spark...")
    if spark_home_env and os.path.isdir(spark_home_env):
        spark_path_found = spark_home_env; found_method = "Variável de Ambiente (SPARK_HOME)"
        try: findspark.init(spark_home_env); st.write(f"findspark inicializado com SPARK_HOME: {spark_home_env}")
        except Exception as e: st.warning(f"Aviso: SPARK_HOME encontrado ({spark_home_env}), mas findspark.init() falhou: {e}")
    else:
        try:
            st.write("SPARK_HOME não definido ou inválido. Tentando localizar com findspark...")
            findspark.init(); spark_path_found = findspark.find()
            os.environ['SPARK_HOME'] = spark_path_found # Define no ambiente atual
            found_method = "findspark (Busca Automática)"; st.write(f"Spark encontrado por findspark em: {spark_path_found}")
        except (ValueError, ImportError, Exception) as e: st.write(f"findspark não conseguiu localizar Spark: {e}"); spark_path_found = None

    if spark_path_found:
        st.session_state.spark_home_discovered = spark_path_found; st.session_state.spark_found_method = found_method
        st.session_state.spark_found = True; st.write("Estado da sessão atualizado: Spark encontrado."); return True
    else:
        st.session_state.spark_home_discovered = None; st.session_state.spark_found_method = "Nenhum"
        st.session_state.spark_found = False; st.write("Estado da sessão atualizado: Spark não encontrado."); return False

def get_llm_client():
    """Inicializa e retorna o cliente LLM (OpenAI)."""
    ui_api_key = st.session_state.get('api_key_input', None)
    current_api_key = api_key or ui_api_key
    if not current_api_key: st.error("Erro: Chave da API OpenAI não configurada."); return None
    try: client = OpenAI(api_key=current_api_key); client.models.list(); return client
    except Exception as e: st.error(f"Erro ao inicializar/conectar ao cliente OpenAI: {e}"); return None
# --- Fim das Funções Não Alteradas ---

def extract_sas_data_samples(sas_code: str, max_sample_rows=5) -> dict:
    """
    Tenta extrair nome do dataset, esquema (colunas/tipos) e dados de amostra
    de blocos DATA/INPUT/DATALINES no código SAS.
    Retorna um dicionário: { 'dataset_name': {'schema': {}, 'data': []} }
    """
    samples = {}
    # Regex para encontrar blocos DATA ... RUN; (simplificado)
    data_step_pattern = re.compile(r"DATA\s+(?:WORK\.)?(\w+)\s*;.*?INPUT(.*?);.*?DATALINES\s*;\s*(.*?)\s*;\s*RUN;", re.IGNORECASE | re.DOTALL)
    # Regex alternativo para CARDS
    cards_step_pattern = re.compile(r"DATA\s+(?:WORK\.)?(\w+)\s*;.*?INPUT(.*?);.*?CARDS\s*;\s*(.*?)\s*;\s*RUN;", re.IGNORECASE | re.DOTALL)

    for pattern in [data_step_pattern, cards_step_pattern]:
        matches = pattern.finditer(sas_code)
        for match in matches:
            try:
                dataset_name = match.group(1).strip()
                input_statement = match.group(2).strip()
                datalines_content = match.group(3).strip()

                if not dataset_name or not input_statement or not datalines_content:
                    continue

                # Parse INPUT statement (simplificado)
                columns = []
                schema = {}
                # Remove possíveis especificadores de formato/informat como 8. ou $20. por simplicidade
                cleaned_input = re.sub(r'\s+\d+\.?\-?\d*', '', input_statement) # Remove 8. ou 10-12
                cleaned_input = re.sub(r'\s+\$\d+\.?', '', cleaned_input)      # Remove $20. ou $
                
                input_parts = cleaned_input.split()
                current_vars = []
                for part in input_parts:
                    if part == '$': # Marca as variáveis anteriores como string
                        for var in current_vars:
                             if var in schema: schema[var] = 'string'
                        current_vars = []
                    elif part.endswith('$'): # Variável termina com $
                        var_name = part[:-1]
                        columns.append(var_name)
                        schema[var_name] = 'string'
                        current_vars = []
                    else: # Assume numérico por padrão, ou marca para verificação posterior
                        columns.append(part)
                        schema[part] = 'numeric' # Inicialmente assume numérico
                        current_vars.append(part)

                # Parse DATALINES
                data_rows = []
                lines = datalines_content.split('\n')
                count = 0
                for line in lines:
                    line = line.strip()
                    if not line: continue
                    if count >= max_sample_rows: break

                    values = line.split() # Assume delimitado por espaço
                    if len(values) == len(columns):
                        row_data = {}
                        is_valid_row = True
                        for i, col_name in enumerate(columns):
                            val = values[i]
                            row_data[col_name] = val
                            # Refinar tipo numérico: se não puder converter para float, é string
                            if schema[col_name] == 'numeric':
                                try:
                                    float(val)
                                except ValueError:
                                    schema[col_name] = 'string' # Corrige para string
                        if is_valid_row:
                             data_rows.append(row_data) # Armazena como dicionário
                             count += 1
                    else:
                         # Log de aviso (opcional): st.warning(f"Linha de dados ignorada no dataset {dataset_name}: número de colunas incompatível.")
                         pass


                if columns and data_rows:
                    # Garante que o schema final reflita a correção string/numeric
                    final_schema = {col: schema.get(col, 'string') for col in columns}
                    samples[dataset_name] = {'schema': final_schema, 'data': data_rows}
            except Exception as e:
                st.warning(f"Erro ao tentar extrair amostra do dataset (match: {match.groups()}): {e}")
                # Continue tentando outros matches

    return samples

def format_sample_as_markdown(dataset_name: str, schema: dict, data: list) -> str:
    """Formata um único conjunto de dados de amostra como tabela Markdown."""
    if not data: return ""

    headers = list(schema.keys())
    types = [f"`{schema[h]}`" for h in headers] # Adiciona tipo inferido abaixo do header

    # Cabeçalho da tabela
    md = f"#### Amostra: `{dataset_name}`\n\n"
    md += "| " + " | ".join(headers) + " |\n"
    md += "|-" + "-|".join(['-' * len(h) for h in headers]) + "-|\n"
    # Linha de Tipos (experimental)
    # md += "| " + " | ".join(types) + " |\n" # Descomente se quiser tipos na tabela

    # Linhas de dados
    for row_dict in data:
        row_values = [str(row_dict.get(h, '')) for h in headers] # Pega valores do dict
        md += "| " + " | ".join(row_values) + " |\n"

    return md + "\n"

def agent_1_generate_documentation(client: OpenAI, sas_code: str) -> tuple[str, str]:
    """
    Simula Agente 1: Analisa SAS -> Documentação Markdown.
    **MODIFICADO:** Extrai amostras de dados e as anexa à documentação.
    Retorna (documentação_completa, amostras_markdown)
    """
    if not sas_code.strip(): return "*Insira o código SAS.*", ""
    if not client: return "*Erro: Cliente LLM não inicializado.*", ""

    # 1. Extrair amostras ANTES de chamar o LLM
    extracted_samples = {}
    samples_markdown = ""
    try:
        extracted_samples = extract_sas_data_samples(sas_code)
        if extracted_samples:
            samples_markdown = "\n\n---\n\n### Dados de Amostra (Inferidos do Código SAS)\n\n"
            for name, sample_info in extracted_samples.items():
                samples_markdown += format_sample_as_markdown(name, sample_info['schema'], sample_info['data'])
            st.success(f"Amostra(s) de dados extraída(s) para: {', '.join(extracted_samples.keys())}")
        else:
            st.info("Nenhuma amostra de dados (INPUT/DATALINES) encontrada ou extraível do código SAS.")
    except Exception as e:
        st.warning(f"Falha na extração da amostra de dados: {e}")


    # 2. Gerar documentação base com LLM
    prompt_agent1 = f"""
    **Tarefa:** Analisar o código SAS e gerar documentação Markdown detalhada.
    Foco em: Objetivo de Negócio, Lógica Técnica, Entradas (nomes dos datasets SAS), Saídas (nomes dos datasets SAS), Detalhamento dos Passos.
    **Importante:** Se o código contiver blocos `DATA` com `INPUT`/`DATALINES`, mencione a estrutura de dados (nomes de coluna) dessas tabelas de entrada.
    **Formato:** Markdown claro e bem estruturado.
    **Código SAS:**
    ```sas
    {sas_code}
    ```
    **Documentação Gerada:**
    """
    base_documentation = "*Erro interno ao gerar documentação base.*" # Default
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini", # Ou outro modelo adequado
            messages=[
                {"role": "system", "content": "Você é um especialista em análise de código SAS e geração de documentação técnica clara."},
                {"role": "user", "content": prompt_agent1}
            ],
            temperature=0.4, max_tokens=1500
        )
        base_documentation = response.choices[0].message.content.strip()
    except Exception as e:
        st.error(f"Erro API (Agente 1 - Doc Base): {e}")
        base_documentation = f"*Erro ao gerar documentação base: {e}*"

    # 3. Combinar documentação base e amostras (se existirem)
    final_documentation = base_documentation + samples_markdown

    return final_documentation, samples_markdown # Retorna ambos para possível uso separado


def agent_2_transpile_to_pyspark(client: OpenAI, sas_code: str, documentation: str) -> str:
    """
    Simula Agente 2: Transpila SAS -> PySpark, usando a documentação (que agora inclui amostras)
    para guiar a criação de DataFrames de entrada.
    **MODIFICADO:** Prompt instrui a usar as amostras da documentação.
    """
    if not sas_code.strip() or not documentation.strip(): return "*Código SAS e documentação necessários.*"
    if not client: return "# Erro: Cliente LLM não inicializado."

    prompt = f"""
    **Tarefa:** Você é um especialista em SAS e PySpark. Sua tarefa é gerar um script PySpark **completo e executável localmente**:
    1.  **Incluir Inicialização da SparkSession:** Comece o script gerando o código necessário para importar `SparkSession` e criar uma instância local chamada `spark`. Use `.appName("GeneratedPySparkJob").master("local[*]")`.
    2.  **Analisar Entradas:** Use o código SAS e a documentação para identificar as tabelas/datasets de ENTRADA.
    3.  **Gerar Dados de Entrada usando Amostra da Documentação:** Crie DataFrames PySpark para essas entradas usando `spark.createDataFrame()`.
        *   **PRIORIZE OS DADOS DA DOCUMENTAÇÃO:** Procure por uma seção "Dados de Amostra" ou tabelas Markdown na documentação fornecida. Se encontrar amostras:
            *   Use os **nomes das colunas** e os **dados das linhas** dessas amostras para criar os DataFrames.
            *   **Infira os tipos de dados PySpark (StringType, IntegerType, DoubleType, DateType etc.)** a partir dos valores na amostra e/ou da descrição na documentação. Importe os tipos de `pyspark.sql.types`. Use `StringType` como padrão seguro se incerto. Tente converter para `IntegerType` ou `DoubleType` se os valores da amostra parecerem numéricos. Para datas, pode ser necessário usar `StringType` inicialmente se o formato não for óbvio, ou `DateType` se parecer `YYYY-MM-DD`. Importe `date` e `datetime` de `datetime`.
            *   Use nomes de variáveis baseados nos nomes SAS dos datasets (ex: `clientes`, `vendas`).
        *   **SE NÃO HOUVER AMOSTRA na documentação:** Gere 5-10 linhas de dados fictícios **plausíveis e funcionais**, inferindo esquema e tipos do SAS/documentação restante. Garanta consistência de chaves para joins.
        *   Coloque esta criação de dados *após* a inicialização da SparkSession.
    4.  **Transpilar Lógica SAS:** Converta a lógica SAS restante (DATA steps, PROC SQL, SORT, etc.) para a API de DataFrames PySpark, operando nos DataFrames criados.
    5.  **Mostrar/Indicar Saídas:** Use `dataframe_name.show()` para saídas de `PROC PRINT`. Comente `# Resultado final 'nome_df' pronto...` para tabelas finais.
    6.  **Incluir Encerramento (Opcional, mas bom):** Adicione `spark.stop()` no final, idealmente em um bloco `finally`.

    **Contexto:** A documentação, **especialmente a seção de amostra de dados (se existir)**, é chave para estrutura de dados, tipos e relações.

    **Diretrizes:**
    *   Use as melhores práticas PySpark.
    *   Adicione comentários claros.
    *   Gere **APENAS o código PySpark completo e executável**, começando com imports e SparkSession, seguido pela criação de dados e lógica. Não inclua explicações externas nem ```python.

    **Código SAS Original:**
    ```sas
    {sas_code}
    ```

    **Documentação de Contexto (Markdown - PODE CONTER AMOSTRAS DE DADOS):**
    ```markdown
    {documentation}
    ```

    **Código PySpark Completo Gerado:**
    ```python
    # -*- coding: utf-8 -*-
    # Script PySpark completo gerado por IA

    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    # Importações essenciais (incluindo SparkSession e tipos)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType # Adicionei BooleanType
    from datetime import date, datetime
    import traceback # Para bloco finally opcional

    spark = None # Inicializa para o bloco finally
    try:
        # --- 1. Inicialização da SparkSession ---
        spark = SparkSession.builder \\
            .appName("GeneratedPySparkJob") \\
            .master("local[*]") \\
            .config("spark.sql.repl.eagerEval.enabled", True) \\
            .getOrCreate()

        print("SparkSession iniciada com sucesso.")

        # --- 2. Geração de Dados de Entrada (Usando amostra da documentação se disponível) ---
        # (O código spark.createDataFrame(...) gerado vai aqui, baseado nas instruções acima)

        # --- 3. Lógica SAS Transpilada ---
        # (O código PySpark transpilado da lógica SAS vai aqui)

        print("\\nScript PySpark (lógica principal) concluído.")

    except Exception as e:
        print("\\n--- ERRO DURANTE A EXECUÇÃO DO SCRIPT ---")
        traceback.print_exc()

    finally:
        # --- 4. Encerramento da SparkSession ---
        if spark:
            print("\\n--- Encerrando SparkSession ---")
            spark.stop()
            print("SparkSession encerrada.")
    ```
    """ # Fim do Prompt

    try:
        response = client.chat.completions.create(
            model="gpt-4o", # Modelo mais capaz é recomendado aqui
            messages=[
                {"role": "system", "content": "Você gera scripts PySpark completos e autossuficientes a partir de SAS e documentação, priorizando dados de amostra fornecidos."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.25, # Ligeiramente mais determinístico para usar a amostra
            max_tokens=3800
        )
        pyspark_code_raw = response.choices[0].message.content

        # Limpeza do código (mesma lógica de antes)
        code_lines = pyspark_code_raw.split('\n')
        start_index = 0; end_index = len(code_lines)
        if code_lines and code_lines[0].strip().startswith("```python"): start_index = 1
        for i in range(len(code_lines) - 1, -1, -1):
             if code_lines[i].strip() == "```": end_index = i; break
        # Remove comentários de seção agora que o LLM os gera internamente
        # cleaned_lines = [line for line in code_lines[start_index:end_index] if not line.strip().startswith("# ---")]
        cleaned_lines = code_lines[start_index:end_index] # Mantém todos por enquanto
        first_code_line = 0
        for i, line in enumerate(cleaned_lines):
            if line.strip() and not line.strip().startswith('#'): first_code_line = i; break # Ignora comentários iniciais
        pyspark_code = "\n".join(cleaned_lines[first_code_line:]).strip()

        # Validações mínimas (o prompt agora é mais robusto)
        if "from pyspark.sql import SparkSession" not in pyspark_code:
             pyspark_code = "from pyspark.sql import SparkSession\n" + pyspark_code
        # Não adiciona F se não for usado
        # if "from pyspark.sql import functions as F" not in pyspark_code and " F." in pyspark_code :
        #      pyspark_code = "from pyspark.sql import functions as F\n" + pyspark_code

        return pyspark_code

    except Exception as e:
        st.error(f"Erro API (Agente 2 - Transpilação com Amostra): {e}")
        return f"# Erro na transpilação (script completo c/ amostra): {e}"


# --- Função agent_3_execute_pyspark - SEM ALTERAÇÕES ---
# (Cole a função agent_3_execute_pyspark aqui como estava antes)
def agent_3_execute_pyspark(pyspark_code: str) -> tuple[str, str, int]:
    """
    Simula Agente 3: Tenta executar o script PySpark **autossuficiente**
    gerado pelo Agente 2 usando spark-submit.
    """
    if not pyspark_code.strip(): return "*Nenhum código PySpark para executar.*", "", -1
    if not st.session_state.get('spark_found', False) or not st.session_state.get('spark_home_discovered'):
        error_msg = "Erro Crítico: SPARK_HOME não encontrado."; st.error(error_msg); return "", error_msg, -5
    spark_home = st.session_state.spark_home_discovered
    spark_submit_executable = os.path.join(spark_home, "bin", "spark-submit")
    is_windows = platform.system() == "Windows"
    if is_windows and not os.path.isfile(spark_submit_executable):
        spark_submit_cmd_path = spark_submit_executable + ".cmd"
        if os.path.isfile(spark_submit_cmd_path): spark_submit_executable = spark_submit_cmd_path
        else: error_msg = f"Erro: spark-submit(.cmd) não encontrado em '{os.path.join(spark_home, 'bin')}'."; st.error(error_msg); return "", error_msg, -6
    elif not is_windows and not os.path.isfile(spark_submit_executable):
         error_msg = f"Erro: spark-submit não encontrado em '{os.path.join(spark_home, 'bin')}'."; st.error(error_msg); return "", error_msg, -6

    stdout_str, stderr_str, return_code = "", "", -1
    tmp_file_path = None
    try: default_encoding = locale.getpreferredencoding(False)
    except (NameError, locale.Error): default_encoding = "cp1252" if is_windows else "utf-8"

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix=".py", delete=False, encoding=default_encoding, errors='replace') as tmp_file:
            tmp_file_path = tmp_file.name
            tmp_file.write(f"# -*- coding: {default_encoding} -*-\n") # Adiciona encoding no topo do arquivo
            tmp_file.write(pyspark_code)
            tmp_file.flush()
        st.info(f"Código PySpark autossuficiente salvo em: {tmp_file_path}")
        st.info(f"Executando com: {spark_submit_executable}")
        spark_submit_cmd = [spark_submit_executable, tmp_file_path]
        st.code(f"Comando: {' '.join(spark_submit_cmd)}", language="bash")
        process = subprocess.run(
            spark_submit_cmd, capture_output=True, text=True,
            encoding=default_encoding, errors='replace', timeout=300 # Timeout 5 min
        )
        stdout_str, stderr_str, return_code = process.stdout, process.stderr, process.returncode
        st.success(f"Subprocess concluído (Retorno: {return_code}).")
    except subprocess.TimeoutExpired as e:
        stderr_str = f"Erro: Timeout (300s).\n{getattr(e, 'stderr', '')}"; stdout_str = getattr(e, 'stdout', '')
        st.error(stderr_str); return_code = -4
    except Exception as e:
        stderr_str = f"Erro inesperado no subprocess:\n{traceback.format_exc()}"; st.error(stderr_str); return_code = -3
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            try: os.remove(tmp_file_path)
            except Exception as e_clean: st.warning(f"Falha ao limpar {tmp_file_path}: {e_clean}")
    return stdout_str, stderr_str, return_code
# --- Fim da Função Não Alterada ---


# --- Interface Streamlit (UI) ---
st.set_page_config(layout="wide", page_title="SAS -> PySpark Transpiler v2")
st.title("🤖 Transpilador Assistido por IA: SAS para PySpark (v2)")
st.caption("Extração Amostra -> Interpretação -> Edição -> Transpilação (Usa Amostra) -> Execução")

# Inicialização do Estado da Sessão (mantém os estados anteriores e adiciona 'sample_markdown')
if 'sas_code' not in st.session_state: st.session_state.sas_code = "/* Cole seu código SAS com DATA/INPUT/DATALINES aqui */"
if 'initial_documentation' not in st.session_state: st.session_state.initial_documentation = ""
if 'edited_documentation' not in st.session_state: st.session_state.edited_documentation = ""
if 'sample_markdown' not in st.session_state: st.session_state.sample_markdown = "" # Novo estado
if 'pyspark_code' not in st.session_state: st.session_state.pyspark_code = ""
if 'agent1_run_complete' not in st.session_state: st.session_state.agent1_run_complete = False
if 'show_transpile_button' not in st.session_state: st.session_state.show_transpile_button = False
if 'execution_stdout' not in st.session_state: st.session_state.execution_stdout = ""
if 'execution_stderr' not in st.session_state: st.session_state.execution_stderr = ""
if 'execution_ran' not in st.session_state: st.session_state.execution_ran = False
if 'execution_return_code' not in st.session_state: st.session_state.execution_return_code = None
if 'api_key_input' not in st.session_state: st.session_state.api_key_input = ""
if 'spark_found' not in st.session_state:
    st.session_state.spark_found = False; st.session_state.spark_home_discovered = None
    st.session_state.spark_found_method = "Nenhum"
    if not st.session_state.spark_found: initialize_spark_environment() # Tenta inicializar

# --- Abas da UI ---
tab1, tab2, tab3 = st.tabs([
    "1. SAS Input & Documentação",
    "2. Código PySpark Gerado",
    "3. Resultados da Execução"
])

with tab1:
    st.header("1. Código SAS")
    # Usar um exemplo SAS padrão que inclua DATALINES
    default_sas_code = """
/* Exemplo SAS com DATALINES */
DATA WORK.CLIENTES;
    INPUT ID_CLIENTE $ NOME $ CIDADE $ IDADE; /* IDADE é numérico */
    DATALINES;
C001 Alice São Paulo 30
C002 Bob Rio de Janeiro 25
C003 Charlie Belo Horizonte 45
C004 David São Paulo 30
;
RUN;

DATA WORK.VENDAS;
    INPUT ID_VENDA $ ID_CLIENTE $ PRODUTO $ VALOR DATA_VENDA yymmdd10.; /* Exemplo de tipo data SAS */
    FORMAT DATA_VENDA date9.; /* Formato de exibição */
    DATALINES;
V01 C001 Laptop 4500.00 2023-01-15
V02 C002 Teclado 350.50 2023-01-20
V03 C001 Monitor 1200.00 2023-02-10
V04 C003 Mouse 80.00 2023-03-05
V05 C001 Webcam 250.00 2023-03-12
V06 C004 Cadeira 950.75 2023-04-01
V07 C002 Monitor 1100.00 2023-04-05
;
RUN;

PROC SQL;
    CREATE TABLE WORK.RELATORIO_FINAL AS
    SELECT
        t1.NOME,
        t1.CIDADE,
        t1.IDADE,
        t2.PRODUTO,
        t2.VALOR,
        t2.DATA_VENDA
    FROM
        WORK.CLIENTES t1
    INNER JOIN
        WORK.VENDAS t2 ON t1.ID_CLIENTE = t2.ID_CLIENTE
    WHERE t1.IDADE > 28
    ORDER BY
        t1.CIDADE, t1.NOME;
QUIT;

PROC PRINT DATA=WORK.RELATORIO_FINAL NOOBS;
    TITLE "Relatório de Vendas (Idade > 28)";
RUN;
    """
    if 'sas_code' not in st.session_state or st.session_state.sas_code == "/* Cole seu código SAS com DATA/INPUT/DATALINES aqui */":
         st.session_state.sas_code = default_sas_code

    sas_input = st.text_area("Cole seu código SAS:", height=300, key="sas_code_input", value=st.session_state.sas_code)
    if sas_input != st.session_state.sas_code:
         st.session_state.sas_code = sas_input
         # Resetar estados dependentes quando o SAS muda
         st.session_state.agent1_run_complete = False; st.session_state.show_transpile_button = False
         st.session_state.pyspark_code = ""; st.session_state.execution_ran = False
         st.session_state.initial_documentation = ""; st.session_state.edited_documentation = ""
         st.session_state.sample_markdown = ""; st.rerun()

    if st.button("🔍 Analisar, Extrair Amostra e Gerar Documentação (Agente 1)", disabled=not st.session_state.sas_code.strip()):
        client = get_llm_client()
        if client:
            with st.spinner("🧠 Agente 1 analisando SAS e extraindo amostra..."):
                # Agente 1 agora retorna (documentação_completa, amostras_markdown)
                doc_completa, amostra_md = agent_1_generate_documentation(client, st.session_state.sas_code)
                st.session_state.initial_documentation = doc_completa
                st.session_state.edited_documentation = doc_completa # Inicia edição com a doc completa
                st.session_state.sample_markdown = amostra_md # Salva o markdown da amostra
                st.session_state.agent1_run_complete = True; st.session_state.show_transpile_button = True
                st.session_state.pyspark_code = ""; st.session_state.execution_ran = False; st.rerun()

    if st.session_state.agent1_run_complete:
        st.header("2. Documentação Gerada (Editável)")
        st.caption("A documentação abaixo inclui a análise do LLM e as tabelas de amostra de dados (se extraídas).")
        edited_doc_input = st.text_area("Edite a documentação (Markdown):", value=st.session_state.edited_documentation, height=300, key="doc_editor")
        if edited_doc_input != st.session_state.edited_documentation:
            st.session_state.edited_documentation = edited_doc_input # Apenas salva a edição

        # Botão para transpilar agora usa a documentação editada (que contém as amostras)
        if st.session_state.show_transpile_button:
            if st.button("✨ Transpilar para PySpark (Agente 2 - Usando Doc/Amostra)", type="primary", disabled=not st.session_state.edited_documentation.strip()):
                client = get_llm_client()
                if client:
                    with st.spinner("🔄 Agente 2 transpilando para PySpark (usando amostra se disponível)..."):
                        # Passa o código SAS original e a documentação EDITADA (que contém a amostra)
                        st.session_state.pyspark_code = agent_2_transpile_to_pyspark(client, st.session_state.sas_code, st.session_state.edited_documentation)
                        st.session_state.execution_ran = False; st.rerun() # Vai para a aba 2 mostrar o código

# --- Aba 2: Código PySpark ---
with tab2:
    if st.session_state.pyspark_code:
        st.header("3. Código PySpark Gerado")
        st.caption("Este código foi gerado usando o código SAS e a documentação (incluindo amostras, se disponíveis).")
        st.code(st.session_state.pyspark_code, language="python", line_numbers=True)
        st.warning("⚠️ **Aviso:** O código gerado tenta usar as amostras de dados. A execução ainda é experimental e pode precisar de ajustes.")

        run_button_disabled = not st.session_state.pyspark_code.strip() or not st.session_state.get('spark_found', False)
        button_tooltip = "Execução desabilitada: Verifique o código PySpark e se o Spark (SPARK_HOME) foi encontrado." if run_button_disabled else "Tentar executar o script PySpark via spark-submit."

        if st.button("🚀 Tentar Executar PySpark (Agente 3)", disabled=run_button_disabled, help=button_tooltip):
            with st.spinner("⏳ Agente 3 tentando executar via spark-submit..."):
                stdout, stderr, retcode = agent_3_execute_pyspark(st.session_state.pyspark_code)
                st.session_state.execution_stdout, st.session_state.execution_stderr = stdout, stderr
                st.session_state.execution_return_code, st.session_state.execution_ran = retcode, True
                st.rerun() # Vai para a aba 3 mostrar os resultados

    elif st.session_state.agent1_run_complete:
        st.info("Clique em 'Transpilar para PySpark (Agente 2)' na Aba 1.")
    else:
        st.info("Insira o código SAS e clique em 'Analisar...' na Aba 1 para começar.")

# --- Pré-visualização da Documentação na Aba 1 ---
with tab1: # Adiciona a pré-visualização de volta à aba 1
    st.markdown("---"); st.header("Pré-visualização da Documentação Editada")
    doc_to_show = st.session_state.edited_documentation # Mostra sempre a versão editável
    if doc_to_show: st.markdown(doc_to_show, unsafe_allow_html=True)
    elif st.session_state.initial_documentation: st.markdown(st.session_state.initial_documentation, unsafe_allow_html=True) # Fallback inicial
    else: st.info("Documentação e amostras aparecerão aqui após a análise.")


# --- Aba 3: Resultados da Execução ---
with tab3:
    st.header("4. Resultados da Execução (Agente 3)")
    if not st.session_state.execution_ran:
        st.info("Resultados da execução (stdout/stderr) do `spark-submit` aparecerão aqui após clicar em 'Tentar Executar PySpark'.")
    else:
        ret_code = st.session_state.execution_return_code
        status_msg = f"Status: {'Sucesso ✅' if ret_code == 0 else 'Falha ❌'} (Código de Retorno: {ret_code})"
        if ret_code == 0: st.success(status_msg)
        else: st.error(status_msg)

        # Mapeamento de erros (igual ao anterior)
        if ret_code != 0:
            error_map = {
                -2: "Falha Crítica: Comando 'spark-submit' não encontrado no PATH (erro subprocess).", -3: "Falha Crítica: Erro interno ao tentar executar o subprocess Python.",
                -4: "Falha: A execução excedeu o tempo limite (300s).", -5: "Falha Crítica: SPARK_HOME não encontrado (findspark/env var).",
                -6: f"Falha Crítica: spark-submit não encontrado em '{os.path.join(st.session_state.get('spark_home_discovered','?'), 'bin')}'."}
            default_error = f"Processo spark-submit terminou com erro ({ret_code}). Verifique stderr abaixo."
            st.warning(error_map.get(ret_code, default_error)) # Usar warning para não ser tão agressivo quanto error

        tab_stdout, tab_stderr = st.tabs(["Standard Output (stdout)", "Standard Error (stderr)"])
        with tab_stdout:
            st.caption("Saída padrão do processo `spark-submit`. Pode conter a saída de `print()` e `show()` do script PySpark.")
            st.code(st.session_state.execution_stdout or "(Saída padrão vazia)", language="log")
        with tab_stderr:
            st.caption("Saída de erro do processo `spark-submit`. Inclui logs do Spark, avisos e tracebacks de erros Python.")
            if st.session_state.execution_stderr.strip(): st.code(st.session_state.execution_stderr, language="log")
            else: st.info("(Saída de erro vazia)")

# --- Barra Lateral (Sidebar) --- (Sem alterações significativas, apenas texto)
st.sidebar.title("Configuração e Status")
st.sidebar.markdown("---")
st.sidebar.subheader("Ambiente Spark")
# (Lógica de exibição do status do Spark igual à anterior)
if st.session_state.get('spark_found', False):
    st.sidebar.success(f"✅ Spark Encontrado!")
    st.sidebar.caption(f"Método: {st.session_state.spark_found_method}")
    st.sidebar.caption(f"SPARK_HOME: {st.session_state.spark_home_discovered}")
else:
    st.sidebar.error("❌ Spark Não Encontrado!")
    st.sidebar.caption("Verifique a instalação do Spark ou defina SPARK_HOME.")
st.sidebar.markdown("---")
st.sidebar.subheader("Configuração API OpenAI")
# (Lógica da API Key igual à anterior)
st.sidebar.text_input("OpenAI API Key (Opcional)", type="password", key="api_key_input_widget",
                      help="Use se não configurado via secrets/env var.",
                      on_change=lambda: st.session_state.update(api_key_input=st.session_state.api_key_input_widget))
current_ui_key = st.session_state.get('api_key_input', None) or st.session_state.get('api_key_input_widget', None)
final_api_key_source = ""
if api_key: final_api_key_source = "Configurada (secrets/env)"
elif current_ui_key: final_api_key_source = "Configurada (UI nesta sessão)"
else: final_api_key_source = "Não Configurada"

if final_api_key_source.startswith("Configurada"): st.sidebar.success(f"API Key: {final_api_key_source}")
else: st.sidebar.warning(f"API Key: {final_api_key_source}")


st.sidebar.markdown("---")
st.sidebar.title("⚠️ Avisos Importantes")
st.sidebar.warning(
    """
    *   **RISCO DE EXECUÇÃO (AGENTE 3):** Experimental. **NÃO USE EM PRODUÇÃO SEM REVISÃO HUMANA.**
    *   **ADAPTAÇÃO DE CÓDIGO:** Código PySpark gerado (mesmo com amostras) **pode precisar de ajustes** (tipos de dados, lógica complexa).
    *   **PRECISÃO IA:** Transpilação, documentação e inferência de tipos/amostras não são perfeitas. Revise.
    *   **EXTRAÇÃO DE AMOSTRA:** A extração é baseada em Regex e funciona melhor com `INPUT`/`DATALINES` simples. Pode falhar em casos complexos.
    *   **CUSTOS API:** Uso da API OpenAI pode gerar custos.
    """
)
st.sidebar.info("Versão com extração de amostra e findspark.")