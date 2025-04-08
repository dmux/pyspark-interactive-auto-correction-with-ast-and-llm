# -----------------------------------------------------------------------------
# SAS to PySpark Transpiler v4.0
# (Separa dados estruturados da documentação para garantir uso na transpilação)
# -----------------------------------------------------------------------------
import streamlit as st
import os
import subprocess
import tempfile
import platform
import traceback
import findspark
import locale
import re
import json # Para passar estrutura de dados ao LLM de forma clara
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("OPENAI_API_KEY")

# --- Funções initialize_spark_environment, get_llm_client, parse_input_statement, identify_datasets_and_schemas ---
# (Cole as funções aqui como estavam na v3.1 - SEM ALTERAÇÕES)
# --- [INSERIR CÓDIGO DAS FUNÇÕES NÃO ALTERADAS AQUI] ---
def initialize_spark_environment():
    if st.session_state.get('spark_found', False): return True
    spark_home_env = os.environ.get("SPARK_HOME")
    spark_path_found = None; found_method = None
    if spark_home_env and os.path.isdir(spark_home_env):
        spark_path_found = spark_home_env; found_method = "Variável de Ambiente (SPARK_HOME)"
        try: findspark.init(spark_home_env)
        except Exception as e: st.warning(f"Aviso: SPARK_HOME encontrado ({spark_home_env}), mas findspark.init() falhou: {e}")
    else:
        try:
            findspark.init(); spark_path_found = findspark.find()
            os.environ['SPARK_HOME'] = spark_path_found
            found_method = "findspark (Busca Automática)"
        except (ValueError, ImportError, Exception) as e: spark_path_found = None

    if spark_path_found:
        st.session_state.spark_home_discovered = spark_path_found; st.session_state.spark_found_method = found_method
        st.session_state.spark_found = True; return True
    else:
        st.session_state.spark_home_discovered = None; st.session_state.spark_found_method = "Nenhum"
        st.session_state.spark_found = False; return False

def get_llm_client():
    ui_api_key = st.session_state.get('api_key_input', None) or st.session_state.get('api_key_input_widget', None)
    current_api_key = api_key or ui_api_key
    if not current_api_key: st.error("Erro: Chave da API OpenAI não configurada."); return None
    try: client = OpenAI(api_key=current_api_key); client.models.list(); return client
    except Exception as e: st.error(f"Erro ao inicializar/conectar ao cliente OpenAI: {e}"); return None

def parse_input_statement(input_statement: str) -> dict:
    schema = {}
    input_statement = re.sub(r'\(\s*[\w\$\.]+\s*(?:[\w\$\.\s]+)?\)', '', input_statement)
    input_statement = re.sub(r'@\(.*?\)|@\d+', '', input_statement)
    input_statement = re.sub(r'\+\d+', '', input_statement)
    input_statement = input_statement.replace('/', ' ')
    input_statement = re.sub(r'\s+\d+\.\d*', '', input_statement)
    input_statement = re.sub(r'\s+\$\d*\.?', '', input_statement)
    parts = input_statement.split()
    idx = 0
    while idx < len(parts):
        part = parts[idx]
        col_name = None
        col_type = 'numeric'
        if part == '$':
            idx += 1
            if idx < len(parts): col_name = parts[idx]; col_type = 'string'
            else: break
        elif part.endswith('$'): col_name = part[:-1]; col_type = 'string'
        else: col_name = part; col_type = 'numeric'
        if col_name and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_name): schema[col_name] = col_type
        idx += 1
    return schema

def identify_datasets_and_schemas(sas_code: str) -> dict:
    datasets = {}
    data_step_pattern_v2 = re.compile(r"DATA\s+(?:WORK\.)?(\w+)\s*;(.*?)?INPUT(.*?);(.*?)?(?:(?:DATALINES|CARDS)\s*;\s*(.*?)\s*;)?\s*RUN;", re.IGNORECASE | re.DOTALL)
    for match in data_step_pattern_v2.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        before_input = match.group(2) or ""; input_content = match.group(3).strip() if match.group(3) else ""
        between_input_data = match.group(4) or ""; inline_data_content = match.group(5).strip() if match.group(5) else ""
        has_infile = "INFILE" in before_input.upper() or "INFILE" in between_input_data.upper()
        has_inline = bool(inline_data_content)
        schema = parse_input_statement(input_content) if input_content else {}
        source = 'unknown'
        if has_inline: source = 'inline'
        elif has_infile: source = 'infile'
        elif schema: source = 'data_step_no_data'
        datasets[ds_name] = {'source': source, 'schema': schema, 'has_inline_data': has_inline, 'inline_data_snippet': inline_data_content[:500]} # Aumenta snippet
    sql_ref_pattern = re.compile(r"(?:FROM|JOIN)\s+(?:WORK\.)?(\w+)", re.IGNORECASE)
    proc_sql_pattern = re.compile(r"PROC\s+SQL;.*?QUIT;", re.IGNORECASE | re.DOTALL)
    create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(?:WORK\.)?(\w+)", re.IGNORECASE)
    for sql_match in proc_sql_pattern.finditer(sas_code):
        sql_block = sql_match.group(0)
        created_tables = {m.group(1).strip().upper() for m in create_table_pattern.finditer(sql_block)}
        for ref_match in sql_ref_pattern.finditer(sql_block):
            ds_name = ref_match.group(1).strip().upper()
            if ds_name not in datasets and ds_name not in created_tables:
                 datasets[ds_name] = {'source': 'proc_sql_ref', 'schema': {}, 'has_inline_data': False}
    proc_data_pattern = re.compile(r"PROC\s+\w+\s+DATA\s*=\s*(?:WORK\.)?(\w+)", re.IGNORECASE)
    for match in proc_data_pattern.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        if ds_name not in datasets:
             datasets[ds_name] = {'source': 'proc_ref', 'schema': {}, 'has_inline_data': False}
    return datasets
# --- Fim Funções Não Alteradas ---


# --- Função para extrair dados inline E retornar estrutura ---
def extract_inline_data_structured(sas_code: str, datasets_info: dict, max_sample_rows=5) -> tuple[dict, dict]:
    """
    Extrai dados inline (DATALINES/CARDS) e retorna uma estrutura de dados Python.
    Retorna (structured_data, updated_datasets_info).
    Onde structured_data = {'dataset_name': {'schema': ['col1', 'col2'], 'data': [['val1', 'val2'], ...], 'source': 'inline'}, ...}
    """
    structured_samples = {}
    updated_info = datasets_info.copy()

    for ds_name, info in datasets_info.items():
        if info['source'] == 'inline' and info.get('inline_data_snippet'):
            schema_dict = info.get('schema', {})
            if not schema_dict: continue

            columns = list(schema_dict.keys())
            datalines_content = info['inline_data_snippet']
            data_rows_as_list_of_lists = [] # Lista de listas
            lines = datalines_content.split('\n')
            count = 0

            current_schema_types = list(schema_dict.values()) # Guarda tipos atuais

            for line in lines:
                line = line.strip()
                if not line: continue
                if count >= max_sample_rows: break
                values = line.split() # Assume delimitado por espaço
                if len(values) == len(columns):
                    row_values_list = []
                    valid_row = True
                    for i, col_name in enumerate(columns):
                        val = values[i]
                        row_values_list.append(val)
                        # Refinar tipo numérico no schema_dict
                        if schema_dict[col_name] == 'numeric':
                            try: float(val)
                            except ValueError: schema_dict[col_name] = 'string'; current_schema_types[i] = 'string'
                    if valid_row:
                        data_rows_as_list_of_lists.append(row_values_list)
                        count += 1

            if data_rows_as_list_of_lists:
                 updated_info[ds_name]['schema'] = schema_dict # Atualiza schema refinado
                 structured_samples[ds_name] = {
                     'schema': columns, # Lista de nomes de colunas
                     'data': data_rows_as_list_of_lists, # Lista de listas
                     'source': 'inline'
                 }

    extracted_count = len(structured_samples)
    if extracted_count > 0:
        st.success(f"{extracted_count} amostra(s) de dados inline extraída(s) para estrutura.")
    # else:
    #     st.info("Nenhuma amostra de dados inline encontrada.")

    return structured_samples, updated_info

# --- Função para PARSEAR tabelas Markdown (geradas pelo LLM) ---
def parse_markdown_tables(markdown_text: str) -> dict:
    """
    Parseia tabelas Markdown no formato esperado (gerado pelo Agente 1)
    e retorna uma estrutura de dados Python.
    Retorna: {'dataset_name': {'schema': ['col1', 'col2'], 'data': [['val1', 'val2'], ...], 'source': 'generated'}, ...}
    """
    parsed_data = {}
    # Regex para encontrar tabelas Markdown precedidas pelo nosso cabeçalho específico
    # table_pattern = re.compile(
    #     r"####\s+Amostra\s+(?:Gerada\s+Automaticamente|Extraída):\s*`(\w+)`\s*\n+"  # 1: Dataset name
    #     r"\|([^|\n]+)\|\s*\n"             # 2: Header row (com pipes extras)
    #     r"\|-*\|(?:-*\|)+\s*\n"           # Separator row
    #     r"((?:\|[^|\n]+\|\s*\n)+)",       # 3: Data rows block
    #     re.IGNORECASE
    # )
     # Regex Simplificado (mais robusto a espaços extras)
    table_pattern = re.compile(
        r"####\s+Amostra\s+(?:Gerada\s+Automaticamente|Extraída):\s*`(\w+)`\s*\n+" # 1: Dataset Name
        r"\s*\|\s*(.*?)\s*\|\s*\n"          # 2: Header content (sem pipes externos)
        r"\s*\|(?:[-:]+\|)+[-:]+\|?\s*\n"   # Separator row (permite alinhamento :---:)
        r"((?:\s*\|.*?\|\s*\n)+)",          # 3: Data rows block
        re.IGNORECASE | re.MULTILINE
    )


    for match in table_pattern.finditer(markdown_text):
        try:
            dataset_name = match.group(1).strip().upper()
            header_content = match.group(2).strip()
            data_rows_block = match.group(3).strip()

            # Limpa e divide cabeçalhos
            headers = [h.strip() for h in header_content.split('|')]

            # Limpa e divide linhas de dados
            data_rows = []
            for line in data_rows_block.split('\n'):
                line = line.strip()
                if not line.startswith('|') or not line.endswith('|'): continue # Ignora linhas mal formatadas
                # Divide pelos pipes, ignora o primeiro e último elemento vazio
                values = [v.strip() for v in line.split('|')[1:-1]]
                if len(values) == len(headers):
                    data_rows.append(values)
                else:
                    st.warning(f"Linha ignorada ao parsear tabela '{dataset_name}': {len(values)} valores, esperava {len(headers)}. Linha: '{line}'")


            if dataset_name and headers and data_rows:
                # Determina a fonte baseado no cabeçalho da seção (melhoria futura: passar explicitamente)
                source = "generated" if "Gerada Automaticamente" in match.group(0) else "inline" # Ou outra heurística
                parsed_data[dataset_name] = {
                    'schema': headers,
                    'data': data_rows,
                    'source': source
                }
        except Exception as e:
            st.warning(f"Erro ao parsear tabela Markdown (dataset: {match.group(1) if match else 'N/A'}): {e}")

    parsed_count = len(parsed_data)
    if parsed_count > 0:
        st.success(f"{parsed_count} tabela(s) Markdown parseada(s) da resposta do LLM.")
    # else:
        # st.info("Nenhuma tabela Markdown formatada encontrada na resposta do LLM para parsear.")

    return parsed_data


# --- Agente 1 - Modificado para retornar estrutura de dados ---
def agent_1_orchestrator(client: OpenAI, sas_code: str) -> tuple[str, dict]:
    """
    Orquestra o Agente 1:
    1. Identifica datasets/schemas.
    2. Extrai dados inline para ESTRUTURA Python.
    3. Chama LLM para gerar documentação E amostras faltantes (como tabelas MD).
    4. PARSEIA as tabelas MD geradas pelo LLM para ESTRUTURA Python.
    5. Combina amostras (inline + geradas) na ESTRUTURA final.
    6. Retorna (documentacao_markdown_completa, estrutura_dados_combinada).
    """
    if not sas_code.strip(): return "*Insira o código SAS.*", {}
    if not client: return "*Erro: Cliente LLM não inicializado.*", {}

    # 1. Identificar
    datasets_info = {}
    try: datasets_info = identify_datasets_and_schemas(sas_code)
    except Exception as e: st.warning(f"Erro na identificação: {e}"); traceback.print_exc()

    # 2. Extrair dados INLINE para estrutura
    inline_structured_data, datasets_info = extract_inline_data_structured(sas_code, datasets_info)

    # 3. Preparar para LLM (gerar documentação + amostras faltantes)
    datasets_to_generate = []
    datasets_summary_for_prompt = "\n**Contexto dos Datasets Identificados:**\n"
    if datasets_info:
        for name, info in datasets_info.items():
            schema_dict = info.get('schema', {})
            schema_str = ", ".join([f"{col} ({typ})" for col, typ in schema_dict.items()]) if schema_dict else "Schema não inferido"
            source_desc = info.get('source', 'unknown'); has_inline_str = "Sim" if info.get('has_inline_data') else "Não"
            datasets_summary_for_prompt += f"- `{name}`: Fonte={source_desc}, Dados Inline={has_inline_str}, Schema Inferido=[{schema_str}]\n"
            # Adiciona para geração APENAS se NÃO foi extraído com sucesso inline
            if name not in inline_structured_data:
                 datasets_to_generate.append({'name': name, 'schema': schema_dict}) # Passa schema inferido se houver

    # Prompt (similar ao v3.1, focado em gerar tabelas MD)
    prompt_agent1 = f"""
    **Sua Tarefa Principal:** Analisar o código SAS e gerar documentação técnica E dados de amostra formatados.

    **Instruções Detalhadas:**
    1.  **Documentação Descritiva:** Descreva o objetivo, lógica, entradas e saídas do processo SAS.
    2.  **GERAÇÃO DE AMOSTRAS (Ação Chave):** Para CADA dataset listado em "Datasets para Gerar Amostra" abaixo:
        *   **Inferir/Refinar Schema:** Defina colunas e tipos plausíveis (String, Integer, Double, Date).
        *   **Gerar Dados Fictícios:** Crie **3 a 5 linhas** de dados consistentes (especialmente para joins).
        *   **FORMATAR COMO TABELA MARKDOWN (Obrigatório):** Apresente CADA conjunto de dados gerado como uma tabela Markdown clara, usando o formato exato abaixo (incluindo o cabeçalho `####`):

            ```markdown
            #### Amostra Gerada Automaticamente: `NOME_DO_DATASET`

            | NOME_COLUNA_1 | NOME_COLUNA_2 | ... |
            |---------------|---------------|-----|
            | valor_linha1_col1 | valor_linha1_col2 | ... |
            | valor_linha2_col1 | valor_linha2_col2 | ... |

            ```
    3.  **Estrutura Final da SUA Resposta:** Comece com a documentação descritiva. Depois, inclua TODAS as tabelas Markdown que você gerou (uma após a outra).

    **Código SAS para Análise:** ```sas\n{sas_code}\n```
    {datasets_summary_for_prompt}
    **Datasets para Gerar Amostra (Gerar Tabela Markdown para cada):**
    {chr(10).join([f"- `{d['name']}` (Schema Básico: {d['schema'] if d.get('schema') else 'Nenhum'})" for d in datasets_to_generate]) if datasets_to_generate else "Nenhum dataset externo identificado para geração de amostra."}
    ---
    **Documentação Descritiva e Tabelas de Amostra GERADAS (Markdown):**
    """ # Fim do prompt

    # Chamar LLM para gerar Descrição + Tabelas MD Faltantes
    llm_generated_markdown = "*Erro ao chamar LLM.*"
    try:
        num_to_generate = len(datasets_to_generate)
        st.info(f"Solicitando ao LLM documentação e geração de {num_to_generate} amostra(s) em formato tabela MD...")
        if num_to_generate > 0: st.caption(f"Gerando para: {', '.join([d['name'] for d in datasets_to_generate])}")

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "Você documenta SAS e gera dados de amostra OBRIGATORIAMENTE como tabelas Markdown formatadas."},
                      {"role": "user", "content": prompt_agent1}],
            temperature=0.4, max_tokens=3000
        )
        llm_generated_markdown = response.choices[0].message.content.strip()
        st.success("LLM respondeu.")

    except Exception as e:
        st.error(f"Erro API (Agente 1 - Doc + Geração Amostra): {e}")
        llm_generated_markdown = f"*Erro ao gerar documentação/amostra via LLM: {e}*"

    # 4. PARSEAR as tabelas Markdown geradas pelo LLM para ESTRUTURA Python
    llm_parsed_structured_data = parse_markdown_tables(llm_generated_markdown)

    # 5. Combinar amostras (inline + geradas) na ESTRUTURA final
    final_structured_data = inline_structured_data.copy()
    final_structured_data.update(llm_parsed_structured_data) # Sobrescreve inline se LLM gerou mesmo nome? Verificar lógica.
    # Idealmente, llm_parsed_structured_data só contém os que FALTAVAM.

    # 6. Preparar documentação final para EXIBIÇÃO (inclui tabelas para visualização)
    #    Recupera as tabelas MD inline (precisamos formatá-las para exibição também)
    inline_samples_md_display = ""
    for ds_name, data_info in inline_structured_data.items():
         # Precisa recriar o schema dict para a função de formatação
         schema_dict_inline = {col: datasets_info.get(ds_name,{}).get('schema',{}).get(col,'string') for col in data_info['schema']}
         # Recria lista de dicts para a função
         data_rows_dict = [dict(zip(data_info['schema'], row)) for row in data_info['data']]
         inline_samples_md_display += format_data_as_markdown_table(ds_name, schema_dict_inline, data_rows_dict, "inline")

    if inline_samples_md_display:
         inline_samples_md_display = "\n\n---\n\n### Dados de Amostra Extraídos (Inline)\n" + inline_samples_md_display


    # A documentação final é o que o LLM retornou (descrição + tabelas geradas) + tabelas inline formatadas
    final_markdown_for_display = llm_generated_markdown + "\n" + inline_samples_md_display

    st.success(f"Processo Agente 1 concluído. {len(final_structured_data)} dataset(s) com amostra na estrutura final.")

    return final_markdown_for_display, final_structured_data


# --- Agente 2 - Modificado para usar estrutura de dados ---
def agent_2_transpile_to_pyspark_from_structure(client: OpenAI, sas_code: str, sample_data: dict) -> str:
    """
    Agente 2 (v4.0): Transpila SAS -> PySpark usando ESTRUTURA de dados de amostra.
    """
    if not sas_code.strip(): return "# Código SAS necessário."
    if not client: return "# Erro: Cliente LLM não inicializado."
    if not sample_data: st.warning("Agente 2 chamado sem dados de amostra estruturados!"); sample_data = {} # Evita erro, mas transpilação pode falhar

    # Converte a estrutura de dados para JSON para inclusão clara no prompt
    try:
        sample_data_json = json.dumps(sample_data, indent=2)
    except Exception as e:
        st.error(f"Erro ao serializar dados de amostra para JSON: {e}")
        sample_data_json = "{}"

    prompt = f"""
    **Tarefa:** Você é um especialista em SAS e PySpark. Gere um script PySpark **completo e executável localmente**.

    **Instruções:**
    1.  **Inicialização SparkSession:** Inclua imports (`SparkSession`, `types`, `functions`, `datetime`) e criação da `SparkSession` local (`spark`).
    2.  **Gerar DataFrames de Entrada a partir da ESTRUTURA JSON:**
        *   Abaixo, em `Dados de Amostra Estruturados (JSON)`, você encontrará um dicionário JSON. As chaves são nomes de datasets SAS. Cada valor contém:
            *   `"schema"`: Uma lista com os nomes das colunas.
            *   `"data"`: Uma lista de listas, onde cada sub-lista representa uma linha de dados.
        *   Para **CADA** dataset neste JSON:
            *   Gere o código PySpark para criar um DataFrame usando `spark.createDataFrame(data, schema)`.
            *   Use os valores de `"data"` e `"schema"` do JSON diretamente.
            *   **Infira os tipos de dados PySpark** (`StringType`, `IntegerType`, `DoubleType`, `DateType`, etc.) a partir dos *valores* nas listas de dados. Importe os tipos necessários de `pyspark.sql.types`. Use `StringType` como padrão seguro se incerto. Tente `IntegerType`/`DoubleType` para números. Use `DateType` para 'YYYY-MM-DD', senão `StringType`. **Você pode precisar especificar o schema explicitamente na chamada `createDataFrame` usando `StructType` e `StructField` se a inferência automática não for suficiente ou se precisar forçar tipos específicos (como DateType).**
            *   Use nomes de variáveis Python baseados nos nomes dos datasets SAS (ex: `clientes_df`, `vendas_df`).
        *   **NÃO INVENTE DADOS.** Use APENAS os dados fornecidos na estrutura JSON. Se um dataset referenciado no SAS não estiver no JSON, a transpilação da lógica que o utiliza pode falhar (isso é esperado).
    3.  **Transpilar Lógica SAS:** Converta a lógica SAS restante (PROC SQL, SORT, WHERE, etc.) para operações de DataFrame PySpark, operando nos DataFrames criados no passo anterior.
    4.  **Saídas:** Use `dataframe_name.show()` para simular `PROC PRINT`.
    5.  **Encerramento:** Inclua `spark.stop()` (idealmente em `finally`).

    **Diretrizes:** Gere **APENAS o código PySpark completo e executável**. Sem explicações externas.

    **Código SAS Original:**
    ```sas
    {sas_code}
    ```

    **Dados de Amostra Estruturados (JSON):**
    ```json
    {sample_data_json}
    ```

    **Código PySpark Completo Gerado:**
    ```python
    # -*- coding: utf-8 -*-
    # Script PySpark completo gerado por IA (v4.0)

    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    from pyspark.sql import SparkSession, Row
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    # Importar TODOS os tipos comuns é mais seguro
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType, LongType, FloatType
    from datetime import date, datetime
    import traceback

    spark = None
    try:
        # --- 1. Inicialização da SparkSession ---
        spark = SparkSession.builder \\
            .appName("GeneratedPySparkJobV4") \\
            .master("local[*]") \\
            .config("spark.sql.repl.eagerEval.enabled", True) \\
            .getOrCreate()
        print("SparkSession iniciada com sucesso.")

        # --- 2. Geração de DataFrames de Entrada (Usando ESTRUTURA JSON) ---
        # (O LLM deve gerar spark.createDataFrame(...) para CADA dataset no JSON)
        # Exemplo esperado (LLM deve gerar isso):
        # clientes_schema = StructType([...]) # Opcional, mas recomendado para tipos
        # clientes_data = [...]
        # clientes_df = spark.createDataFrame(clientes_data, schema=clientes_schema)
        # vendas_df = spark.createDataFrame(...)
        # produtos_detalhes_df = spark.createDataFrame(...)


        # --- 3. Lógica SAS Transpilada ---
        # (O código PySpark transpilado vai aqui)


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
        st.info("Enviando SAS e dados ESTRUTURADOS para o Agente 2...")
        response = client.chat.completions.create(
            model="gpt-4o", # Modelo capaz de seguir instruções complexas e gerar código
            messages=[
                {"role": "system", "content": "Você gera scripts PySpark completos a partir de SAS e uma ESTRUTURA JSON de dados de amostra, usando APENAS os dados fornecidos."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2, # Mais determinístico para usar a estrutura
            max_tokens=4000 # Aumentar token limit
        )
        pyspark_code_raw = response.choices[0].message.content

        # Limpeza do código (mesma lógica de antes)
        code_lines = pyspark_code_raw.split('\n')
        start_index = 0; end_index = len(code_lines)
        if code_lines and code_lines[0].strip().startswith("```python"): start_index = 1
        for i in range(len(code_lines) - 1, -1, -1):
             if code_lines[i].strip() == "```": end_index = i; break
        cleaned_lines = code_lines[start_index:end_index]
        first_code_line = 0
        for i, line in enumerate(cleaned_lines):
            if line.strip() and not line.strip().startswith('#'): first_code_line = i; break
        pyspark_code = "\n".join(cleaned_lines[first_code_line:]).strip()

        # Validações mínimas (menos cruciais agora, pois o prompt é mais direto)
        if "from pyspark.sql import SparkSession" not in pyspark_code:
             pyspark_code = "from pyspark.sql import SparkSession\n" + pyspark_code
        # Adicionar imports de tipos se o LLM esquecer (menos provável)
        if "from pyspark.sql.types" not in pyspark_code and "StructType" in pyspark_code:
             pyspark_code = "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType, LongType, FloatType\n" + pyspark_code
        if "from datetime" not in pyspark_code and ("date(" in pyspark_code or "datetime(" in pyspark_code):
             pyspark_code = "from datetime import date, datetime\n" + pyspark_code

        st.success("Código PySpark recebido do Agente 2.")
        return pyspark_code

    except Exception as e:
        st.error(f"Erro API (Agente 2 - Transpilação v4): {e}")
        return f"# Erro na transpilação (v4 - estrutura): {e}"


# --- agent_3_execute_pyspark - SEM ALTERAÇÕES ---
# (Cole a função agent_3_execute_pyspark aqui como estava antes)
# --- [INSERIR CÓDIGO DA FUNÇÃO agent_3_execute_pyspark DA v3.1 AQUI] ---
def agent_3_execute_pyspark(pyspark_code: str) -> tuple[str, str, int]:
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
    try: safe_encoding = 'utf-8'
    except Exception: safe_encoding = 'utf-8'

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix=".py", delete=False, encoding=safe_encoding, errors='replace') as tmp_file:
            tmp_file_path = tmp_file.name; tmp_file.write(pyspark_code); tmp_file.flush()
        st.info(f"Código PySpark salvo em: {tmp_file_path} (encoding: {safe_encoding})")
        st.info(f"Executando com: {spark_submit_executable}")
        spark_submit_cmd = [spark_submit_executable, tmp_file_path]; st.code(f"Comando: {' '.join(spark_submit_cmd)}", language="bash")
        run_env = os.environ.copy(); run_env['PYTHONIOENCODING'] = safe_encoding
        process = subprocess.run(spark_submit_cmd, capture_output=True, text=True, encoding=safe_encoding, errors='replace', timeout=300, env=run_env)
        stdout_str, stderr_str, return_code = process.stdout, process.stderr, process.returncode
        st.success(f"Subprocess concluído (Retorno: {return_code}).")
    except subprocess.TimeoutExpired as e: stderr_str = f"Erro: Timeout (300s).\n{getattr(e, 'stderr', '')}"; stdout_str = getattr(e, 'stdout', ''); st.error(stderr_str); return_code = -4
    except Exception as e: stderr_str = f"Erro inesperado no subprocess:\n{traceback.format_exc()}"; st.error(stderr_str); return_code = -3
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            try: os.remove(tmp_file_path)
            except Exception as e_clean: st.warning(f"Falha ao limpar {tmp_file_path}: {e_clean}")
    return stdout_str, stderr_str, return_code


# --- Interface Streamlit (UI) ---
st.set_page_config(layout="wide", page_title="SAS -> PySpark Transpiler v4.0")
st.title("🤖 Transpilador Assistido por IA: SAS para PySpark (v4.0)")
st.caption("Extrai/Gera Dados Estruturados -> Documenta (MD) -> Transpila (Usa Estrutura) -> Executa")

# --- Inicialização do Estado da Sessão ---
# Adicionar estado para guardar a estrutura de dados
if 'sas_code' not in st.session_state: st.session_state.sas_code = "/* Cole seu código SAS aqui */"
if 'markdown_documentation' not in st.session_state: st.session_state.markdown_documentation = "" # Para exibição
if 'sample_data_structure' not in st.session_state: st.session_state.sample_data_structure = {} # Para Agente 2
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
    if not st.session_state.spark_found: initialize_spark_environment()

# --- Abas da UI ---
tab1, tab2, tab3 = st.tabs([
    "1. SAS Input & Documentação",
    "2. Código PySpark Gerado",
    "3. Resultados da Execução"
])

with tab1:
    st.header("1. Código SAS")
    # (Usar o default_sas_code_v3 da versão anterior)
    default_sas_code_v4 = """
/* Exemplo SAS v4: Inline + Referência Externa */
DATA WORK.CLIENTES; INPUT ID_CLIENTE $ NOME $ CIDADE $ REGIAO $; DATALINES;
C01 Alice São Paulo Sudeste
C02 Bob Rio de Janeiro Sudeste
C03 Charlie Salvador Nordeste
C04 David Manaus Norte
; RUN;
DATA WORK.VENDAS; INPUT ID_VENDA $ ID_CLIENTE $ PRODUTO $ VALOR ; DATALINES;
V101 C01 Laptop 4500.00
V102 C02 Teclado 350.50
V103 C01 Monitor 1200.00
V104 C03 Mouse 80.00
V105 C01 Webcam 250.00
V106 C04 Cadeira 950.75
V107 C02 Monitor 1100.00
V108 C03 Impressora 600.00
; RUN;
/* Tabela externa WORK.PRODUTOS_DETALHES a ser gerada */
PROC SQL; CREATE TABLE WORK.RELATORIO_COMPLETO AS SELECT t1.NOME, t1.CIDADE, t2.PRODUTO, t2.VALOR, t3.CATEGORIA, t3.FORNECEDOR FROM WORK.CLIENTES t1 INNER JOIN WORK.VENDAS t2 ON t1.ID_CLIENTE = t2.ID_CLIENTE LEFT JOIN WORK.PRODUTOS_DETALHES t3 ON t2.PRODUTO = t3.NOME_PRODUTO WHERE t1.REGIAO = 'Sudeste' ORDER BY t1.NOME, t2.PRODUTO; QUIT;
PROC PRINT DATA=WORK.RELATORIO_COMPLETO NOOBS; TITLE "Relatório Completo Vendas Sudeste"; RUN;
/* Outro uso da tabela externa */
PROC MEANS DATA=WORK.PRODUTOS_DETALHES NWAY NOPRINT; CLASS CATEGORIA; VAR MARGEM_LUCRO; OUTPUT OUT=WORK.MARGENS_POR_CATEGORIA (DROP=_TYPE_ _FREQ_) MEAN=MARGEM_MEDIA; RUN;
PROC PRINT DATA=WORK.MARGENS_POR_CATEGORIA NOOBS; TITLE "Margem Média por Categoria"; RUN;
"""
    if 'sas_code' not in st.session_state or not st.session_state.sas_code or st.session_state.sas_code == "/* Cole seu código SAS aqui */":
         st.session_state.sas_code = default_sas_code_v4

    sas_input = st.text_area("Cole seu código SAS:", height=300, key="sas_code_input", value=st.session_state.sas_code)
    if sas_input != st.session_state.sas_code:
         st.session_state.sas_code = sas_input
         # Resetar estados dependentes
         st.session_state.agent1_run_complete = False; st.session_state.show_transpile_button = False
         st.session_state.pyspark_code = ""; st.session_state.execution_ran = False
         st.session_state.markdown_documentation = ""; st.session_state.sample_data_structure = {}
         st.rerun()

    # Botão Agente 1
    if st.button("📊 Analisar SAS, Extrair/Gerar Dados Estruturados e Documentar (Agente 1)", disabled=not st.session_state.sas_code.strip()):
        client = get_llm_client()
        if client:
            with st.spinner("🧠 Agente 1 Analisando, Processando Dados e Documentando..."):
                # Agente 1 agora retorna (markdown_para_exibicao, estrutura_de_dados)
                md_doc, structured_data = agent_1_orchestrator(client, st.session_state.sas_code)
                st.session_state.markdown_documentation = md_doc # Guarda MD para exibição
                st.session_state.sample_data_structure = structured_data # Guarda estrutura para Agente 2
                st.session_state.agent1_run_complete = True; st.session_state.show_transpile_button = True
                st.session_state.pyspark_code = ""; st.session_state.execution_ran = False;
                # Opcional: Exibir a estrutura de dados para debug
                # st.expander("Debug: Estrutura de Dados Gerada").json(st.session_state.sample_data_structure)
                st.rerun()

    # Exibição da Documentação Markdown
    if st.session_state.agent1_run_complete:
        st.header("2. Documentação Gerada (com Amostras para Visualização)")
        st.caption("A documentação Markdown abaixo é para leitura. Os dados para transpilação foram salvos separadamente.")
        # Permitir edição da documentação é menos útil agora, mas pode ser mantido
        edited_doc_input = st.text_area("Edite a documentação (Markdown - apenas para exibição):", value=st.session_state.markdown_documentation, height=300, key="doc_editor_display")
        if edited_doc_input != st.session_state.markdown_documentation:
            st.session_state.markdown_documentation = edited_doc_input # Salva apenas para exibição

        # Botão Agente 2 (usa a ESTRUTURA de dados guardada)
        if st.session_state.show_transpile_button:
            transpile_disabled = not st.session_state.sample_data_structure # Desabilita se não há dados
            transpile_help = "Transpilar usando os dados de amostra estruturados." if not transpile_disabled else "Execute o Agente 1 primeiro para gerar a estrutura de dados."
            if st.button("✨ Transpilar para PySpark (Agente 2 - Usando Dados Estruturados)", type="primary",
                         disabled=transpile_disabled, help=transpile_help):
                client = get_llm_client()
                if client:
                    with st.spinner("🔄 Agente 2 Transpilando para PySpark (usando dados estruturados)..."):
                        st.session_state.pyspark_code = agent_2_transpile_to_pyspark_from_structure(
                            client,
                            st.session_state.sas_code,
                            st.session_state.sample_data_structure # PASSA A ESTRUTURA
                        )
                        st.session_state.execution_ran = False; st.rerun() # Vai para a aba 2

    # Pré-visualização da Documentação na Aba 1
    st.markdown("---"); st.header("Pré-visualização da Documentação Markdown")
    doc_to_show = st.session_state.markdown_documentation
    if doc_to_show: st.markdown(doc_to_show, unsafe_allow_html=True)
    else: st.info("Documentação e tabelas de amostra (para visualização) aparecerão aqui após a análise.")
    # Opcional: Exibir a estrutura de dados aqui também
    if st.session_state.sample_data_structure:
        with st.expander("Ver Estrutura de Dados para Transpilação"):
             st.json(st.session_state.sample_data_structure)


# --- Aba 2: Código PySpark ---
with tab2:
    if st.session_state.pyspark_code:
        st.header("3. Código PySpark Gerado")
        st.caption("Código gerado usando SAS e a **estrutura de dados** de amostra (extraída/gerada).")
        st.code(st.session_state.pyspark_code, language="python", line_numbers=True)
        st.warning("⚠️ **Aviso:** Verifique se os DataFrames (`spark.createDataFrame`) usam os dados da estrutura corretamente. A execução é experimental.")
        run_button_disabled = not st.session_state.pyspark_code.strip() or not st.session_state.get('spark_found', False)
        button_tooltip = "Execução desabilitada: Verifique o código e o status do Spark." if run_button_disabled else "Tentar executar o script PySpark via spark-submit."
        if st.button("🚀 Tentar Executar PySpark (Agente 3)", disabled=run_button_disabled, help=button_tooltip):
            with st.spinner("⏳ Agente 3 tentando executar via spark-submit..."):
                stdout, stderr, retcode = agent_3_execute_pyspark(st.session_state.pyspark_code)
                st.session_state.execution_stdout, st.session_state.execution_stderr = stdout, stderr
                st.session_state.execution_return_code, st.session_state.execution_ran = retcode, True
                st.rerun()
    elif st.session_state.agent1_run_complete: st.info("Clique em 'Transpilar para PySpark (Agente 2)' na Aba 1.")
    else: st.info("Insira o código SAS e clique em 'Analisar...' na Aba 1 para começar.")

# --- Aba 3: Resultados da Execução ---
# (Sem alterações na lógica da Aba 3)
with tab3:
    st.header("4. Resultados da Execução (Agente 3)")
    if not st.session_state.execution_ran: st.info("Resultados da execução (stdout/stderr) do `spark-submit` aparecerão aqui.")
    else:
        ret_code = st.session_state.execution_return_code
        status_msg = f"Status: {'Sucesso ✅' if ret_code == 0 else 'Falha ❌'} (Código de Retorno: {ret_code})"
        if ret_code == 0: st.success(status_msg)
        else: st.error(status_msg)
        if ret_code != 0:
            error_map = { -2: "Falha Crítica: Comando 'spark-submit' não encontrado.", -3: "Falha Crítica: Erro interno no subprocess.", -4: "Falha: Timeout da execução.", -5: "Falha Crítica: SPARK_HOME não encontrado.", -6: f"Falha Crítica: spark-submit não encontrado no SPARK_HOME."}
            default_error = f"Processo spark-submit terminou com erro ({ret_code}). Verifique stderr."
            st.warning(error_map.get(ret_code, default_error))
        tab_stdout, tab_stderr = st.tabs(["Standard Output (stdout)", "Standard Error (stderr)"])
        with tab_stdout: st.caption("Saída padrão do processo `spark-submit`."); st.code(st.session_state.execution_stdout or "(Saída padrão vazia)", language="log")
        with tab_stderr: st.caption("Saída de erro do processo `spark-submit`."); st.code(st.session_state.execution_stderr or "(Saída de erro vazia)", language="log")


# --- Barra Lateral (Sidebar) ---
# (Sem alterações na lógica da Sidebar)
st.sidebar.title("Configuração e Status")
st.sidebar.markdown("---")
st.sidebar.subheader("Ambiente Spark")
if st.session_state.get('spark_found', False): st.sidebar.success(f"✅ Spark Encontrado!"); st.sidebar.caption(f"Método: {st.session_state.spark_found_method}"); st.sidebar.caption(f"SPARK_HOME: {st.session_state.spark_home_discovered}")
else: st.sidebar.error("❌ Spark Não Encontrado!"); st.sidebar.caption("Verifique a instalação do Spark ou defina SPARK_HOME.")
st.sidebar.markdown("---")
st.sidebar.subheader("Configuração API OpenAI")
st.sidebar.text_input("OpenAI API Key (Opcional)", type="password", key="api_key_input_widget", help="Use se não configurado via secrets/env var.", on_change=lambda: st.session_state.update(api_key_input=st.session_state.api_key_input_widget))
current_ui_key = st.session_state.get('api_key_input', None) or st.session_state.get('api_key_input_widget', None); final_api_key_source = ""
if api_key: final_api_key_source = "Configurada (secrets/env)"
elif current_ui_key: final_api_key_source = "Configurada (UI nesta sessão)"
else: final_api_key_source = "Não Configurada"
if final_api_key_source.startswith("Configurada"): st.sidebar.success(f"API Key: {final_api_key_source}")
else: st.sidebar.warning(f"API Key: {final_api_key_source}")
st.sidebar.markdown("---")
st.sidebar.title("⚠️ Avisos Importantes")
st.sidebar.warning("""*   **RISCO DE EXECUÇÃO:** Experimental. **REVISE CÓDIGO.**\n*   **ADAPTAÇÃO:** Código PySpark gerado **pode precisar de ajustes**.\n*   **PRECISÃO IA:** Documentação, inferência e geração/parsing de amostras não são perfeitas.\n*   **AMOSTRAGEM:** Foco em casos comuns. Geração automática é **fictícia**.\n*   **CUSTOS API:** Uso da API OpenAI pode gerar custos.""")
st.sidebar.info("Versão 4.0: Dados Estruturados para Transpilação.")