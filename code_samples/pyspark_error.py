# -----------------------------------------------------------------------------
# SAS to PySpark Transpiler v3
# (Com extração de amostra inline E geração automática para datasets externos)
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
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("OPENAI_API_KEY")

# --- Funções initialize_spark_environment, get_llm_client ---
# (Cole as funções aqui como estavam antes - SEM ALTERAÇÕES)
def initialize_spark_environment():
    """
    Tenta encontrar SPARK_HOME usando findspark ou a variável de ambiente.
    Atualiza o estado da sessão com o resultado.
    Retorna True se encontrado, False caso contrário.
    """
    if st.session_state.get('spark_found', False): return True
    spark_home_env = os.environ.get("SPARK_HOME")
    spark_path_found = None; found_method = None
    # st.write("Inicializando descoberta do Spark...") # Mover para log se necessário
    if spark_home_env and os.path.isdir(spark_home_env):
        spark_path_found = spark_home_env; found_method = "Variável de Ambiente (SPARK_HOME)"
        try: findspark.init(spark_home_env); # st.write(f"findspark inicializado com SPARK_HOME: {spark_home_env}")
        except Exception as e: st.warning(f"Aviso: SPARK_HOME encontrado ({spark_home_env}), mas findspark.init() falhou: {e}")
    else:
        try:
            # st.write("SPARK_HOME não definido ou inválido. Tentando localizar com findspark...")
            findspark.init(); spark_path_found = findspark.find()
            os.environ['SPARK_HOME'] = spark_path_found # Define no ambiente atual
            found_method = "findspark (Busca Automática)"; # st.write(f"Spark encontrado por findspark em: {spark_path_found}")
        except (ValueError, ImportError, Exception) as e: # st.write(f"findspark não conseguiu localizar Spark: {e}");
             spark_path_found = None

    if spark_path_found:
        st.session_state.spark_home_discovered = spark_path_found; st.session_state.spark_found_method = found_method
        st.session_state.spark_found = True; # st.write("Estado da sessão atualizado: Spark encontrado.");
        return True
    else:
        st.session_state.spark_home_discovered = None; st.session_state.spark_found_method = "Nenhum"
        st.session_state.spark_found = False; # st.write("Estado da sessão atualizado: Spark não encontrado.");
        return False

def get_llm_client():
    """Inicializa e retorna o cliente LLM (OpenAI)."""
    ui_api_key = st.session_state.get('api_key_input', None) or st.session_state.get('api_key_input_widget', None)
    current_api_key = api_key or ui_api_key
    if not current_api_key: st.error("Erro: Chave da API OpenAI não configurada."); return None
    try: client = OpenAI(api_key=current_api_key); client.models.list(); return client
    except Exception as e: st.error(f"Erro ao inicializar/conectar ao cliente OpenAI: {e}"); return None
# --- Fim Funções Não Alteradas ---


def parse_input_statement(input_statement: str) -> dict:
    """
    Parse um INPUT statement SAS simplificado para nomes e tipos básicos.
    Retorna: {'col_name': 'type_hint', ...} onde type_hint é 'string' ou 'numeric'.
    """
    schema = {}
    # Remove especificadores de formato/informat complexos para simplificar
    # Tira ( informat ) ou ( informat informat ... )
    input_statement = re.sub(r'\(\s*[\w\$\.]+\s*(?:[\w\$\.\s]+)?\)', '', input_statement)
     # Tira @(start_expression) ou @start_position
    input_statement = re.sub(r'@\(.*?\)|@\d+', '', input_statement)
    # Tira +offset
    input_statement = re.sub(r'\+\d+', '', input_statement)
    # Tira / (ir para próxima linha) - pode complicar, tratar com cuidado
    input_statement = input_statement.replace('/', ' ') # Substitui por espaço
    # Tira ddd.dd ou dd.
    input_statement = re.sub(r'\s+\d+\.\d*', '', input_statement)
    # Tira $dd. or $
    input_statement = re.sub(r'\s+\$\d*\.?', '', input_statement)

    parts = input_statement.split()
    idx = 0
    while idx < len(parts):
        part = parts[idx]
        col_name = None
        col_type = 'numeric' # Default

        if part == '$': # Próxima variável é string
            idx += 1
            if idx < len(parts):
                col_name = parts[idx]
                col_type = 'string'
            else: break # Fim inesperado
        elif part.endswith('$'): # Var$
            col_name = part[:-1]
            col_type = 'string'
        else: # Assume numérico por padrão
             col_name = part
             col_type = 'numeric' # Será refinado na extração/geração se possível

        if col_name and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_name): # Valida nome básico
            schema[col_name] = col_type
        idx += 1
    return schema


def identify_datasets_and_schemas(sas_code: str) -> dict:
    """
    Identifica todos os datasets SAS mencionados e tenta inferir seus esquemas.
    Retorna um dicionário:
    {
        'dataset_name': {
            'source': 'inline' | 'infile' | 'proc_sql_ref' | 'proc_ref' | 'unknown',
            'schema': {'col': 'type_hint', ...}, # Schema inferido (pode ser vazio)
            'has_inline_data': True | False
        }, ...
    }
    """
    datasets = {}

    # 1. Datasets definidos com DATA step (inline ou infile)
    data_step_pattern = re.compile(
        r"DATA\s+(?:WORK\.)?(\w+)\s*;" # Captura nome do dataset
        r"(?:.*?INFILE\s+.*?;"      # Opcional: Bloco INFILE
           r".*?INPUT(.*?);"        # Captura INPUT após INFILE
           r"|.*?INPUT(.*?);"        # OU: Captura INPUT sem INFILE
           r".*?(?:DATALINES|CARDS)\s*;" # Requer DATALINES/CARDS
           r"(.*?);"                 # Captura dados inline
           r"|.*?INPUT(.*?);"        # OU: INPUT isolado (sem DATALINES/CARDS explícitos antes de RUN) - caso raro?
        r")"                         # Fim do grupo de INPUT/DATA
        r".*?RUN;",                  # Fim do DATA step
        re.IGNORECASE | re.DOTALL | re.VERBOSE
    )
    # Tentativa de Regex mais refinada para DATA step
    data_step_pattern_v2 = re.compile(
        r"DATA\s+(?:WORK\.)?(\w+)\s*;"  # Group 1: Dataset name
        r"(.*?)?"                      # Group 2: Content before INPUT (may contain INFILE)
        r"INPUT(.*?);"                 # Group 3: INPUT statement content
        r"(.*?)?"                      # Group 4: Content between INPUT and DATALINES/CARDS/RUN
        r"(?:(?:DATALINES|CARDS)\s*;\s*(.*?)\s*;)??" # Group 5: Optional DATALINES/CARDS content
        r"\s*RUN;",
        re.IGNORECASE | re.DOTALL
    )

    for match in data_step_pattern_v2.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        before_input = match.group(2) or ""
        input_content = match.group(3).strip() if match.group(3) else ""
        between_input_data = match.group(4) or ""
        inline_data_content = match.group(5).strip() if match.group(5) else ""

        has_infile = "INFILE" in before_input.upper() or "INFILE" in between_input_data.upper()
        has_inline = bool(inline_data_content)

        schema = parse_input_statement(input_content) if input_content else {}

        source = 'unknown'
        if has_inline: source = 'inline'
        elif has_infile: source = 'infile'
        elif schema: source = 'data_step_no_data' # INPUT sem dados inline/infile explícito?

        datasets[ds_name] = {
            'source': source,
            'schema': schema,
            'has_inline_data': has_inline,
            'inline_data_snippet': inline_data_content[:200] # Guarda um trecho dos dados inline
        }

    # 2. Datasets referenciados em PROC SQL (FROM/JOIN) - simplificado
    #    Nota: Isso pode pegar tabelas temporárias criadas no mesmo PROC SQL.
    #    Uma análise mais completa exigiria entender o fluxo do SQL.
    sql_ref_pattern = re.compile(r"(?:FROM|JOIN)\s+(?:WORK\.)?(\w+)", re.IGNORECASE)
    # Encontra todos os PROC SQL blocks
    proc_sql_pattern = re.compile(r"PROC\s+SQL;.*?QUIT;", re.IGNORECASE | re.DOTALL)
    # Encontra tabelas criadas (para evitar confundi-las com input externo)
    create_table_pattern = re.compile(r"CREATE\s+TABLE\s+(?:WORK\.)?(\w+)", re.IGNORECASE)

    for sql_match in proc_sql_pattern.finditer(sas_code):
        sql_block = sql_match.group(0)
        created_tables = {m.group(1).strip().upper() for m in create_table_pattern.finditer(sql_block)}

        for ref_match in sql_ref_pattern.finditer(sql_block):
            ds_name = ref_match.group(1).strip().upper()
            # Se não foi definido antes e não está sendo criado AGORA, assume referência
            if ds_name not in datasets and ds_name not in created_tables:
                 # Tentar inferir schema do SQL é complexo com regex, deixaremos para o LLM
                 datasets[ds_name] = {
                     'source': 'proc_sql_ref',
                     'schema': {}, # Schema a ser inferido/gerado pelo LLM
                     'has_inline_data': False
                 }

    # 3. Datasets referenciados em outros PROCs (PRINT, SORT, MEANS, etc.)
    proc_data_pattern = re.compile(r"PROC\s+\w+\s+DATA\s*=\s*(?:WORK\.)?(\w+)", re.IGNORECASE)
    for match in proc_data_pattern.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        if ds_name not in datasets:
             datasets[ds_name] = {
                 'source': 'proc_ref',
                 'schema': {}, # Schema a ser inferido/gerado pelo LLM
                 'has_inline_data': False
             }

    return datasets


def extract_and_format_inline_samples(sas_code: str, datasets_info: dict, max_sample_rows=5) -> tuple[str, dict]:
    """
    Extrai dados inline (DATALINES/CARDS) e formata como Markdown.
    Atualiza o schema no datasets_info se refinar tipos.
    Retorna (markdown_string, updated_datasets_info).
    """
    samples_markdown = ""
    updated_info = datasets_info.copy()

    for ds_name, info in datasets_info.items():
        if info['source'] == 'inline' and info.get('inline_data_snippet'):
            schema = info.get('schema', {})
            if not schema: continue # Precisa do schema do INPUT

            columns = list(schema.keys())
            datalines_content = info['inline_data_snippet'] # Usa o trecho guardado
            data_rows = []
            lines = datalines_content.split('\n')
            count = 0

            for line in lines:
                line = line.strip()
                if not line: continue
                if count >= max_sample_rows: break

                values = line.split() # Assume delimitado por espaço (simplificação)
                if len(values) == len(columns):
                    row_data = {}
                    for i, col_name in enumerate(columns):
                        val = values[i]
                        row_data[col_name] = val
                        # Refinar tipo numérico: se não puder converter para float, é string
                        if schema[col_name] == 'numeric':
                            try: float(val)
                            except ValueError: schema[col_name] = 'string' # Corrige para string
                    data_rows.append(row_data)
                    count += 1

            if data_rows:
                 # Atualiza o schema no dicionário principal
                 updated_info[ds_name]['schema'] = schema
                 # Formata Markdown
                 md_table = f"#### Amostra Extraída: `{ds_name}` (de DATALINES/CARDS)\n\n"
                 md_table += "| " + " | ".join(columns) + " |\n"
                 md_table += "|-" + "-|".join(['-' * max(3, len(h)) for h in columns]) + "-|\n"
                 # Linha de Tipos Inferidos (opcional, pode adicionar se quiser)
                 # types_str = [f"`{schema.get(h,'?')}`" for h in columns]
                 # md_table += "| " + " | ".join(types_str) + " |\n"
                 for row_dict in data_rows:
                     row_values = [str(row_dict.get(h, '')) for h in columns]
                     md_table += "| " + " | ".join(row_values) + " |\n"
                 samples_markdown += md_table + "\n"

    if samples_markdown:
        samples_markdown = "\n\n---\n\n### Dados de Amostra Extraídos (Inline)\n" + samples_markdown
        st.success(f"Amostra(s) de dados inline extraída(s).")
    else:
        st.info("Nenhuma amostra de dados inline (DATALINES/CARDS) encontrada ou extraível.")

    return samples_markdown, updated_info


def agent_1_generate_documentation_and_samples(client: OpenAI, sas_code: str) -> str:
    """
    Agente 1:
    1. Analisa SAS para identificar todos datasets e schemas básicos (inline/externo).
    2. Extrai amostras de dados inline (DATALINES/CARDS).
    3. Constrói um prompt para o LLM:
        - Pedir documentação descritiva.
        - Fornecer a lista de datasets identificados (com seus schemas inferidos).
        - Pedir para GERAR dados de amostra para datasets SEM dados inline.
    4. Retorna a documentação completa gerada pelo LLM (descrição + amostras extraídas/geradas).
    """
    if not sas_code.strip(): return "*Insira o código SAS.*"
    if not client: return "*Erro: Cliente LLM não inicializado.*"

    # 1. Identificar todos datasets e schemas básicos
    datasets_info = {}
    try:
        datasets_info = identify_datasets_and_schemas(sas_code)
    except Exception as e:
        st.warning(f"Erro na identificação inicial de datasets: {e}")
        traceback.print_exc() # Log detalhado para depuração

    # 2. Extrair e formatar amostras INLINE
    inline_samples_md, datasets_info = extract_and_format_inline_samples(sas_code, datasets_info)

    # 3. Preparar informações para o prompt do LLM
    datasets_to_generate = []
    datasets_summary_for_prompt = ""
    if datasets_info:
        datasets_summary_for_prompt = "\n**Contexto dos Datasets Identificados:**\n"
        for name, info in datasets_info.items():
            schema_str = ", ".join([f"{col} ({typ})" for col, typ in info['schema'].items()]) if info.get('schema') else "Schema não inferido"
            source_desc = info.get('source', 'unknown source')
            has_inline_str = "Sim" if info.get('has_inline_data') else "Não"
            datasets_summary_for_prompt += f"- `{name}`: Fonte={source_desc}, Dados Inline={has_inline_str}, Schema Inferido=[{schema_str}]\n"
            if not info.get('has_inline_data'):
                datasets_to_generate.append({'name': name, 'schema': info.get('schema', {})}) # Passa schema inferido se houver

    # 4. Construir o prompt final para o Agente 1 (LLM)
    prompt_agent1 = f"""
    **Sua Tarefa Principal:** Analisar o código SAS fornecido e gerar documentação técnica abrangente em Markdown.

    **Instruções Detalhadas:**
    1.  **Documentação Descritiva:** Descreva o objetivo de negócio (se inferível), a lógica técnica passo a passo, as entradas e saídas principais do processo SAS.
    2.  **Análise de Datasets (Use o Contexto Fornecido):** Considere a lista de datasets identificados abaixo. Mencione-os na sua documentação, especialmente suas fontes e propósitos.
    3.  **GERAÇÃO DE AMOSTRAS (Ação Necessária):** Para cada dataset listado em "Datasets para Gerar Amostra" abaixo (aqueles SEM dados inline no SAS original), você DEVE:
        *   **Inferir/Refinar Schema:** Use o schema básico fornecido (se houver) e o contexto do código SAS (como a variável é usada em PROCs, WHERE, JOINs) para refinar os tipos de dados (ex: String, Integer, Double, Date). Se nenhum schema foi inferido, crie um schema plausível baseado no nome do dataset e no código.
        *   **Gerar Dados Fictícios:** Crie **3 a 5 linhas** de dados de amostra **plausíveis e consistentes**.
            *   Se houver chaves de join entre datasets gerados (ex: ID_CLIENTE em CLIENTES e VENDAS), **garanta que os valores das chaves correspondam** em algumas linhas para permitir joins funcionais.
            *   Use formatos razoáveis para tipos comuns (ex: 'YYYY-MM-DD' para datas).
        *   **Formatar como Markdown:** Apresente cada conjunto de dados gerado como uma tabela Markdown clara, sob um cabeçalho como "#### Amostra Gerada Automaticamente: `nome_dataset`". Inclua os nomes das colunas no cabeçalho da tabela.
    4.  **Estrutura Final:** Organize a saída Markdown de forma lógica: Descrição primeiro, depois as tabelas de amostra geradas. Se houver amostras extraídas (fornecidas separadamente), elas devem vir antes das geradas.

    **Código SAS para Análise:**
    ```sas
    {sas_code}
    ```

    {datasets_summary_for_prompt}

    **Datasets para Gerar Amostra (Se houver):**
    {chr(10).join([f"- `{d['name']}` (Schema Básico: {d['schema'] if d['schema'] else 'Nenhum'})" for d in datasets_to_generate]) if datasets_to_generate else "Nenhum dataset externo identificado para geração de amostra."}

    ---
    **Documentação Completa e Amostras Geradas (Markdown):**
    """

    # 5. Chamar o LLM
    final_documentation = "*Erro ao chamar LLM para documentação e geração de amostra.*"
    try:
        st.info(f"Solicitando ao LLM para documentar e gerar amostras para: {', '.join([d['name'] for d in datasets_to_generate]) if datasets_to_generate else 'Nenhum dataset externo.'}")
        response = client.chat.completions.create(
            model="gpt-4o", # Usar um modelo capaz para geração + documentação
            messages=[
                {"role": "system", "content": "Você é um especialista em SAS que documenta código e gera dados de amostra plausíveis para datasets externos referenciados."},
                {"role": "user", "content": prompt_agent1}
            ],
            temperature=0.5, # Um pouco mais de criatividade para geração de dados
            max_tokens=2500 # Aumentar tokens para incluir descrição e tabelas geradas
        )
        llm_generated_content = response.choices[0].message.content.strip()

        # Combina descrição/amostras geradas pelo LLM com amostras extraídas inline
        final_documentation = llm_generated_content + "\n" + inline_samples_md

    except Exception as e:
        st.error(f"Erro API (Agente 1 - Doc + Geração Amostra): {e}")
        final_documentation = f"*Erro ao gerar documentação/amostra: {e}*\n\n{inline_samples_md}" # Inclui inline mesmo em erro

    return final_documentation


# --- agent_2_transpile_to_pyspark ---
# (Prompt ligeiramente ajustado para reforçar o uso de TODAS as amostras)
def agent_2_transpile_to_pyspark(client: OpenAI, sas_code: str, documentation: str) -> str:
    """
    Agente 2: Transpila SAS -> PySpark, usando a documentação (que agora inclui
    amostras EXTRAÍDAS e GERADAS) para guiar a criação de DataFrames de entrada.
    """
    if not sas_code.strip() or not documentation.strip(): return "*Código SAS e documentação necessários.*"
    if not client: return "# Erro: Cliente LLM não inicializado."

    prompt = f"""
    **Tarefa:** Você é um especialista em SAS e PySpark. Gere um script PySpark **completo e executável localmente**:
    1.  **Inicialização SparkSession:** Inclua imports e criação da `SparkSession` local (`spark`).
    2.  **Analisar Entradas:** Identifique os datasets SAS de entrada a partir do código e da documentação.
    3.  **Gerar DataFrames de Entrada (Crucial):**
        *   **USE AS AMOSTRAS DA DOCUMENTAÇÃO:** Procure por **TODAS** as tabelas Markdown na seção "Dados de Amostra" (extraídas ou geradas) da documentação.
        *   Para **CADA** tabela de amostra encontrada:
            *   Crie um DataFrame PySpark usando `spark.createDataFrame()`.
            *   Use os **nomes das colunas** e os **dados das linhas** da tabela Markdown.
            *   **Infira os tipos de dados PySpark** (`StringType`, `IntegerType`, `DoubleType`, `DateType`, etc.) a partir dos valores na amostra e/ou do schema mencionado na documentação. Importe os tipos de `pyspark.sql.types` e `datetime`. Use `StringType` como padrão seguro se incerto. Tente `IntegerType`/`DoubleType` para números. Use `DateType` para 'YYYY-MM-DD', senão `StringType`.
            *   Use nomes de variáveis baseados nos nomes dos datasets SAS (ex: `clientes_df`, `vendas_externas_df`).
        *   **Fallback (Raro):** Se, por algum motivo, um dataset de entrada óbvio do SAS não tiver uma amostra na documentação, gere 3-5 linhas de dados fictícios plausíveis, inferindo o schema do código SAS.
        *   Coloque a criação dos DataFrames *após* a inicialização da SparkSession.
    4.  **Transpilar Lógica SAS:** Converta a lógica SAS (DATA steps, PROC SQL, SORT, etc.) para PySpark (operações de DataFrame), usando os DataFrames criados no passo anterior.
    5.  **Saídas:** Use `dataframe_name.show()` para `PROC PRINT`. Comente para indicar DataFrames finais.
    6.  **Encerramento:** Inclua `spark.stop()` (idealmente em `finally`).

    **Contexto:** A documentação fornecida é a fonte primária para os dados de entrada via `spark.createDataFrame`. Use TODAS as amostras presentes.

    **Diretrizes:** Código PySpark completo, executável, com comentários. **APENAS o código.**

    **Código SAS Original:**
    ```sas
    {sas_code}
    ```

    **Documentação de Contexto (COM AMOSTRAS EXTRAÍDAS E/OU GERADAS):**
    ```markdown
    {documentation}
    ```

    **Código PySpark Completo Gerado:**
    ```python
    # -*- coding: utf-8 -*-
    # Script PySpark completo gerado por IA (v3)

    import warnings
    # (warnings filter)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)


    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType
    from datetime import date, datetime
    import traceback

    spark = None
    try:
        # --- 1. Inicialização da SparkSession ---
        spark = SparkSession.builder \\
            .appName("GeneratedPySparkJobV3") \\
            .master("local[*]") \\
            .config("spark.sql.repl.eagerEval.enabled", True) \\
            .getOrCreate()
        print("SparkSession iniciada com sucesso.")

        # --- 2. Geração de DataFrames de Entrada (Usando amostras da documentação) ---
        # (O LLM deve gerar spark.createDataFrame(...) para CADA amostra na documentação)


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
        response = client.chat.completions.create(
            model="gpt-4o", # Modelo capaz
            messages=[
                {"role": "system", "content": "Você gera scripts PySpark completos e autossuficientes a partir de SAS e documentação detalhada, criando DataFrames a partir de TODAS as amostras de dados fornecidas na documentação."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3, # Mais determinístico para seguir instruções
            max_tokens=3900 # Pode precisar de mais espaço
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

        # Validações mínimas
        if "from pyspark.sql import SparkSession" not in pyspark_code:
             pyspark_code = "from pyspark.sql import SparkSession\n" + pyspark_code
        # Garantir imports de tipos comuns se o LLM esquecer (menos provável agora)
        basic_types = ["StringType", "IntegerType", "DoubleType", "DateType"]
        if "from pyspark.sql.types" not in pyspark_code and any(t in pyspark_code for t in basic_types):
             pyspark_code = "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType\n" + pyspark_code
        if "from datetime" not in pyspark_code and ("date(" in pyspark_code or "datetime(" in pyspark_code):
             pyspark_code = "from datetime import date, datetime\n" + pyspark_code


        return pyspark_code

    except Exception as e:
        st.error(f"Erro API (Agente 2 - Transpilação v3): {e}")
        return f"# Erro na transpilação (script completo v3): {e}"


# --- agent_3_execute_pyspark - SEM ALTERAÇÕES ---
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
        # Usar utf-8 explicitamente pode ser mais seguro
        safe_encoding = 'utf-8'
        with tempfile.NamedTemporaryFile(mode='w', suffix=".py", delete=False, encoding=safe_encoding, errors='replace') as tmp_file:
            tmp_file_path = tmp_file.name
            # tmp_file.write(f"# -*- coding: {safe_encoding} -*-\n") # Boa prática, mas spark-submit pode ignorar
            tmp_file.write(pyspark_code)
            tmp_file.flush()
        st.info(f"Código PySpark autossuficiente salvo em: {tmp_file_path} (encoding: {safe_encoding})")
        st.info(f"Executando com: {spark_submit_executable}")
        spark_submit_cmd = [spark_submit_executable, tmp_file_path]
        st.code(f"Comando: {' '.join(spark_submit_cmd)}", language="bash")

        # Definir PYTHONIOENCODING pode ajudar com problemas de codificação no subprocesso
        run_env = os.environ.copy()
        run_env['PYTHONIOENCODING'] = safe_encoding

        process = subprocess.run(
            spark_submit_cmd, capture_output=True, text=True,
            encoding=safe_encoding, errors='replace', timeout=300, # Timeout 5 min
            env=run_env # Passa o ambiente modificado
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
# --- Fim Funções Não Alteradas ---


# --- Interface Streamlit (UI) ---
st.set_page_config(layout="wide", page_title="SAS -> PySpark Transpiler v3")
st.title("🤖 Transpilador Assistido por IA: SAS para PySpark (v3)")
st.caption("Identifica/Extrai/Gera Amostras -> Documenta -> Transpila -> Executa")

# Inicialização do Estado da Sessão (sem mudanças nos nomes das chaves)
# ... (manter a inicialização do session_state como na v2) ...
if 'sas_code' not in st.session_state: st.session_state.sas_code = "/* Cole seu código SAS aqui */"
if 'initial_documentation' not in st.session_state: st.session_state.initial_documentation = ""
if 'edited_documentation' not in st.session_state: st.session_state.edited_documentation = ""
# 'sample_markdown' não é mais explicitamente necessário, pois tudo é gerenciado pelo Agente 1
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
    # Exemplo SAS que mistura inline e referência externa (PROC SQL)
    default_sas_code_v3 = """
/* Exemplo SAS v3: Inline + Referência Externa */

/* Dataset de Clientes (Inline) */
DATA WORK.CLIENTES;
    INPUT ID_CLIENTE $ NOME $ CIDADE $ REGIAO $;
    DATALINES;
C01 Alice São Paulo Sudeste
C02 Bob Rio de Janeiro Sudeste
C03 Charlie Salvador Nordeste
C04 David Manaus Norte
;
RUN;

/* Dataset de Vendas (Inline) */
DATA WORK.VENDAS;
    INPUT ID_VENDA $ ID_CLIENTE $ PRODUTO $ VALOR ;
    DATALINES;
V101 C01 Laptop 4500.00
V102 C02 Teclado 350.50
V103 C01 Monitor 1200.00
V104 C03 Mouse 80.00
V105 C01 Webcam 250.00
V106 C04 Cadeira 950.75
V107 C02 Monitor 1100.00
V108 C03 Impressora 600.00
;
RUN;

/* Supõe que WORK.PRODUTOS_DETALHES existe externamente */
/* O sistema deve gerar uma amostra para esta tabela */
PROC SQL;
    CREATE TABLE WORK.RELATORIO_COMPLETO AS
    SELECT
        t1.NOME,
        t1.CIDADE,
        t2.PRODUTO,
        t2.VALOR,
        t3.CATEGORIA,  /* Coluna de PRODUTOS_DETALHES */
        t3.FORNECEDOR  /* Coluna de PRODUTOS_DETALHES */
    FROM
        WORK.CLIENTES t1
    INNER JOIN
        WORK.VENDAS t2 ON t1.ID_CLIENTE = t2.ID_CLIENTE
    LEFT JOIN
        WORK.PRODUTOS_DETALHES t3 ON t2.PRODUTO = t3.NOME_PRODUTO /* Junta com tabela externa */
    WHERE
        t1.REGIAO = 'Sudeste'
    ORDER BY
        t1.NOME, t2.PRODUTO;
QUIT;

/* Imprime o resultado final que depende da tabela externa */
PROC PRINT DATA=WORK.RELATORIO_COMPLETO NOOBS;
    TITLE "Relatório Completo de Vendas (Sudeste) com Detalhes do Produto";
RUN;

/* Usa a tabela externa em outro PROC */
PROC MEANS DATA=WORK.PRODUTOS_DETALHES NWAY NOPRINT;
    CLASS CATEGORIA;
    VAR MARGEM_LUCRO; /* Supõe que esta coluna existe em PRODUTOS_DETALHES */
    OUTPUT OUT=WORK.MARGENS_POR_CATEGORIA (DROP=_TYPE_ _FREQ_) MEAN=MARGEM_MEDIA;
RUN;

PROC PRINT DATA=WORK.MARGENS_POR_CATEGORIA NOOBS;
    TITLE "Margem Média por Categoria (da Tabela Externa)";
RUN;

"""
    # Define o código SAS no estado da sessão se for a primeira vez ou se estiver vazio
    if 'sas_code' not in st.session_state or not st.session_state.sas_code or st.session_state.sas_code == "/* Cole seu código SAS aqui */":
         st.session_state.sas_code = default_sas_code_v3

    sas_input = st.text_area("Cole seu código SAS:", height=350, key="sas_code_input", value=st.session_state.sas_code)
    if sas_input != st.session_state.sas_code:
         st.session_state.sas_code = sas_input
         # Resetar estados dependentes
         st.session_state.agent1_run_complete = False; st.session_state.show_transpile_button = False
         st.session_state.pyspark_code = ""; st.session_state.execution_ran = False
         st.session_state.initial_documentation = ""; st.session_state.edited_documentation = ""
         st.rerun()

    # Botão do Agente 1
    if st.button("📄 Analisar, Extrair/Gerar Amostras e Documentar (Agente 1)", disabled=not st.session_state.sas_code.strip()):
        client = get_llm_client()
        if client:
            with st.spinner("🧠 Agente 1 Analisando SAS, Extraindo/Gerando Amostras e Documentando..."):
                # Agente 1 agora faz todo o trabalho de documentação e amostragem
                full_documentation = agent_1_generate_documentation_and_samples(client, st.session_state.sas_code)
                st.session_state.initial_documentation = full_documentation
                st.session_state.edited_documentation = full_documentation # Inicia edição com a doc completa
                st.session_state.agent1_run_complete = True; st.session_state.show_transpile_button = True
                st.session_state.pyspark_code = ""; st.session_state.execution_ran = False; st.rerun()

    # Área de Edição e Botão do Agente 2
    if st.session_state.agent1_run_complete:
        st.header("2. Documentação Gerada (Editável)")
        st.caption("A documentação inclui descrição, amostras extraídas (inline) e/ou amostras geradas (externas). Edite se necessário.")
        edited_doc_input = st.text_area("Edite a documentação (Markdown):", value=st.session_state.edited_documentation, height=350, key="doc_editor")
        if edited_doc_input != st.session_state.edited_documentation:
            st.session_state.edited_documentation = edited_doc_input # Salva edição

        # Botão para transpilar
        if st.session_state.show_transpile_button:
            if st.button("✨ Transpilar para PySpark (Agente 2 - Usando Doc/Amostras)", type="primary", disabled=not st.session_state.edited_documentation.strip()):
                client = get_llm_client()
                if client:
                    with st.spinner("🔄 Agente 2 Transpilando para PySpark (usando todas as amostras)..."):
                        st.session_state.pyspark_code = agent_2_transpile_to_pyspark(client, st.session_state.sas_code, st.session_state.edited_documentation)
                        st.session_state.execution_ran = False; st.rerun() # Vai para a aba 2

    # Pré-visualização da Documentação na Aba 1
    st.markdown("---"); st.header("Pré-visualização da Documentação Editada")
    doc_to_show = st.session_state.edited_documentation # Mostra sempre a versão editável
    if doc_to_show: st.markdown(doc_to_show, unsafe_allow_html=True)
    elif st.session_state.initial_documentation: st.markdown(st.session_state.initial_documentation, unsafe_allow_html=True)
    else: st.info("Documentação e amostras aparecerão aqui após a análise.")


# --- Aba 2: Código PySpark ---
with tab2:
    if st.session_state.pyspark_code:
        st.header("3. Código PySpark Gerado")
        st.caption("Código gerado usando SAS e documentação (com amostras extraídas/geradas).")
        st.code(st.session_state.pyspark_code, language="python", line_numbers=True)
        st.warning("⚠️ **Aviso:** Verifique se os DataFrames de entrada (`spark.createDataFrame`) foram gerados corretamente a partir das amostras. A execução ainda é experimental.")

        run_button_disabled = not st.session_state.pyspark_code.strip() or not st.session_state.get('spark_found', False)
        button_tooltip = "Execução desabilitada: Verifique o código e o status do Spark." if run_button_disabled else "Tentar executar o script PySpark via spark-submit."

        if st.button("🚀 Tentar Executar PySpark (Agente 3)", disabled=run_button_disabled, help=button_tooltip):
            with st.spinner("⏳ Agente 3 tentando executar via spark-submit..."):
                stdout, stderr, retcode = agent_3_execute_pyspark(st.session_state.pyspark_code)
                st.session_state.execution_stdout, st.session_state.execution_stderr = stdout, stderr
                st.session_state.execution_return_code, st.session_state.execution_ran = retcode, True
                st.rerun() # Vai para a aba 3

    elif st.session_state.agent1_run_complete:
        st.info("Clique em 'Transpilar para PySpark (Agente 2)' na Aba 1.")
    else:
        st.info("Insira o código SAS e clique em 'Analisar...' na Aba 1 para começar.")


# --- Aba 3: Resultados da Execução ---
# (Sem alterações na lógica da Aba 3)
with tab3:
    st.header("4. Resultados da Execução (Agente 3)")
    if not st.session_state.execution_ran:
        st.info("Resultados da execução (stdout/stderr) do `spark-submit` aparecerão aqui.")
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
        with tab_stdout:
            st.caption("Saída padrão do processo `spark-submit`.")
            st.code(st.session_state.execution_stdout or "(Saída padrão vazia)", language="log")
        with tab_stderr:
            st.caption("Saída de erro do processo `spark-submit` (logs Spark, tracebacks).")
            if st.session_state.execution_stderr.strip(): st.code(st.session_state.execution_stderr, language="log")
            else: st.info("(Saída de erro vazia)")

# --- Barra Lateral (Sidebar) ---
# (Sem alterações na lógica da Sidebar, apenas textos atualizados)
st.sidebar.title("Configuração e Status")
st.sidebar.markdown("---")
st.sidebar.subheader("Ambiente Spark")
if st.session_state.get('spark_found', False):
    st.sidebar.success(f"✅ Spark Encontrado!")
    st.sidebar.caption(f"Método: {st.session_state.spark_found_method}")
    st.sidebar.caption(f"SPARK_HOME: {st.session_state.spark_home_discovered}")
else:
    st.sidebar.error("❌ Spark Não Encontrado!")
    st.sidebar.caption("Verifique a instalação do Spark ou defina SPARK_HOME.")
st.sidebar.markdown("---")
st.sidebar.subheader("Configuração API OpenAI")
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
    *   **RISCO DE EXECUÇÃO:** Experimental. **REVISE CÓDIGO ANTES DE USAR.**
    *   **ADAPTAÇÃO:** Código PySpark gerado **pode precisar de ajustes** (tipos, lógica).
    *   **PRECISÃO IA:** Documentação, inferência e geração de amostras não são perfeitas. Revise.
    *   **AMOSTRAGEM:** Extração/Geração focada em casos comuns. Pode falhar em SAS complexo. A geração automática é **fictícia**.
    *   **CUSTOS API:** Uso da API OpenAI pode gerar custos.
    """
)
st.sidebar.info("Versão com geração automática de amostras.")