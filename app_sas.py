# -----------------------------------------------------------------------------
# SAS to PySpark Transpiler with Multi-Agent Assistance & findspark
#
# Funcionalidades:
# 1. Agente 1 (LLM): Interpreta SAS -> Documentação Markdown.
# 2. Editor Integrado: Edição da documentação.
# 3. Agente 2 (LLM): Transpila SAS -> PySpark (usa documentação editada).
# 4. findspark: Tenta localizar SPARK_HOME automaticamente.
# 5. Agente 3 (Subprocess): Tenta executar PySpark via 'spark-submit' usando o caminho descoberto.
#
# Pré-requisitos para Execução Completa (Agente 3):
# - Apache Spark instalado. findspark tentará localizá-lo.
#   Se findspark falhar, definir SPARK_HOME manualmente pode funcionar.
# - Permissões para executar subprocessos.
#
# Aviso de Segurança:
# A execução de código gerado (Agente 3) é inerentemente arriscada.
# Use com extrema cautela em ambientes controlados.
# O código PySpark gerado provavelmente precisará de adaptação manual.
# -----------------------------------------------------------------------------

import streamlit as st
import os
import subprocess
import tempfile
import platform
import traceback
import findspark # Para localizar Spark
import locale    # Para encoding
from openai import OpenAI
from dotenv import load_dotenv # Descomente se usar .env localmente

# --- Configuração e Carregamento da API Key ---

load_dotenv() # Descomente se usar .env localmente
api_key = os.environ.get("OPENAI_API_KEY")

# --- Funções Auxiliares (incluindo inicialização do Spark) ---

def initialize_spark_environment():
    """
    Tenta encontrar SPARK_HOME usando findspark ou a variável de ambiente.
    Atualiza o estado da sessão com o resultado.
    Retorna True se encontrado, False caso contrário.
    """
    # Não executa novamente se já foi encontrado na sessão
    if st.session_state.get('spark_found', False):
        return True

    spark_home_env = os.environ.get("SPARK_HOME")
    spark_path_found = None
    found_method = None

    st.write("Inicializando descoberta do Spark...") # Log no Streamlit app (visível durante dev)

    if spark_home_env and os.path.isdir(spark_home_env):
        spark_path_found = spark_home_env
        found_method = "Variável de Ambiente (SPARK_HOME)"
        try:
            findspark.init(spark_home_env)
            st.write(f"findspark inicializado com SPARK_HOME: {spark_home_env}")
        except Exception as e:
            st.warning(f"Aviso: SPARK_HOME encontrado ({spark_home_env}), mas findspark.init() falhou: {e}")
    else:
        try:
            st.write("SPARK_HOME não definido ou inválido. Tentando localizar com findspark...")
            findspark.init()
            spark_path_found = findspark.find()
            # Define SPARK_HOME no ambiente do processo atual após encontrar
            os.environ['SPARK_HOME'] = spark_path_found
            found_method = "findspark (Busca Automática)"
            st.write(f"Spark encontrado por findspark em: {spark_path_found}")
        except (ValueError, ImportError, Exception) as e:
            st.write(f"findspark não conseguiu localizar Spark: {e}")
            spark_path_found = None

    if spark_path_found:
        st.session_state.spark_home_discovered = spark_path_found
        st.session_state.spark_found_method = found_method
        st.session_state.spark_found = True
        st.write("Estado da sessão atualizado: Spark encontrado.")
        return True
    else:
        st.session_state.spark_home_discovered = None
        st.session_state.spark_found_method = "Nenhum"
        st.session_state.spark_found = False
        st.write("Estado da sessão atualizado: Spark não encontrado.")
        return False

def get_llm_client():
    """Inicializa e retorna o cliente LLM (OpenAI)."""
    ui_api_key = st.session_state.get('api_key_input', None)
    current_api_key = api_key or ui_api_key

    if not current_api_key:
        st.error("Erro: Chave da API OpenAI não configurada.")
        return None
    try:
        client = OpenAI(api_key=current_api_key)
        client.models.list() # Teste rápido
        return client
    except Exception as e:
        st.error(f"Erro ao inicializar/conectar ao cliente OpenAI: {e}")
        return None

# --- Funções dos Agentes (LLM e Subprocess) ---

def agent_1_generate_documentation(client: OpenAI, sas_code: str) -> str:
    """Simula Agente 1: Analisa SAS -> Documentação Markdown."""
    if not sas_code.strip(): return "*Insira o código SAS.*"
    if not client: return "*Erro: Cliente LLM não inicializado.*"
    # (Prompt e chamada da API como antes)
    prompt = f"""
    **Tarefa:** Analisar o código SAS e gerar documentação Markdown detalhada.
    Foco em: Objetivo de Negócio, Lógica Técnica, Entradas, Saídas, Detalhamento dos Passos.
    **Formato:** Markdown claro.
    **Código SAS:**
    ```sas
    {sas_code}
    ```
    **Documentação Gerada:**
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Especialista em análise SAS e documentação."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4, max_tokens=1500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        st.error(f"Erro API (Agente 1): {e}")
        return f"*Erro ao gerar documentação: {e}*"

def agent_2_transpile_to_pyspark(client: OpenAI, sas_code: str, documentation: str) -> str:
    """
    Simula Agente 2: Transpila SAS -> PySpark, gerando DataFrames de entrada
    fictícios E **incluindo a inicialização da SparkSession local**.
    """
    if not sas_code.strip() or not documentation.strip(): return "*Código SAS e documentação necessários.*"
    if not client: return "# Erro: Cliente LLM não inicializado."

    # --- PROMPT ATUALIZADO PARA INCLUIR SPARK SESSION ---
    prompt = f"""
    **Tarefa:** Você é um especialista em SAS e PySpark. Sua tarefa é gerar um script PySpark **completo e executável localmente**:
    1.  **Incluir Inicialização da SparkSession:** Comece o script gerando o código necessário para importar `SparkSession` e criar uma instância local chamada `spark`. Use `.appName("GeneratedPySparkJob").master("local[*]")`.
    2.  **Analisar Entradas:** Use o código SAS e a documentação para identificar as tabelas/datasets de ENTRADA.
    3.  **Gerar Dados de Entrada:** Crie DataFrames PySpark para essas entradas usando `spark.createDataFrame()`.
        *   Use nomes de variáveis baseados nos nomes SAS (ex: `customers`, `sales`).
        *   Infira esquema (nomes/tipos de colunas: StringType, IntegerType, DoubleType, DateType, etc.) do SAS/documentação. Importe de `pyspark.sql.types` e `datetime`.
        *   Gere 3-5 linhas de dados fictícios **plausíveis e funcionais**, garantindo que **chaves de relacionamento sejam consistentes** para joins.
        *   Coloque esta criação de dados *após* a inicialização da SparkSession.
    4.  **Transpilar Lógica SAS:** Converta a lógica SAS restante (DATA steps, PROC SQL, SORT, etc.) para a API de DataFrames PySpark, operando nos DataFrames criados.
    5.  **Mostrar/Indicar Saídas:** Use `dataframe_name.show()` para saídas de `PROC PRINT`. Comente `# Resultado final 'nome_df' pronto...` para tabelas finais.
    6.  **Incluir Encerramento (Opcional, mas bom):** Se possível, adicione `spark.stop()` no final do script, idealmente em um bloco `finally` para robustez, embora `spark-submit` geralmente gerencie o ciclo de vida.

    **Contexto:** A documentação é chave para estrutura de dados, tipos e relações.

    **Diretrizes:**
    *   Use as melhores práticas PySpark.
    *   Adicione comentários claros.
    *   Gere **APENAS o código PySpark completo e executável**, começando com imports e SparkSession, seguido pela criação de dados e lógica. Não inclua explicações externas nem ```python.

    **Código SAS Original:**
    ```sas
    {sas_code}
    ```

    **Documentação de Contexto (Markdown):**
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
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
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

        # --- 2. Geração de Dados de Entrada Fictícios ---
        # (O código spark.createDataFrame(...) gerado vai aqui)

        # --- 3. Lógica SAS Transpilada ---
        # (O código PySpark transpilado da lógica SAS vai aqui)

        print("\\nScript executado com sucesso (antes do finally).")

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
    """
    # --- FIM DO PROMPT ATUALIZADO ---

    try:
        response = client.chat.completions.create(
            model="gpt-4o", # Modelo capaz necessário
            messages=[
                {"role": "system", "content": "Você gera scripts PySpark completos e autossuficientes a partir de SAS, incluindo inicialização Spark e dados de teste."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=3800 # Aumentar ligeiramente para o código extra da sessão/finally
        )
        pyspark_code_raw = response.choices[0].message.content

        # --- Limpeza da resposta (similar a antes) ---
        code_lines = pyspark_code_raw.split('\n')
        start_index = 0
        end_index = len(code_lines)
        if code_lines[0].strip().startswith("```python"): start_index = 1
        for i in range(len(code_lines) - 1, -1, -1):
             if code_lines[i].strip() == "```": end_index = i; break
        cleaned_lines = [line for line in code_lines[start_index:end_index] if not line.strip().startswith("# ---")]
        first_code_line = 0
        for i, line in enumerate(cleaned_lines):
            if line.strip(): first_code_line = i; break
        pyspark_code = "\n".join(cleaned_lines[first_code_line:]).strip()

        # Validações/adições de import mínimas (o prompt agora pede a maioria)
        if "from pyspark.sql import SparkSession" not in pyspark_code:
             pyspark_code = "from pyspark.sql import SparkSession\n" + pyspark_code
        if "from pyspark.sql import functions as F" not in pyspark_code and " F." not in pyspark_code :
             pyspark_code = "from pyspark.sql import functions as F\n" + pyspark_code
        # Não adiciona mais imports de tipos ou datetime aqui, confia no LLM

        return pyspark_code

    except Exception as e:
        st.error(f"Erro API (Agente 2 - Script Completo): {e}")
        return f"# Erro na transpilação (script completo): {e}"

def agent_3_execute_pyspark(pyspark_code: str) -> tuple[str, str, int]:
    """
    Simula Agente 3: Tenta executar o script PySpark **autossuficiente**
    gerado pelo Agente 2 usando spark-submit.
    """
    if not pyspark_code.strip(): return "*Nenhum código PySpark para executar.*", "", -1

    if not st.session_state.get('spark_found', False) or not st.session_state.get('spark_home_discovered'):
        error_msg = "Erro Crítico: SPARK_HOME não encontrado."
        st.error(error_msg); return "", error_msg, -5

    spark_home = st.session_state.spark_home_discovered
    spark_submit_executable = os.path.join(spark_home, "bin", "spark-submit")
    is_windows = platform.system() == "Windows"
    if is_windows and not os.path.isfile(spark_submit_executable):
        spark_submit_cmd_path = spark_submit_executable + ".cmd"
        if os.path.isfile(spark_submit_cmd_path): spark_submit_executable = spark_submit_cmd_path
        else: error_msg = f"Erro: spark-submit(.cmd) não encontrado em '{os.path.join(spark_home, 'bin')}'." ; st.error(error_msg); return "", error_msg, -6
    elif not is_windows and not os.path.isfile(spark_submit_executable):
         error_msg = f"Erro: spark-submit não encontrado em '{os.path.join(spark_home, 'bin')}'." ; st.error(error_msg); return "", error_msg, -6

    stdout_str, stderr_str, return_code = "", "", -1
    tmp_file_path = None

    try: default_encoding = locale.getpreferredencoding(False)
    except (NameError, locale.Error): default_encoding = "cp1252" if is_windows else "utf-8"

    try:
        # --- SIMPLIFICADO: Escreve o código gerado DIRETAMENTE ---
        with tempfile.NamedTemporaryFile(mode='w', suffix=".py", delete=False, encoding=default_encoding, errors='replace') as tmp_file:
            tmp_file_path = tmp_file.name
            # Adiciona a linha de encoding no topo para garantir
            tmp_file.write(f"# -*- coding: {default_encoding} -*-\n")
            tmp_file.write(pyspark_code) # Escreve o código como recebido
            tmp_file.flush()
        # --- FIM DA SIMPLIFICAÇÃO ---

        st.info(f"Código PySpark autossuficiente salvo em: {tmp_file_path}")
        st.info(f"Executando com: {spark_submit_executable}")
        spark_submit_cmd = [spark_submit_executable, tmp_file_path]
        st.code(f"Comando: {' '.join(spark_submit_cmd)}", language="bash")

        process = subprocess.run(
            spark_submit_cmd, capture_output=True, text=True,
            encoding=default_encoding, errors='replace', timeout=300
        )
        stdout_str, stderr_str, return_code = process.stdout, process.stderr, process.returncode
        st.success(f"Subprocess concluído (Retorno: {return_code}).")

    except subprocess.TimeoutExpired as e:
        stderr_str = f"Erro: Timeout (300s).\n{getattr(e, 'stderr', '')}" # Tenta obter stderr do timeout
        stdout_str = getattr(e, 'stdout', '')
        st.error(stderr_str)
        return_code = -4
    except Exception as e:
        stderr_str = f"Erro inesperado no subprocess:\n{traceback.format_exc()}"
        st.error(stderr_str)
        return_code = -3
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            try: os.remove(tmp_file_path)
            except Exception as e_clean: st.warning(f"Falha ao limpar {tmp_file_path}: {e_clean}")

    return stdout_str, stderr_str, return_code

# --- Interface Gráfica com Streamlit ---

st.set_page_config(layout="wide", page_title="SAS -> PySpark Transpiler")
st.title("🤖 Transpilador Assistido por IA: SAS para PySpark")
st.caption("Interpretação -> Edição -> Transpilação -> Execução (com findspark)")

# --- Inicialização do Estado da Sessão ---
# (Estados para código, docs, flags, resultados de execução)
if 'sas_code' not in st.session_state: st.session_state.sas_code = "/* Cole seu código SAS aqui */"
if 'initial_documentation' not in st.session_state: st.session_state.initial_documentation = ""
if 'edited_documentation' not in st.session_state: st.session_state.edited_documentation = ""
if 'pyspark_code' not in st.session_state: st.session_state.pyspark_code = ""
if 'agent1_run_complete' not in st.session_state: st.session_state.agent1_run_complete = False
if 'show_transpile_button' not in st.session_state: st.session_state.show_transpile_button = False
if 'execution_stdout' not in st.session_state: st.session_state.execution_stdout = ""
if 'execution_stderr' not in st.session_state: st.session_state.execution_stderr = ""
if 'execution_ran' not in st.session_state: st.session_state.execution_ran = False
if 'execution_return_code' not in st.session_state: st.session_state.execution_return_code = None
if 'api_key_input' not in st.session_state: st.session_state.api_key_input = ""
# Estados para Spark (findspark)
if 'spark_found' not in st.session_state:
    st.session_state.spark_found = False
    st.session_state.spark_home_discovered = None
    st.session_state.spark_found_method = "Nenhum"
    # Tenta inicializar na primeira carga ou se ainda não foi encontrado
    if not st.session_state.spark_found:
        initialize_spark_environment()

# --- Layout Principal ---
col1, col2 = st.columns(2)

with col1:
    # --- Seção 1: SAS & Agente 1 ---
    st.header("1. Código SAS")
    sas_input = st.text_area("Cole seu código SAS:", height=250, key="sas_code_input", value=st.session_state.sas_code)
    if sas_input != st.session_state.sas_code:
         st.session_state.sas_code = sas_input
         st.session_state.agent1_run_complete = False; st.session_state.show_transpile_button = False
         st.session_state.pyspark_code = ""; st.session_state.execution_ran = False; st.rerun()

    if st.button("🔍 Analisar e Gerar Documentação (Agente 1)", disabled=not st.session_state.sas_code.strip()):
        client = get_llm_client()
        if client:
            with st.spinner("🧠 Agente 1 analisando SAS..."):
                st.session_state.initial_documentation = agent_1_generate_documentation(client, st.session_state.sas_code)
                st.session_state.edited_documentation = st.session_state.initial_documentation
                st.session_state.agent1_run_complete = True; st.session_state.show_transpile_button = True
                st.session_state.pyspark_code = ""; st.session_state.execution_ran = False; st.rerun()

    # --- Seção 2: Documentação & Agente 2 ---
    if st.session_state.agent1_run_complete:
        st.header("2. Documentação Gerada (Editável)")
        edited_doc_input = st.text_area("Edite a documentação (Markdown):", value=st.session_state.edited_documentation, height=250, key="doc_editor")
        if edited_doc_input != st.session_state.edited_documentation: st.session_state.edited_documentation = edited_doc_input

        if st.session_state.show_transpile_button:
            if st.button("✨ Transpilar para PySpark (Agente 2)", type="primary", disabled=not st.session_state.edited_documentation.strip()):
                client = get_llm_client()
                if client:
                    with st.spinner("🔄 Agente 2 transpilando para PySpark..."):
                        st.session_state.pyspark_code = agent_2_transpile_to_pyspark(client, st.session_state.sas_code, st.session_state.edited_documentation)
                        st.session_state.execution_ran = False; st.rerun()

    # --- Seção 3: PySpark & Agente 3 ---
    if st.session_state.pyspark_code:
        st.header("3. Código PySpark Gerado")
        st.code(st.session_state.pyspark_code, language="python", line_numbers=True)
        st.warning("⚠️ **Aviso:** Execução experimental e requer adaptação do código (leitura de dados).")

        run_button_disabled = not st.session_state.pyspark_code.strip() or not st.session_state.get('spark_found', False)
        button_tooltip = "Execução desabilitada: Spark (SPARK_HOME) não encontrado." if not st.session_state.get('spark_found', False) else None

        if st.button("🚀 Tentar Executar PySpark (Agente 3)", disabled=run_button_disabled, help=button_tooltip):
            with st.spinner("⏳ Agente 3 tentando executar via spark-submit..."):
                stdout, stderr, retcode = agent_3_execute_pyspark(st.session_state.pyspark_code)
                st.session_state.execution_stdout, st.session_state.execution_stderr = stdout, stderr
                st.session_state.execution_return_code, st.session_state.execution_ran = retcode, True
                st.rerun()

    elif st.session_state.agent1_run_complete:
        st.info("Clique em 'Transpilar para PySpark (Agente 2)'.")

    st.markdown("---"); st.header("Pré-visualização da Documentação")
    doc_to_show = st.session_state.edited_documentation or st.session_state.initial_documentation
    if doc_to_show: st.markdown(doc_to_show, unsafe_allow_html=True)
    else: st.info("Documentação aparecerá aqui.")

with col2:
    # --- Seção 4: Resultados da Execução ---
    st.header("4. Resultados da Execução (Agente 3)")
    if not st.session_state.execution_ran:
        st.info("Resultados da execução (stdout/stderr) aparecerão aqui.")
    else:
        ret_code = st.session_state.execution_return_code
        status_msg = f"Status: {'Sucesso ✅' if ret_code == 0 else 'Falha ❌'} (Retorno: {ret_code})"
        st.subheader(status_msg)

        if ret_code != 0: # Mostra mensagem de erro específica
            error_map = {
                -2: "Falha Crítica: Comando 'spark-submit' não encontrado no PATH (erro subprocess).",
                -3: "Falha Crítica: Erro interno ao tentar executar o subprocess Python.",
                -4: "Falha: A execução excedeu o tempo limite.",
                -5: "Falha Crítica: SPARK_HOME não encontrado (findspark/env var).",
                -6: f"Falha Crítica: spark-submit não encontrado em '{os.path.join(st.session_state.get('spark_home_discovered','?'), 'bin')}'."
            }
            default_error = f"Processo spark-submit terminou com erro ({ret_code}). Verifique stderr."
            st.error(error_map.get(ret_code, default_error))

        tab_stdout, tab_stderr = st.tabs(["Standard Output (stdout)", "Standard Error (stderr)"])
        with tab_stdout:
            st.caption("Saída padrão do processo spark-submit.")
            st.code(st.session_state.execution_stdout or "(Vazio)", language="log")
        with tab_stderr:
            st.caption("Saída de erro do processo spark-submit (logs Spark, tracebacks).")
            if st.session_state.execution_stderr.strip():
                st.code(st.session_state.execution_stderr, language="log")
            else: st.info("(Vazio)")

# --- Barra Lateral ---
st.sidebar.title("Configuração e Status")
st.sidebar.markdown("---")

# Status do Ambiente Spark (findspark)
st.sidebar.subheader("Ambiente Spark")
if st.session_state.get('spark_found', False):
    st.sidebar.success(f"✅ Spark Encontrado!")
    st.sidebar.caption(f"Método: {st.session_state.spark_found_method}")
    st.sidebar.caption(f"SPARK_HOME: {st.session_state.spark_home_discovered}")
else:
    st.sidebar.error("❌ Spark Não Encontrado!")
    st.sidebar.caption("Verifique a instalação do Spark ou defina SPARK_HOME.")
st.sidebar.markdown("---")

# Configuração da API Key
st.sidebar.subheader("Configuração API OpenAI")
st.sidebar.text_input("OpenAI API Key (Opcional)", type="password", key="api_key_input_widget",
                      help="Use se não configurado via secrets/env var.",
                      on_change=lambda: st.session_state.update(api_key_input=st.session_state.api_key_input_widget))
if st.session_state.api_key_input and not api_key:
     api_key = st.session_state.api_key_input # Usa como fallback
     st.sidebar.info("API Key da UI carregada para esta sessão.")
elif not api_key: st.sidebar.warning("API Key não configurada.")
else: st.sidebar.success("API Key configurada (secrets/env).")

st.sidebar.markdown("---")
st.sidebar.title("⚠️ Avisos Importantes")
st.sidebar.warning(
    """
    *   **RISCO DE EXECUÇÃO (AGENTE 3):** Experimental e potencialmente perigoso. **NÃO USE EM PRODUÇÃO SEM REVISÃO HUMANA.**
    *   **ADAPTAÇÃO DE CÓDIGO:** Código PySpark gerado **precisará de adaptações** (leitura/escrita de dados reais).
    *   **PRECISÃO IA:** Transpilação e documentação não são perfeitas. Revise.
    *   **CUSTOS API:** Uso da API OpenAI pode gerar custos.
    """
)
st.sidebar.info("Versão com findspark.")