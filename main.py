import ast
import html
import io
import os
import queue
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
import streamlit.components.v1 as components
from openai import OpenAI
from streamlit_ace import st_ace

# --- Configurações Iniciais ---
# Ignorar avisos comuns (opcional, mas útil)
# import warnings
# warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- Configuração do PySpark ---
# Garante que o PySpark use o mesmo Python que o Streamlit
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- Constantes ---
SCRIPT_FILENAME_TEMPLATE = "user_pyspark_script_{}_{}.py" # Adiciona ID da run + tentativa
MAX_ATTEMPTS_DEFAULT = 3
LLM_MODEL_DEFAULT = "gpt-4o-mini"
# Nome do arquivo/diretório que o código de exemplo espera para entrada PARQUET (se usado)
EXPECTED_INITIAL_INPUT_NAME = "dados_entrada_raw.parquet"


def generate_base_dynamic_dir(base_dir_prefix="temp_pyspark_st_run_") -> Path:
    """
    Gera um diretório base único e absoluto para a execução no diretório temp do sistema.
    """
    run_id = str(uuid.uuid4())[:8]
    base_path = Path(tempfile.gettempdir()) / f"{base_dir_prefix}{run_id}"
    base_path.mkdir(parents=True, exist_ok=True)
    st.write(f"✓ Diretório Base da Execução (em {tempfile.gettempdir()}): `{base_path}`")
    return base_path

class ParquetPathRewriter(ast.NodeTransformer):
    """
    Percorre uma AST Python e reescreve os argumentos de string literal
    em chamadas a '.parquet()' para apontar para dentro de um diretório base,
    preservando o nome original do arquivo/diretório.
    """
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.rewritten_paths: Dict[str, str] = {} # Rastreia: original -> novo_completo
        self.identified_inputs: List[str] = [] # Rastreia inputs originais lidos

    def visit_Call(self, node: ast.Call) -> ast.AST:
        """Visita nós de chamada de função/método."""
        # Verifica se é uma chamada de método (tem um '.') e se o nome é 'parquet'
        is_parquet_call = isinstance(node.func, ast.Attribute) and node.func.attr == 'parquet'
        if not is_parquet_call:
            return self.generic_visit(node) # Continua se não for .parquet()

        # Determina se é uma leitura (spark.read.parquet) ou escrita (df.write...parquet)
        is_read = False
        is_write = False
        func_expr = node.func
        call_chain = []
        while isinstance(func_expr, ast.Attribute):
            call_chain.insert(0, func_expr.attr)
            func_expr = func_expr.value
        if isinstance(func_expr, ast.Name):
             call_chain.insert(0, func_expr.id)

        if 'read' in call_chain: is_read = True
        if 'write' in call_chain: is_write = True

        # Encontra o argumento do caminho (string literal)
        path_arg_node: Optional[ast.Constant] = None
        arg_index: Optional[int] = None
        is_keyword = False

        # 1. Primeiro argumento posicional
        if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            path_arg_node = node.args[0]
            arg_index = 0
        # 2. Argumento nomeado 'path'
        else:
            for i, kw in enumerate(node.keywords):
                if kw.arg == 'path' and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    path_arg_node = kw.value
                    arg_index = i
                    is_keyword = True
                    break

        if path_arg_node is not None and arg_index is not None:
            original_path_str = path_arg_node.value
            # Ignora caminhos absolutos ou já reescritos
            if not Path(original_path_str).is_absolute() and not original_path_str.startswith(str(self.base_path)):
                relative_original_path = Path(original_path_str)
                new_path = self.base_path / relative_original_path
                new_path_str = str(new_path)

                self.rewritten_paths[original_path_str] = new_path_str
                if is_read:
                    self.identified_inputs.append(original_path_str)

                # Cria o novo nó AST
                new_const_node = ast.Constant(value=new_path_str)
                ast.copy_location(new_const_node, path_arg_node)

                # Substitui na AST
                if is_keyword:
                    node.keywords[arg_index].value = new_const_node
                else:
                    node.args[arg_index] = new_const_node

        # Continua visitando
        return self.generic_visit(node)

# Mesmo que o default_code não use, mantemos para robustez caso o usuário
# cole código que use Parquet ou o LLM gere código que use.
def prepare_initial_input(base_dir: Path, original_input_name: str = EXPECTED_INITIAL_INPUT_NAME) -> bool:
    """
    Cria um diretório Parquet de exemplo DENTRO do diretório base temporário,
    usando o nome que o código original espera.
    Retorna True se bem-sucedido, False caso contrário.
    """
    initial_input_dynamic_dir_path = base_dir / original_input_name
    st.write(f"✓ Preparando (potencial) dados de entrada em: `{initial_input_dynamic_dir_path}`...")

    # Dados de exemplo (não usados pelo default code atual, mas prontos se necessário)
    input_data = {'coluna_a': [15, 25, 35],
                  'coluna_b': ['exemplo1', 'exemplo2', 'exemplo3'],
                  'data_evento': pd.to_datetime(['2022-03-10', '2023-07-22', '2024-12-05'])}
    df_pandas_input = pd.DataFrame(input_data)

    try:
        initial_input_dynamic_dir_path.mkdir(parents=True, exist_ok=True)
        pandas_actual_file_path = initial_input_dynamic_dir_path / "data.parquet"
        df_pandas_input.to_parquet(pandas_actual_file_path, engine='pyarrow')
        # Removido o log excessivo sobre salvamento, apenas confirma a preparação geral
        # st.write(f"✓ DataFrame Pandas de entrada salvo em: `{pandas_actual_file_path}`")

        # Verificação rápida
        if not initial_input_dynamic_dir_path.is_dir() or not pandas_actual_file_path.is_file():
             raise RuntimeError(f"FALHA na verificação do diretório/arquivo Parquet de entrada!")
        st.write(f"✓ Verificação OK: Diretório/arquivo Parquet de entrada pronto (se necessário).")
        return True

    except Exception as e:
        st.error(f"ERRO CRÍTICO ao preparar o Parquet de entrada inicial: {e}")
        st.error(traceback.format_exc())
        return False

def stream_reader(stream, queue, stream_name):
    """Helper para ler stream em thread separada."""
    try:
        for line in iter(stream.readline, ''):
            queue.put((stream_name, line))
    except ValueError:
         pass
    except Exception as e:
         print(f"Erro lendo stream {stream_name}: {e}")
         pass
    finally:
        try:
            stream.close()
        except Exception:
            pass

def run_pyspark_script_realtime(script_path: str):
    """
    Executa um script PySpark (já modificado por AST, se aplicável) e gera output em tempo real.
    """
    st.write(f"Executando script: `{Path(script_path).name}`")
    output_lines = []

    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            env=env
        )

        output_queue = queue.Queue()
        stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, output_queue, 'stdout'))
        stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, output_queue, 'stderr'))
        stdout_thread.start()
        stderr_thread.start()

        while stdout_thread.is_alive() or stderr_thread.is_alive() or not output_queue.empty():
            try:
                stream_name, line = output_queue.get(timeout=0.1)
                output_lines.append(line) # Acumula todas as linhas
                yield stream_name, line # Gera para exibição
                output_queue.task_done()
            except queue.Empty:
                if process.poll() is not None and output_queue.empty():
                   break
                continue

        stdout_thread.join(timeout=1.0)
        stderr_thread.join(timeout=1.0)

        return_code = process.poll()
        if return_code is None:
             process.terminate()
             time.sleep(0.2)
             return_code = process.wait(timeout=1.0)

        yield 'status', return_code
        # Gera a saída completa no final, útil para o LLM
        yield 'full_output', "".join(output_lines)

    except FileNotFoundError:
        error_msg = f"Erro: O executável Python ou o script '{script_path}' não foi encontrado."
        st.error(error_msg)
        yield 'stderr', error_msg + "\n"
        yield 'status', 1
        yield 'full_output', error_msg
    except Exception as e:
        error_msg = f"Erro inesperado ao executar o script '{script_path}': {e}"
        st.error(error_msg)
        st.error(traceback.format_exc())
        yield 'stderr', error_msg + "\n"
        yield 'status', 1
        yield 'full_output', error_msg + "\n" + "".join(output_lines)


def call_llm_for_correction(code: str, error_message: str, model: str, api_key: str) -> str | None:
    """Chama a API OpenAI para obter correções de código."""
    # Garante que haja uma mensagem de erro para enviar
    clean_error_message = error_message.strip()
    if not clean_error_message:
        st.warning("Mensagem de erro vazia recebida. Enviando pedido genérico ao LLM.")
        clean_error_message = "[Streamlit Note] Script execution failed with a non-zero exit code, but no specific error message was captured from stdout/stderr. Please analyze the code for potential issues."

    st.info(f"Consultando {model} para correção...")
    try:
        client = OpenAI(api_key=api_key)
        prompt = f"""
        O seguinte script PySpark falhou. A saída combinada (stdout/stderr) é fornecida abaixo.
        Analise o código E a saída/erro. Se for um erro de sintaxe, corrija-o. Se for um erro de tempo de execução, tente corrigi-lo.
        Corrija APENAS o código Python para que ele funcione corretamente.
        Retorne SOMENTE o bloco de código Python corrigido, completo e pronto para ser executado.
        Não inclua explicações, introduções, marcadores de linguagem (```python), ou comentários fora do código, a menos que sejam comentários Python válidos dentro do script.

        --- CÓDIGO COM ERRO ---
        ```python
        {code}
        ```

        --- SAÍDA COMBINADA (STDOUT/STDERR) ---
        ```
        {clean_error_message}
        ```

        --- CÓDIGO PYTHON CORRIGIDO ---
        """

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Você é um assistente especialista em PySpark focado em depurar e corrigir códigos Python/PySpark. Retorne apenas o código corrigido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )
        corrected_code = response.choices[0].message.content.strip()

        # Limpeza de marcadores de código
        if corrected_code.startswith("```python"):
            corrected_code = corrected_code[len("```python"):].strip()
        elif corrected_code.startswith("```"):
             corrected_code = corrected_code[len("```"):].strip()
        if corrected_code.endswith("```"):
            corrected_code = corrected_code[:-len("```")].strip()

        if not corrected_code:
            st.warning("LLM retornou uma resposta vazia.")
            return None

        st.info("LLM retornou uma sugestão de correção.")
        return corrected_code

    except Exception as e:
        st.error(f"Erro ao chamar a API do OpenAI: {e}")
        st.error(traceback.format_exc())
        return None


# --- Interface Streamlit e Lógica Principal ---

def main():
    st.set_page_config(
        page_title="PySpark AST Auto-Correction",
        page_icon="⚡",
        layout="wide"
    )

    st.title("⚡ PySpark Interactive Auto-Correction with AST")

    # --- DESCRIÇÃO E AUTOR ---
    st.markdown("""
    Esta aplicação permite testar e depurar scripts PySpark interativamente com correção automática via LLM.
    1.  Use o editor abaixo ou cole seu código PySpark.
    2.  Se o seu código usar caminhos relativos para arquivos Parquet (ex: `df.write.parquet("output")`), a aplicação **reescreverá automaticamente** esses caminhos para usar um diretório temporário seguro, usando um AST Python.
    3.  Um arquivo Parquet de **entrada padrão** (`dados_entrada_raw.parquet`) é preparado nesse diretório, caso seu código precise lê-lo.
    4.  Clique em "Corrigir e Executar". A saída será exibida em tempo real.
    5.  Se ocorrer um erro (como o erro de sintaxe no código padrão), o LLM tentará corrigir o código automaticamente.
    """)
    # Seus detalhes
    st.markdown("""
    ---
    *   **LinkedIn:** [Rafael Sales](https://www.linkedin.com/in/rafaelsales)
    *   **GitHub:** [dmux](https://github.com/dmux)
    *   **Email:** rafael.sales@gmail.com
    """)
    st.divider()

    # --- Inputs ---
    col1, col2 = st.columns([1, 2])

    with col1:
        openai_api_key = st.text_input("🔑 Chave da API OpenAI", type="password", help="Sua chave não será armazenada.")
        llm_model = st.selectbox("🧠 Modelo LLM", ["gpt-4o-mini", "gpt-4-turbo", "gpt-4o", "gpt-3.5-turbo"], index=0, key="llm_model_select")
        max_attempts = st.slider("🔄 Máximo de Tentativas", 1, 10, MAX_ATTEMPTS_DEFAULT, key="max_attempts_slider")

        # Estado da sessão
        if 'base_run_dir' not in st.session_state:
            st.session_state.base_run_dir = None
        if 'run_id' not in st.session_state:
            st.session_state.run_id = str(uuid.uuid4())[:8]

    # ****** CÓDIGO PADRÃO ATUALIZADO ******
    # Inclui o exemplo simples com o erro de sintaxe (vírgula faltando)
    # e o boilerplate do SparkSession.
    default_code = f"""# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import traceback

spark = None
try:
    print("PySpark: Iniciando SparkSession...")
    spark = SparkSession.builder \\
        .appName("Streamlit_PySpark_AST_Run_{st.session_state.run_id}") \\
        .config("spark.driver.memory", "512m") \\
        .config("spark.sql.adaptive.enabled", "false") \\
        .master("local[*]") \\
        .getOrCreate()
    print(f"PySpark: SparkSession iniciada. Spark version: {{spark.version}}")

    # --- Código do Usuário Inserido Aqui ---

    # Step 1: Extract - Create a DataFrame manually
    # !! INTENTIONAL SYNTAX ERROR: Missing comma after the third tuple !!
    data = [
        (1, "Alice", 45),
        (2, "Luiz", 70),
        (3, "Elaine", 42)
        (4, "Carlos", 23),
        (5, "Vitor", 30),
        (6, "David", 17),
        (7, "Eva", 25)
    ]
    columns = ["id", "name", "age"]

    print("Criando DataFrame...")
    df = spark.createDataFrame(data, columns)
    print("DataFrame criado.")

    # Step 2: Transform - Filter rows where age >= 18
    print("Filtrando DataFrame (age >= 18)...")
    transformed_df = df.filter(col("age") >= 18)
    print("DataFrame filtrado.")

    # Step 3: Load - Show result (instead of saving to disk)
    print("Exibindo resultado:")
    transformed_df.show()

    # --- Fim do Código do Usuário ---

    print("\\nScript executado com SUCESSO.")
    result_message = "Execução concluída com SUCESSO."

except Exception as e:
    print(f"PySpark Script: ERRO FATAL durante a execução.")
    print(traceback.format_exc())
    result_message = f"Execução falhou: {{e}}"

finally:
    if spark:
        print("PySpark: Parando SparkSession...")
        spark.stop()
        print("PySpark: SparkSession parada.")

# Mensagem final (útil para scripts mais complexos)
# print(result_message)
"""

    with col2:
        user_code = st_ace(
            value=default_code, # USA O NOVO CÓDIGO PADRÃO
            placeholder="Cole seu código PySpark aqui...",
            language="python",
            key="ace_editor",
            height=550,
            font_size=13,
            wrap=True,
            auto_update=True,
            show_gutter=True,
            theme="github",
        )

    # --- Botões de Ação ---
    col_btn1, col_btn2, col_btn3 = st.columns(3)
    run_button_clicked = col_btn1.button("🚀 Corrigir e Executar", key="run_button", use_container_width=True)
    if col_btn2.button("🧹 Limpar Diretório Temporário Atual", key="clean_button", use_container_width=True):
        # Lógica de limpeza (sem mudanças)
        if st.session_state.base_run_dir and Path(st.session_state.base_run_dir).exists():
            try:
                shutil.rmtree(st.session_state.base_run_dir)
                st.success(f"Diretório `{st.session_state.base_run_dir}` removido.")
                st.session_state.base_run_dir = None
            except Exception as e:
                st.warning(f"Falha ao remover diretório `{st.session_state.base_run_dir}`: {e}")
        else:
            st.info("Nenhum diretório temporário ativo para limpar ou já removido.")
        cleaned_scripts = 0
        for f in Path(".").glob(SCRIPT_FILENAME_TEMPLATE.replace("{}", "*").replace("{}_{}", "*_*")):
            try: os.remove(f); cleaned_scripts += 1
            except OSError: pass
        if cleaned_scripts > 0: st.write(f"Limpos {cleaned_scripts} scripts temporários antigos.")
    

    # ** DEFINIÇÃO DO CÓDIGO HTML PARA SCROLL **
    html_code = f"""
    <div id="scroll-to-me" style='background: cyan; height=1px;'>hi</div>
    <script id="{random.randint(1000, 9999)}">
    var e = document.getElementById("scroll-to-me");
    if (e) {{
        e.scrollIntoView({{behavior: "smooth"}});
        e.remove();
    }}
    </script>
    """

    # --- Lógica de Execução ---
    if run_button_clicked:
        if not openai_api_key:
            st.error("❌ Por favor, insira sua chave da API OpenAI.")
            st.stop()
        if not user_code.strip():
            st.error("❌ Por favor, insira o código PySpark para executar.")
            st.stop()

        # --- Preparação do Ambiente ---
        base_run_dir = None
        try:
            with st.status("⚙️ Preparando ambiente de execução...", expanded=True) as setup_status:
                st.write("Gerando diretório temporário único...")
                base_run_dir = generate_base_dynamic_dir(f"temp_pyspark_st_run_{st.session_state.run_id}_")
                st.session_state.base_run_dir = str(base_run_dir)

                st.write(f"Preparando (potencial) arquivo Parquet de entrada ('{EXPECTED_INITIAL_INPUT_NAME}')...")
                input_prep_ok = prepare_initial_input(base_run_dir)
                if not input_prep_ok:
                    # Aviso em vez de erro fatal, pois o default code não usa
                    st.warning("Falha ao preparar o arquivo Parquet de entrada opcional. A execução continuará, mas a leitura de Parquet pode falhar se o código tentar.")
                else:
                     st.write("Arquivo Parquet opcional pronto.")

                st.write("Limpando scripts temporários anteriores desta run...")
                cleaned_run_scripts = 0
                # Usa o run_id para limpar apenas os scripts desta sessão
                for f in Path(".").glob(SCRIPT_FILENAME_TEMPLATE.format(st.session_state.run_id, "*")):
                    try: os.remove(f); cleaned_run_scripts += 1
                    except OSError: pass
                st.write(f"Limpeza de scripts da run ({cleaned_run_scripts}) concluída.")

                setup_status.update(label="✅ Ambiente pronto!", state="complete", expanded=False)

        except Exception as e:
            st.error(f"❌ Erro fatal durante a preparação do ambiente: {e}")
            st.error(traceback.format_exc())
            if base_run_dir and base_run_dir.exists():
                 try: shutil.rmtree(base_run_dir); st.write(f"Limpeza parcial do diretório {base_run_dir} realizada.")
                 except Exception as clean_e: st.warning(f"Falha na limpeza pós-erro: {clean_e}")
            st.session_state.base_run_dir = None
            st.stop()

        # --- Loop de Correção e Execução ---
        current_code = user_code
        success = False
        final_output_summary = ""
        script_path = ""

        st.markdown("---")
        st.subheader("🔄 Processamento e Tentativas")
        results_placeholder = st.container()

        for attempt in range(max_attempts):
            attempt_num = attempt + 1
            with results_placeholder.expander(f"📜 Tentativa {attempt_num}/{max_attempts}", expanded=True):

                st.markdown(f"**Tentativa {attempt_num}**")
                st.write("1. Analisando e (se aplicável) Reescrevendo Caminhos Parquet com AST...")
                modified_pyspark_code = None
                rewriter = None
                try:
                    original_ast = ast.parse(current_code)
                    rewriter = ParquetPathRewriter(base_run_dir)
                    modified_ast = rewriter.visit(original_ast)
                    ast.fix_missing_locations(modified_ast)

                    if sys.version_info >= (3, 9):
                         modified_pyspark_code = ast.unparse(modified_ast)
                    else:
                        st.error("ERRO: Python 3.9+ necessário para `ast.unparse`.")
                        raise ImportError("ast.unparse não disponível.")

                    # Log se caminhos foram reescritos ou não
                    if rewriter.rewritten_paths:
                        st.write("✓ Caminhos Parquet reescritos:")
                        for orig, new in rewriter.rewritten_paths.items():
                            st.code(f"'{orig}'  ->  '{new}'", language='diff')
                    else:
                        st.write("✓ Nenhum caminho Parquet relativo encontrado para reescrever.")

                    # Mostra o código que será executado (modificado ou não)
                    st.write("Código a ser executado:")
                    st.code(modified_pyspark_code, language="python", line_numbers=True)

                # Tratamento de Erros AST/Sintaxe
                except ImportError:
                     st.error("❌ Falha crítica: Versão do Python incompatível.")
                     success = False; break
                except SyntaxError as e:
                    st.error(f"❌ Erro de Sintaxe no código Python (Tentativa {attempt_num}): {e}")
                    st.warning("O LLM tentará corrigir a sintaxe.")
                    final_output_summary = f"SyntaxError: {e}\n{traceback.format_exc()}" # Dá mais contexto ao LLM
                    modified_pyspark_code = current_code # Tenta executar o código original para capturar o erro de sintaxe no stderr
                    # Não quebra o loop aqui, deixa a execução falhar e o LLM tentar corrigir
                except Exception as e:
                    st.error(f"❌ Erro durante a análise AST: {e}")
                    st.error(traceback.format_exc())
                    final_output_summary = f"Erro AST: {e}"
                    success = False; break

                # Continua para salvar e executar, mesmo com SyntaxError detectado pelo AST,
                # para que o stderr seja capturado pela execução do subprocesso.
                st.write(f"2. Salvando e Executando Script...")
                script_path = SCRIPT_FILENAME_TEMPLATE.format(st.session_state.run_id, attempt_num)
                try:
                    # Salva o código (modificado por AST ou original se AST falhou/não aplicável)
                    with open(script_path, "w", encoding='utf-8') as f:
                        f.write(modified_pyspark_code)
                    st.write(f"✓ Script temporário salvo: `{script_path}`")
                except Exception as e:
                    st.error(f"❌ Falha ao salvar script: {e}")
                    final_output_summary = f"Falha ao salvar script: {e}"; success = False; break

                # Execução com output real-time
                st.write(f"3. Saída da Execução (stdout/stderr):")
                realtime_output_placeholder = st.empty()
                current_display_content = f"--- Iniciando execução da Tentativa {attempt_num} ---\n"
                realtime_output_placeholder.code(current_display_content, language="log")
                components.html(html_code, height=1) # ** SCROLL 1: Leva para o início do output **

                process_generator = run_pyspark_script_realtime(script_path)
                exec_success = False
                exit_code = -99
                run_stdout = ""
                run_stderr = "" # Captura especificamente o stderr

                try:
                    for item_type, data in process_generator:
                        if item_type == 'stdout':
                            run_stdout += data
                            current_display_content += data
                        elif item_type == 'stderr':
                            run_stderr += data # Acumula stderr
                            current_display_content += f"[STDERR] {data}" # Destaca no log
                        elif item_type == 'status':
                            exit_code = data
                        elif item_type == 'full_output': # Captura a saída completa agregada
                             final_output_summary = data.strip()

                        # Atualiza o placeholder
                        realtime_output_placeholder.code(current_display_content, language="log")

                    # Após o loop, verifica o código de saída final
                    exec_success = (exit_code == 0)
                    current_display_content += f"\n--- Fim da execução (Tentativa {attempt_num}) --- Código de Saída: {exit_code} ---\n"
                    realtime_output_placeholder.code(current_display_content, language="log")

                    # Garante que final_output_summary tenha algo se a captura falhar
                    if not final_output_summary.strip() and (run_stdout or run_stderr):
                        final_output_summary = f"STDOUT:\n{run_stdout}\n\nSTDERR:\n{run_stderr}".strip()
                    elif not final_output_summary.strip() and exit_code != 0:
                         final_output_summary = f"[Streamlit Note] Execution failed with exit code {exit_code}. No stdout/stderr captured by the primary mechanism."

                except Exception as e:
                    st.error(f"❌ Erro durante a execução/streaming: {e}")
                    st.error(traceback.format_exc())
                    current_display_content += f"\n[STREAMLIT ERROR] Erro no streaming: {e}\n"
                    realtime_output_placeholder.code(current_display_content, language="log")
                    exec_success = False
                    final_output_summary += f"\nSTREAMING ERROR: {e}"

                # --- Verificação e Correção ---
                if exec_success:
                    st.success(f"✅ Sucesso na Tentativa {attempt_num}!")
                    components.html(html_code, height=1) # ** SCROLL 2: Leva para a msg de sucesso da tentativa **
                    success = True
                    # Opcional: Checar se a última linha indica sucesso
                    if final_output_summary and "SUCESSO" not in final_output_summary.splitlines()[-1]:
                         st.warning("🤔 Execução com código 0, mas 'SUCESSO' não encontrado na última linha do output.")
                    break

                else:
                    st.warning(f"❌ Falha na Tentativa {attempt_num} (Código de Saída: {exit_code}).")
                    components.html(html_code, height=1) # ** SCROLL 3: Leva para a msg de falha da tentativa **
                    if attempt < max_attempts - 1:
                        st.write("4. Tentando correção com LLM...")
                        # Passa o CÓDIGO ATUAL (current_code) e a SAÍDA COMPLETA para correção
                        corrected_code_suggestion = call_llm_for_correction(current_code, final_output_summary, llm_model, openai_api_key)

                        if corrected_code_suggestion and corrected_code_suggestion.strip() != current_code.strip():
                            st.info("💡 LLM sugeriu uma correção.")
                            st.text("Código Sugerido pelo LLM:")
                            st.code(corrected_code_suggestion, language="python", line_numbers=True)
                            current_code = corrected_code_suggestion # Atualiza para a próxima tentativa
                            components.html(html_code, height=1) # ** SCROLL 4: Leva para a sugestão do LLM **
                            time.sleep(1)
                        elif corrected_code_suggestion: # LLM retornou o mesmo código
                            st.warning("⚠️ LLM retornou o mesmo código. Interrompendo.")
                            final_output_summary += "\n\n[STREAMLIT] LLM sugeriu o mesmo código. Processo interrompido."
                            components.html(html_code, height=1) # ** SCROLL 5: Leva para a sugestão do LLM **
                            success = False; break
                        else: # LLM falhou ou retornou vazio
                            st.error("❌ Falha ao obter correção do LLM. Interrompendo.")
                            final_output_summary += "\n\n[STREAMLIT] Falha ao obter correção do LLM."
                            components.html(html_code, height=1) # ** SCROLL 6: Leva para a sugestão do LLM **
                            success = False; break
                    else:
                        st.error(f"🚫 Máximo de tentativas ({max_attempts}) atingido sem sucesso.")
                        components.html(html_code, height=1) # ** SCROLL 7: Leva para a msg de max tentativas **


            if success: break # Sai do loop FOR principal

        # --- Resultado Final ---
        st.markdown("---")
        st.subheader("🏁 Resultado Final do Processo")
        if success:
            st.success("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
            st.markdown("O código final (ver na última tentativa bem-sucedida acima) foi executado corretamente.")
        else:
            st.error("❌ PROCESSO CONCLUÍDO COM FALHA.")
            st.markdown(f"Não foi possível executar o código com sucesso após {attempt_num} tentativa(s).")
            st.markdown("Verifique a **última tentativa** acima para o código final tentado e a saída/erro.")
            if final_output_summary:
                st.subheader("Resumo da Saída/Erro da Última Tentativa:")
                st.text_area("Output Final", final_output_summary, height=200, key="final_output_area")
            elif exit_code != 0:
                 st.warning("A execução falhou, mas nenhum output detalhado foi capturado.")
        components.html(html_code, height=1) # ** SCROLL 8: Leva para a msg final **

        # --- Limpeza Final Automática ---
        st.write("---")
        st.write("🧹 Limpeza final do diretório temporário da execução...")
        if base_run_dir and isinstance(base_run_dir, Path) and base_run_dir.exists():
            try:
                shutil.rmtree(base_run_dir)
                st.write(f"✓ Diretório temporário `{base_run_dir}` removido.")
                st.session_state.base_run_dir = None
            except OSError as e:
                st.warning(f"⚠️ Falha ao remover diretório temporário `{base_run_dir}`: {e}.")
        else:
            st.write("ℹ️ Diretório temporário não encontrado ou já removido.")


if __name__ == "__main__":
    main()