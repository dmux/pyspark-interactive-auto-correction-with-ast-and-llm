import streamlit as st
import streamlit.components.v1 as components
import subprocess
import sys
import os
import time
import shutil
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from openai import OpenAI
import io
import queue
import threading
import html
import random
from streamlit_ace import st_ace
import ast


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


# add layout wide
st.set_page_config(
    page_title="PySpark Code Auto-Correction",
    page_icon=":sparkles:",
    layout="wide" # Use wide layout for better usability
)


st.title("⚡ PySpark Interactive Fixer")

# --- DESCRIÇÃO E AUTOR (NOVO HEADER) ---
st.markdown("""Esta aplicação permite testar scripts PySpark de forma interativa com auto correção usando LLMs.
Você pode colar seu código PySpark, gerar um arquivo .parquet de teste e executar o código.
A execução do código PySpark é feita em tempo real, permitindo que você veja os resultados imediatamente.
Você pode também corrigir erros de execução com a ajuda de um modelo LLM embutido.
A execução sempre ocorrerá em um ambiente
seguro com tratamento automático de caminhos para arquivos Parquet.
""")

# Substitua com seus dados reais!
st.markdown("""
---
*   **LinkedIn:** [Rafael Sales](https://www.linkedin.com/in/rafaelsales)
*   **GitHub:** [dmux](https://github.com/dmux)
*   **Email:** rafael.sales@gmail.com
""")
st.divider() # Linha divisória para separar o header do conteúdo principal

# --- Configuration ---
DATA_DIR = "spark_data_auto_correct_st" # Use a specific dir for Streamlit app
PARQUET_FILENAME = "input_data.parquet" # Fixed name for user code to reference
SCRIPT_FILENAME_TEMPLATE = "user_pyspark_script_{}.py" # Temporary script name template
MAX_ATTEMPTS_DEFAULT = 5
LLM_MODEL_DEFAULT = "gpt-4o-mini" # Cost-effective and good default
OPENAI_API_KEY = None # Will be taken from user input

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def generate_dummy_parquet(filename: str, data_dir: str) -> str:
    """Generates a simple dummy Parquet file."""
    st.write(f"Gerando arquivo Parquet de teste em '{data_dir}/{filename}'...")
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    file_path = data_path / filename
    data = {
        'id': [1, 2, 3, 4, 5, None, 7],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace'],
        'value': [10.5, 20.0, 15.2, 10.5, 22.8, 15.2, None],
        'category': ['A', 'B', 'A', 'C', 'B', 'C', 'A']
    }
    df = pd.DataFrame(data)
    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('value', pa.float64()),
        ('category', pa.string())
    ])
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, file_path)
    st.write("Arquivo Parquet gerado com sucesso.")
    return str(file_path.resolve())

def stream_reader(stream, queue, stream_name):
    try:
        for line in iter(stream.readline, ''):
            queue.put((stream_name, line))
    except Exception as e:
         pass
    finally:
        stream.close()

def run_pyspark_script_realtime(script_path: str):
    """
    Executes a PySpark script using Popen and yields (stream_name, line) tuples in real-time.
    Finally yields ('status', exit_code).
    """
    st.write(f"Executando: {Path(script_path).name} (com output em tempo real)")
    output_lines = []

    try:
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            bufsize=1,
            env=os.environ.copy()
        )

        output_queue = queue.Queue()
        stdout_thread = threading.Thread(target=stream_reader, args=(process.stdout, output_queue, 'stdout'))
        stderr_thread = threading.Thread(target=stream_reader, args=(process.stderr, output_queue, 'stderr'))
        stdout_thread.start()
        stderr_thread.start()

        while stdout_thread.is_alive() or stderr_thread.is_alive() or not output_queue.empty():
            try:
                stream_name, line = output_queue.get(timeout=0.1)
                output_lines.append(line)
                yield stream_name, line
                output_queue.task_done()
            except queue.Empty:
                if process.poll() is not None and output_queue.empty():
                   break
                continue

        stdout_thread.join()
        stderr_thread.join()
        return_code = process.wait()

        yield 'status', return_code
        yield 'full_output', "".join(output_lines)


    except FileNotFoundError:
        error_msg = f"Erro: O script '{script_path}' não foi encontrado."
        st.error(error_msg)
        yield 'stderr', error_msg + "\n"
        yield 'status', 1
        yield 'full_output', error_msg
    except Exception as e:
        error_msg = f"Erro inesperado ao executar o script '{script_path}': {e}"
        st.error(error_msg)
        yield 'stderr', error_msg + "\n"
        yield 'status', 1
        yield 'full_output', error_msg + "\n" + "".join(output_lines)

def call_llm_for_correction(code: str, error_message: str, model: str, api_key: str) -> str | None:
    """Calls the OpenAI API to get code corrections."""
    st.info(f"Consultando {model} para correção...")
    try:
        client = OpenAI(api_key=api_key)
        prompt = f"""
        O seguinte script PySpark falhou com o erro abaixo.
        Por favor, corrija o script para que ele funcione corretamente.
        Retorne APENAS o bloco de código Python corrigido, sem explicações adicionais, introduções ou comentários fora do código. Garanta que o código final esteja completo e funcional.

        --- CÓDIGO COM ERRO ---
        ```python
        {code}

        --- MENSAGEM DE ERRO (stdout/stderr) ---
        {error_message}
        --- CÓDIGO CORRIGIDO ---
        """

        response = client.chat.completions.create(
            model=model,
            messages=[
            {"role": "system", "content": "Você é um assistente especialista em PySpark."},
            {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            )
        corrected_code = response.choices[0].message.content.strip()

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
        return None


def main():
    # --- Inputs ---
    col1, col2 = st.columns([1, 2]) # Divide a tela em colunas

    with col1:
        # Inputs para API Key, Modelo LLM, Tentativas
        openai_api_key = st.text_input("🔑 Chave da API OpenAI", type="password", help="Sua chave não será armazenada.")
        llm_model = st.selectbox("🧠 Modelo LLM", ["gpt-4o-mini", "gpt-4-turbo", "gpt-4o", "gpt-3.5-turbo"], index=0)
        max_attempts = st.slider("🔄 Máximo de Tentativas", 1, 10, MAX_ATTEMPTS_DEFAULT)

    # Código de exemplo que aparece inicialmente na caixa de texto
    default_code = """from pyspark.sql.functions import col

# Step 1: Extract - Create a DataFrame manually
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

df = spark.createDataFrame(data, columns)

# Step 2: Transform - Filter rows where age >= 18
transformed_df = df.filter(col("age") >= 18)

# Step 3: Load - Show result (instead of saving to disk)
transformed_df.show()
"""

    with col2: # Na segunda coluna (mais larga)
        # AQUI é onde o usuário insere o código:
        user_code = st_ace(
        value=default_code,                  # Código padrão
        placeholder="Cole seu código PySpark aqui...", # Placeholder
        language="python",                          # Define a linguagem para syntax highlighting
        key="ace_editor",                           # Chave única para o widget
        height=500,                                 # Altura do editor em pixels
        font_size=14,                               # Tamanho da fonte (opcional)
        wrap=True,                                  # Habilita quebra de linha (opcional)
        auto_update=True,
        show_gutter=True,                           # Exibe o número da linha (opcional)
    )
    
    if st.button("🧹 Limpar Artefatos"):
        try:
            if Path(DATA_DIR).exists(): shutil.rmtree(DATA_DIR)
            if Path(PARQUET_FILENAME).exists(): os.remove(PARQUET_FILENAME)
            for f in Path(".").glob(SCRIPT_FILENAME_TEMPLATE.replace("{}", "*")): os.remove(f)
            
            st.success("Artefatos limpos com sucesso.")
        except Exception as e:
            st.warning(f"Aviso: Erro durante a limpeza - {e}")

    # --- Execution Logic (triggered by button) ---
    if st.button("🚀 Corrigir e Executar"):
        if not openai_api_key:
            st.error("Por favor, insira sua chave da API OpenAI.")
        elif not user_code: # Verifica se a caixa de texto está vazia
            st.error("Por favor, insira o código PySpark para executar.")
        else:
            st.info("Iniciando o processo...")

            # --- Setup ---
            with st.status("Preparando ambiente...", expanded=False) as setup_status:
                try:
                    # Limpeza
                    st.write("Limpando artefatos anteriores...")
                    for f in Path(".").glob(SCRIPT_FILENAME_TEMPLATE.replace("{}", "*")):
                        try: os.remove(f)
                        except OSError: pass
                    if Path(DATA_DIR).exists():
                        try: shutil.rmtree(DATA_DIR)
                        except Exception: pass
                    if Path(PARQUET_FILENAME).exists():
                        try: os.remove(PARQUET_FILENAME)
                        except OSError: pass
                    st.write("Limpeza concluída.")

                    # Gerar dados
                    st.write("Gerando arquivo Parquet de teste...")
                    parquet_file_path = generate_dummy_parquet(PARQUET_FILENAME, DATA_DIR) # Chama helper
                    shutil.copy(parquet_file_path, PARQUET_FILENAME)
                    st.write(f"Arquivo '{PARQUET_FILENAME}' pronto.")
                    setup_status.update(label="Ambiente pronto!", state="complete")
                except Exception as e:
                    st.error(f"Erro fatal durante a preparação: {e}")
                    setup_status.update(label="Falha na preparação!", state="error")
                    st.stop() # Impede a continuação

            # Inicialização para o loop
            current_code = user_code
            success = False
            final_output_summary = "" # Para erros
            script_path = ""

            # --- Área de Output em Tempo Real ---
            st.markdown("---")
            st.subheader("⚡ Processamento e Saída em Tempo Real")
            # Container para agrupar código e output de cada tentativa
            attempt_output_container = st.container()

            # --- Correction Loop ---
            # Usar spinner aqui pode ser redundante com o status acima e msgs abaixo
            for attempt in range(max_attempts):

                # Container específico para esta tentativa
                with attempt_output_container.container():
                    st.markdown(f"---") # Separador visual
                    st.info(f"**Tentativa {attempt + 1}/{max_attempts}**")
                    st.subheader(f"Código a ser executado:")
                    st.code(current_code, language="python", line_numbers=True)

                    st.subheader(f"Saída da Execução (Tentativa {attempt + 1}):")
                    realtime_output_placeholder = st.empty()
                    current_display_content = "Preparando para executar...\n"
                    realtime_output_placeholder.code(current_display_content, language="log")
                    time.sleep(0.2) # Pequena pausa para UI
                    components.html(html_code)

                    # Salvar código atual
                    script_path = SCRIPT_FILENAME_TEMPLATE.format(attempt + 1)
                    try:
                        with open(script_path, "w", encoding='utf-8') as f:
                            f.write(current_code)
                        current_display_content += f"Código salvo em '{script_path}'\nIniciando execução...\n---\n"
                        realtime_output_placeholder.code(current_display_content, language="log")
                    except Exception as e:
                        st.error(f"Falha ao salvar o script temporário na tentativa {attempt + 1}: {e}")
                        final_output_summary = f"Falha ao salvar o script temporário: {e}"
                        success = False # Garante que falhou
                        break # Não pode continuar sem salvar

                    # Executar com real-time output
                    process_generator = run_pyspark_script_realtime(script_path) # Chama helper
                    exec_success = False
                    exit_code = -1
                    full_run_output = ""
                    has_stderr = False # Flag para destacar falhas

                    try:
                        for item_type, data in process_generator:
                            if item_type == 'stdout':
                                current_display_content += data
                                realtime_output_placeholder.code(current_display_content, language="log")
                            elif item_type == 'stderr':
                                has_stderr = True # Marcar que houve erro
                                current_display_content += f"[STDERR] {data}" # Destacar stderr
                                realtime_output_placeholder.code(current_display_content, language="log")
                            elif item_type == 'status':
                                exit_code = data
                                exec_success = (exit_code == 0)
                            elif item_type == 'full_output':
                                full_run_output = data
                            # elif item_type == 'info': # Se run_pyspark_script_realtime gerasse 'info'
                            #    st.write(data) # Escreve info diretamente

                    except Exception as e:
                        st.error(f"Erro durante a execução/streaming da tentativa {attempt + 1}: {e}")
                        current_display_content += f"\n[STREAMLIT ERROR] Erro no streaming: {e}\n"
                        realtime_output_placeholder.code(current_display_content, language="log")
                        exec_success = False
                        full_run_output += f"\nSTREAMING ERROR: {e}"

                    final_output_summary = full_run_output.strip() # Usar para LLM

                    # --- Check Result and Correct (dentro do container da tentativa) ---
                    if exec_success:
                        st.success(f"✅ Sucesso na Tentativa {attempt + 1}!")
                        success = True
                        components.html(html_code)
                        break # Sai do loop de tentativas
                    else:
                        st.warning(f"❌ Falha na Tentativa {attempt + 1} (Código de Saída: {exit_code}).")

                        # Tentar corrigir se ainda houver tentativas
                        if attempt < max_attempts - 1:
                            corrected_code = call_llm_for_correction(current_code, final_output_summary, llm_model, openai_api_key) # Chama helper

                            if corrected_code and corrected_code.strip() != current_code.strip():
                                # st.info("LLM sugeriu uma correção. Próxima tentativa usará o código corrigido.")
                                current_code = corrected_code
                                # O código corrigido será exibido no início da próxima iteração
                                st.info("Código corrigido pelo LLM. Preparando próxima tentativa...")
                                time.sleep(1.5) # Pausa para ler
                            elif corrected_code and corrected_code.strip() == current_code.strip():
                                st.warning("LLM retornou o mesmo código. Interrompendo para evitar loop.")
                                final_output_summary += "\n\nLLM sugeriu o mesmo código. Interrompido."
                                break # Sai do loop
                            else:
                                st.error("Falha ao obter correção do LLM ou LLM não retornou código. Interrompendo.")
                                final_output_summary += "\n\nFalha ao obter correção do LLM."
                                break # Sai do loop
                        else:
                            st.error(f"Número máximo de tentativas ({max_attempts}) atingido.")
                            # Não precisa de break aqui, o loop vai terminar naturalmente

                # Fim do `with attempt_output_container.container():`
                if success: # Sai do loop for se sucesso foi alcançado
                    break
            components.html(html_code)

            # --- Final Result ---
            st.markdown("---")
            st.subheader("🏁 Resultado Final do Processo")
            if success:
                st.success("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
                st.markdown("O código final (exibido na última tentativa bem-sucedida acima) foi executado corretamente.")
            else:
                st.error("❌ PROCESSO CONCLUÍDO COM FALHA.")
                st.markdown(f"Não foi possível executar o código com sucesso após {attempt + 1} tentativa(s).") # Usa 'attempt + 1' que é o número correto de tentativas feitas
                st.markdown("Verifique a última tentativa acima para o código final tentado e a saída/erro correspondente.")
                if final_output_summary:
                    st.subheader("Resumo da Última Saída/Erro:")
                    st.text(final_output_summary)
            components.html(html_code)

if __name__ == "__main__":
    main()
