# -----------------------------------------------------------------------------
# SAS to PySpark Transpiler v4.2
# (Suporte Multi-LLM: OpenAI, Google Gemini, Anthropic Claude)
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
import json
from dotenv import load_dotenv


try:
    from openai import OpenAI, APIError, AuthenticationError
except ImportError:
    st.error("Biblioteca OpenAI não encontrada. `pip install openai`")
    OpenAI = None
    APIError = Exception
    AuthenticationError = Exception
try:
    import google.generativeai as genai
    from google.api_core import exceptions as google_exceptions
except ImportError:
    st.error(
        "Biblioteca Google GenAI não encontrada. `pip install google-generativeai`"
    )
    genai = None
    google_exceptions = None
try:
    from anthropic import (
        Anthropic,
        APIError as AnthropicAPIError,
        AuthenticationError as AnthropicAuthError,
    )
except ImportError:
    st.error("Biblioteca Anthropic não encontrada. `pip install anthropic`")
    Anthropic = None
    AnthropicAPIError = Exception
    AnthropicAuthError = Exception

# load_dotenv()

DEFAULT_MODELS = {
    "OpenAI": ["gpt-4o-mini", "gpt-4o", "gpt-3.5-turbo"],
    "Google": ["gemini-1.5-flash-latest", "gemini-1.5-pro-latest", "gemini-1.0-pro"],
    "Anthropic": [
        "claude-3-haiku-20240307",
        "claude-3-sonnet-20240229",
        "claude-3-opus-20240229",
    ],
}
ALL_PROVIDERS = list(DEFAULT_MODELS.keys())


def initialize_spark_environment():
    if st.session_state.get("spark_found", False):
        return True
    spark_home_env = os.environ.get("SPARK_HOME")
    spark_path_found = None
    found_method = None
    if spark_home_env and os.path.isdir(spark_home_env):
        spark_path_found = spark_home_env
        found_method = "Variável de Ambiente (SPARK_HOME)"
        try:
            findspark.init(spark_home_env)
        except Exception as e:
            st.warning(
                f"Aviso: SPARK_HOME encontrado ({spark_home_env}), mas findspark.init() falhou: {e}"
            )
    else:
        try:
            findspark.init()
            spark_path_found = findspark.find()
            os.environ["SPARK_HOME"] = spark_path_found
            found_method = "findspark (Busca Automática)"
        except (ValueError, ImportError, Exception) as e:
            spark_path_found = None
    if spark_path_found:
        st.session_state.spark_home_discovered = spark_path_found
        st.session_state.spark_found_method = found_method
        st.session_state.spark_found = True
        return True
    else:
        st.session_state.spark_home_discovered = None
        st.session_state.spark_found_method = "Nenhum"
        st.session_state.spark_found = False
        return False


def parse_input_statement(input_statement: str) -> dict:
    schema = {}
    input_statement = re.sub(
        r"\(\s*[\w\$\.]+\s*(?:[\w\$\.\s]+)?\)", "", input_statement
    )
    input_statement = re.sub(r"@\(.*?\)|@\d+", "", input_statement)
    input_statement = re.sub(r"\+\d+", "", input_statement)
    input_statement = input_statement.replace("/", " ")
    input_statement = re.sub(r"\s+\d+\.\d*", "", input_statement)
    input_statement = re.sub(r"\s+\$\d*\.?", "", input_statement)
    parts = input_statement.split()
    idx = 0
    while idx < len(parts):
        part = parts[idx]
        col_name = None
        col_type = "numeric"
        if part == "$":
            idx += 1
            if idx < len(parts):
                col_name = parts[idx]
                col_type = "string"
            else:
                break
        elif part.endswith("$"):
            col_name = part[:-1]
            col_type = "string"
        else:
            col_name = part
            col_type = "numeric"
        if col_name and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
            schema[col_name] = col_type
        idx += 1
    return schema


def identify_datasets_and_schemas(sas_code: str) -> dict:
    datasets = {}
    data_step_pattern_v2 = re.compile(
        r"DATA\s+(?:WORK\.)?(\w+)\s*;(.*?)?INPUT(.*?);(.*?)?(?:(?:DATALINES|CARDS)\s*;\s*(.*?)\s*;)?\s*RUN;",
        re.IGNORECASE | re.DOTALL,
    )
    for match in data_step_pattern_v2.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        before_input = match.group(2) or ""
        input_content = match.group(3).strip() if match.group(3) else ""
        between_input_data = match.group(4) or ""
        inline_data_content = match.group(5).strip() if match.group(5) else ""
        has_infile = (
            "INFILE" in before_input.upper() or "INFILE" in between_input_data.upper()
        )
        has_inline = bool(inline_data_content)
        schema = parse_input_statement(input_content) if input_content else {}
        source = "unknown"
        if has_inline:
            source = "inline"
        elif has_infile:
            source = "infile"
        elif schema:
            source = "data_step_no_data"
        datasets[ds_name] = {
            "source": source,
            "schema": schema,
            "has_inline_data": has_inline,
            "inline_data_snippet": inline_data_content[:500],
        }
    sql_ref_pattern = re.compile(r"(?:FROM|JOIN)\s+(?:WORK\.)?(\w+)", re.IGNORECASE)
    proc_sql_pattern = re.compile(r"PROC\s+SQL;.*?QUIT;", re.IGNORECASE | re.DOTALL)
    create_table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:WORK\.)?(\w+)", re.IGNORECASE
    )
    for sql_match in proc_sql_pattern.finditer(sas_code):
        sql_block = sql_match.group(0)
        created_tables = {
            m.group(1).strip().upper() for m in create_table_pattern.finditer(sql_block)
        }
        for ref_match in sql_ref_pattern.finditer(sql_block):
            ds_name = ref_match.group(1).strip().upper()
            if ds_name not in datasets and ds_name not in created_tables:
                datasets[ds_name] = {
                    "source": "proc_sql_ref",
                    "schema": {},
                    "has_inline_data": False,
                }
    proc_data_pattern = re.compile(
        r"PROC\s+\w+\s+DATA\s*=\s*(?:WORK\.)?(\w+)", re.IGNORECASE
    )
    for match in proc_data_pattern.finditer(sas_code):
        ds_name = match.group(1).strip().upper()
        if ds_name not in datasets:
            datasets[ds_name] = {
                "source": "proc_ref",
                "schema": {},
                "has_inline_data": False,
            }
    return datasets


def format_data_as_markdown_table(
    dataset_name: str, schema: dict, data_rows: list[dict], source_type: str
) -> str:
    if not data_rows or not schema:
        return ""
    headers = list(schema.keys())
    if not headers:
        if data_rows:
            headers = list(data_rows[0].keys())
        else:
            return ""
    source_label = "Extraída" if source_type == "inline" else "Gerada Automaticamente"
    md_table = f"#### Amostra {source_label}: `{dataset_name}`\n\n| {' | '.join(headers)} |\n|{'-|'.join(['-' * max(3, len(h)) for h in headers])}-|\n"
    for row_dict in data_rows:
        row_values = [str(row_dict.get(h, "")) for h in headers]
        md_table += f"| {' | '.join(row_values)} |\n"
    return md_table + "\n"


def extract_inline_data_structured(
    sas_code: str, datasets_info: dict, max_sample_rows=5
) -> tuple[dict, dict]:
    structured_samples = {}
    updated_info = datasets_info.copy()
    for ds_name, info in datasets_info.items():
        if info["source"] == "inline" and info.get("inline_data_snippet"):
            schema_dict = info.get("schema", {})
            if not schema_dict:
                continue
            columns = list(schema_dict.keys())
            datalines_content = info["inline_data_snippet"]
            data_rows_as_list_of_lists = []
            lines = datalines_content.split("\n")
            count = 0
            current_schema_types = list(schema_dict.values())
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if count >= max_sample_rows:
                    break
                values = line.split()
                if len(values) == len(columns):
                    row_values_list = []
                    valid_row = True
                    for i, col_name in enumerate(columns):
                        val = values[i]
                        row_values_list.append(val)
                        if schema_dict[col_name] == "numeric":
                            try:
                                float(val)
                            except ValueError:
                                schema_dict[col_name] = "string"
                                current_schema_types[i] = "string"
                    if valid_row:
                        data_rows_as_list_of_lists.append(row_values_list)
                        count += 1
            if data_rows_as_list_of_lists:
                updated_info[ds_name]["schema"] = schema_dict
                structured_samples[ds_name] = {
                    "schema": columns,
                    "data": data_rows_as_list_of_lists,
                    "source": "inline",
                }
    extracted_count = len(structured_samples)
    if extracted_count > 0:
        st.success(f"{extracted_count} amostra(s) inline extraída(s) para estrutura.")
    return structured_samples, updated_info


def parse_markdown_tables(markdown_text: str) -> dict:
    parsed_data = {}
    table_pattern = re.compile(
        r"####\s+Amostra\s+(?:Gerada\s+Automaticamente|Extraída):\s*`(\w+)`.+?\n\s*\|\s*(.*?)\s*\|\s*\n\s*\|(?:[-:]+\|)+[-:]+\|?\s*\n((?:\s*\|.*?\|\s*\n)+)",
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    for match in table_pattern.finditer(markdown_text):
        try:
            dataset_name = match.group(1).strip().upper()
            header_content = match.group(2).strip()
            data_rows_block = match.group(3).strip()
            headers = [h.strip() for h in header_content.split("|")]
            data_rows = []
            for line in data_rows_block.split("\n"):
                line = line.strip()
                if not line.startswith("|") or not line.endswith("|"):
                    continue
                values = [v.strip() for v in line.split("|")[1:-1]]
                if len(values) == len(headers):
                    data_rows.append(values)
                else:
                    st.warning(
                        f"Linha ignorada ao parsear MD '{dataset_name}': {len(values)}/{len(headers)} vals. Linha: '{line}'"
                    )
            if dataset_name and headers and data_rows:
                source = (
                    "generated"
                    if "Gerada Automaticamente" in match.group(0)
                    else "inline"
                )
                parsed_data[dataset_name] = {
                    "schema": headers,
                    "data": data_rows,
                    "source": source,
                }
        except Exception as e:
            st.warning(
                f"Erro ao parsear tabela MD (dataset: {match.group(1) if match else 'N/A'}): {e}"
            )
    parsed_count = len(parsed_data)
    if parsed_count > 0:
        st.success(f"{parsed_count} tabela(s) Markdown parseada(s) da resposta do LLM.")
    return parsed_data


def agent_3_execute_pyspark(pyspark_code: str) -> tuple[str, str, int]:
    if not pyspark_code.strip():
        return "*Nenhum código PySpark para executar.*", "", -1
    if not st.session_state.get("spark_found", False) or not st.session_state.get(
        "spark_home_discovered"
    ):
        error_msg = "Erro Crítico: SPARK_HOME não encontrado."
        st.error(error_msg)
        return "", error_msg, -5
    spark_home = st.session_state.spark_home_discovered
    spark_submit_executable = os.path.join(spark_home, "bin", "spark-submit")
    is_windows = platform.system() == "Windows"
    if is_windows and not os.path.isfile(spark_submit_executable):
        spark_submit_cmd_path = spark_submit_executable + ".cmd"
        if os.path.isfile(spark_submit_cmd_path):
            spark_submit_executable = spark_submit_cmd_path
        else:
            error_msg = f"Erro: spark-submit(.cmd) não encontrado em '{os.path.join(spark_home, 'bin')}'."
            st.error(error_msg)
            return "", error_msg, -6
    elif not is_windows and not os.path.isfile(spark_submit_executable):
        error_msg = (
            f"Erro: spark-submit não encontrado em '{os.path.join(spark_home, 'bin')}'."
        )
        st.error(error_msg)
        return "", error_msg, -6
    stdout_str, stderr_str, return_code = "", "", -1
    tmp_file_path = None
    safe_encoding = "utf-8"
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".py",
            delete=False,
            encoding=safe_encoding,
            errors="replace",
        ) as tmp_file:
            tmp_file_path = tmp_file.name
            tmp_file.write(pyspark_code)
            tmp_file.flush()
        st.info(f"Código PySpark salvo em: {tmp_file_path} (encoding: {safe_encoding})")
        st.info(f"Executando com: {spark_submit_executable}")
        spark_submit_cmd = [spark_submit_executable, tmp_file_path]
        st.code(f"Comando: {' '.join(spark_submit_cmd)}", language="bash")
        run_env = os.environ.copy()
        run_env["PYTHONIOENCODING"] = safe_encoding
        process = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            encoding=safe_encoding,
            errors="replace",
            timeout=300,
            env=run_env,
        )
        stdout_str, stderr_str, return_code = (
            process.stdout,
            process.stderr,
            process.returncode,
        )
        st.success(f"Subprocess concluído (Retorno: {return_code}).")
    except subprocess.TimeoutExpired as e:
        stderr_str = f"Erro: Timeout (300s).\n{getattr(e, 'stderr', '')}"
        stdout_str = getattr(e, "stdout", "")
        st.error(stderr_str)
        return_code = -4
    except Exception as e:
        stderr_str = f"Erro inesperado no subprocess:\n{traceback.format_exc()}"
        st.error(stderr_str)
        return_code = -3
    finally:
        if tmp_file_path and os.path.exists(tmp_file_path):
            try:
                os.remove(tmp_file_path)
            except Exception as e_clean:
                st.warning(f"Falha ao limpar {tmp_file_path}: {e_clean}")
    return stdout_str, stderr_str, return_code


def initialize_llm_client(provider: str, api_key: str):
    """Inicializa e retorna o cliente LLM com base no provedor."""
    client = None
    error_message = None

    if not api_key:
        return None, "API Key não fornecida."

    try:
        if provider == "OpenAI":
            if OpenAI:
                client = OpenAI(api_key=api_key)
                client.models.list()  # Test connection
            else:
                error_message = "Biblioteca OpenAI não carregada."
        elif provider == "Google":
            if genai:
                genai.configure(api_key=api_key)
                client = genai  # Retorna o módulo configurado
            else:
                error_message = "Biblioteca Google GenAI não carregada."
        elif provider == "Anthropic":
            if Anthropic:
                client = Anthropic(api_key=api_key)
                client.count_tokens(text="test")  # Test connection
            else:
                error_message = "Biblioteca Anthropic não carregada."
        else:
            error_message = "Provedor LLM inválido selecionado."

    except AuthenticationError as e:  # OpenAI Auth Error
        error_message = f"Erro de Autenticação ({provider}): Chave inválida ou permissões insuficientes. {e}"
    except AnthropicAuthError as e:  # Anthropic Auth Error
        error_message = f"Erro de Autenticação ({provider}): Chave inválida ou permissões insuficientes. {e}"
    except google_exceptions.PermissionDenied as e:  # Google Auth Error
        error_message = f"Erro de Autenticação/Permissão ({provider}): Chave inválida ou API não habilitada? {e}"
    except APIError as e:  # OpenAI API Error
        error_message = f"Erro de API ({provider}): {e}"
    except AnthropicAPIError as e:  # Anthropic API Error
        error_message = f"Erro de API ({provider}): {e}"
    except Exception as e:  # Outros erros (Google config, rede, etc.)
        error_message = f"Erro ao inicializar cliente {provider}: {e}"
        # traceback.print_exc() # Para debug mais detalhado no console

    if error_message:
        st.error(error_message)
        return None, error_message
    else:
        st.success(f"Cliente {provider} inicializado com sucesso.")
        return client, None  # Retorna o cliente e None para erro


def call_llm(
    provider: str,
    client,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_tokens: int,
) -> tuple[str, str | None]:
    """
    Chama o LLM apropriado e retorna a resposta de texto.
    Retorna (texto_resposta, mensagem_erro | None)
    """
    if client is None:
        return "*Erro: Cliente LLM não inicializado.*", "Cliente não inicializado"

    response_text = None
    error_message = None

    try:
        if provider == "OpenAI":
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": user_prompt})
            completion = client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            response_text = completion.choices[0].message.content.strip()

        elif provider == "Google":
            # Google GenAI (usando gemini-pro ou flash)
            model = client.GenerativeModel(model_name)  # Obter o modelo específico
            # Formatar prompt para Google (pode variar um pouco, simplificado aqui)
            # Google prefere menos a ideia de "system prompt" separado no básico API
            # Combinamos no user prompt ou usamos estruturas mais complexas para multi-turn
            full_prompt_google = (
                f"{system_prompt}\n\n{user_prompt}" if system_prompt else user_prompt
            )
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,  # Nome diferente para max_tokens
            )
            response = model.generate_content(
                full_prompt_google, generation_config=generation_config
            )
            # Extrair texto da resposta (pode precisar de mais tratamento de erro se bloqueado, etc.)
            if response.parts:
                response_text = "".join(part.text for part in response.parts).strip()
            elif response.candidates and response.candidates[0].content.parts:
                response_text = "".join(
                    part.text for part in response.candidates[0].content.parts
                ).strip()
            else:
                # Tentar obter o texto de parada ou erro
                try:
                    reason = response.candidates[0].finish_reason
                    error_message = (
                        f"Geração Google interrompida: {reason}. Resposta: {response}"
                    )
                except (IndexError, AttributeError):
                    error_message = f"Resposta inesperada do Google: {response}"
                response_text = (
                    f"*Falha ao obter conteúdo da resposta Google: {error_message}*"
                )

        elif provider == "Anthropic":
            # Anthropic Claude
            response = client.messages.create(
                model=model_name,
                system=(
                    system_prompt if system_prompt else None
                ),  # Parâmetro 'system' separado
                messages=[{"role": "user", "content": user_prompt}],
                temperature=temperature,
                max_tokens=max_tokens,  # Anthropic usa max_tokens
            )

            # Extrair texto (geralmente o primeiro bloco de texto)
            if (
                response.content
                and isinstance(response.content, list)
                and hasattr(response.content[0], "text")
            ):
                response_text = response.content[0].text.strip()
            else:
                error_message = f"Resposta inesperada do Anthropic: {response}"
                response_text = (
                    f"*Falha ao obter conteúdo da resposta Anthropic: {error_message}*"
                )

    # --- Tratamento de Erros Específicos da API ---
    except AuthenticationError as e:
        error_message = f"Erro de Autenticação ({provider}): {e}"
    except AnthropicAuthError as e:
        error_message = f"Erro de Autenticação ({provider}): {e}"
    except google_exceptions.PermissionDenied as e:
        error_message = f"Erro de Autenticação/Permissão ({provider}): {e}"
    except APIError as e:
        error_message = f"Erro de API ({provider}): {e}"
        traceback.print_exc()  # Print traceback para debug
    except AnthropicAPIError as e:
        error_message = f"Erro de API ({provider}): {e}"
        traceback.print_exc()
    except google_exceptions.GoogleAPIError as e:
        error_message = f"Erro de API ({provider}): {e}"
        traceback.print_exc()
    except Exception as e:  # Erros gerais
        error_message = f"Erro inesperado na chamada LLM ({provider}): {e}"
        traceback.print_exc()

    if error_message:
        st.error(f"Erro na chamada LLM: {error_message}")
        return (
            response_text or f"*Erro durante chamada LLM: {error_message}*",
            error_message,
        )
    elif response_text is None:
        st.error(f"Erro: Nenhuma resposta de texto obtida do LLM ({provider}).")
        return (
            f"*Erro: Resposta vazia do LLM ({provider}).*",
            f"Resposta vazia do LLM ({provider}).",
        )

    return response_text, None  # Retorna texto e None para erro


def agent_1_orchestrator(
    client, provider: str, model_name: str, sas_code: str
) -> tuple[str, dict]:
    """Orquestra Agente 1 usando o wrapper call_llm."""
    if not sas_code.strip():
        return "*Insira o código SAS.*", {}
    if client is None:
        return "*Erro: Cliente LLM não inicializado para Agente 1.*", {}

    datasets_info = {}
    try:
        datasets_info = identify_datasets_and_schemas(sas_code)
    except Exception as e:
        st.warning(f"Erro na identificação: {e}")
    inline_structured_data, datasets_info = extract_inline_data_structured(
        sas_code, datasets_info
    )

    # 3. Preparar para LLM
    datasets_to_generate = []
    datasets_summary_for_prompt = "\n**Contexto dos Datasets Identificados:**\n"
    if datasets_info:
        for name, info in datasets_info.items():
            schema_dict = info.get("schema", {})
            schema_str = (
                ", ".join([f"{col} ({typ})" for col, typ in schema_dict.items()])
                if schema_dict
                else "Schema não inferido"
            )
            source_desc = info.get("source", "unknown")
            has_inline_str = "Sim" if info.get("has_inline_data") else "Não"
            datasets_summary_for_prompt += f"- `{name}`: Fonte={source_desc}, Dados Inline={has_inline_str}, Schema Inferido=[{schema_str}]\n"
            if name not in inline_structured_data:
                datasets_to_generate.append({"name": name, "schema": schema_dict})

    system_prompt_agent1 = "Você é um especialista SAS que documenta código e gera dados de amostra OBRIGATORIAMENTE como tabelas Markdown formatadas."
    user_prompt_agent1 = f"""**Sua Tarefa Principal:** Analisar SAS -> Gerar documentação + dados de amostra formatados.\n**Instruções:**\n1.  **Documentação Descritiva:** Descreva objetivo, lógica, I/O.\n2.  **GERAÇÃO DE AMOSTRAS:** Para CADA dataset em "Datasets para Gerar Amostra":\n    *   Inferir/Refinar Schema.\n    *   Gerar 3-5 linhas de dados fictícios consistentes.\n    *   **FORMATAR COMO TABELA MARKDOWN (Obrigatório):** Usar o formato:\n        ```markdown\n        #### Amostra Gerada Automaticamente: `NOME_DATASET`\n\n        | COL1 | COL2 | ... |\n        |------|------|-----|\n        | val1 | val2 | ... |\n        ...\n\n        ```\n3.  **Estrutura da Resposta:** Documentação primeiro, depois TODAS as tabelas Markdown geradas.\n\n**Código SAS:** ```sas\n{sas_code}\n```\n{datasets_summary_for_prompt}\n**Datasets para Gerar Amostra (Gerar Tabela MD para cada):**\n{chr(10).join([f"- `{d['name']}` (Schema Básico: {d['schema'] if d.get('schema') else 'Nenhum'})" for d in datasets_to_generate]) if datasets_to_generate else "Nenhum."}\n---\n**Documentação Descritiva e Tabelas de Amostra GERADAS (Markdown):**"""

    llm_generated_markdown = "*Erro inicial*"
    llm_response_text, error = call_llm(
        provider=provider,
        client=client,
        model_name=model_name,
        system_prompt=system_prompt_agent1,
        user_prompt=user_prompt_agent1,
        temperature=0.4,
        max_tokens=3000,
    )
    llm_generated_markdown = llm_response_text  # Mesmo se houver erro, pegamos o texto parcial/mensagem de erro
    if error:
        st.error(f"Erro na chamada do Agente 1: {error}")
    else:
        st.success("Agente 1 LLM respondeu.")

    llm_parsed_structured_data = parse_markdown_tables(llm_generated_markdown)

    final_structured_data = inline_structured_data.copy()
    final_structured_data.update(llm_parsed_structured_data)

    inline_samples_md_display = ""
    for ds_name, data_info in inline_structured_data.items():
        schema_dict_inline = {
            col: datasets_info.get(ds_name, {}).get("schema", {}).get(col, "string")
            for col in data_info["schema"]
        }
        data_rows_dict = [
            dict(zip(data_info["schema"], row)) for row in data_info["data"]
        ]
        inline_samples_md_display += format_data_as_markdown_table(
            ds_name, schema_dict_inline, data_rows_dict, "inline"
        )
    if inline_samples_md_display:
        inline_samples_md_display = (
            "\n\n---\n\n### Dados de Amostra Extraídos (Inline)\n"
            + inline_samples_md_display
        )
    final_markdown_for_display = (
        llm_generated_markdown + "\n" + inline_samples_md_display
    )

    st.success(
        f"Agente 1: {len(final_structured_data)} dataset(s) com amostra estruturada."
    )
    return final_markdown_for_display, final_structured_data


def agent_2_transpile_sas_using_structure_and_context(
    client,
    provider: str,
    model_name: str,
    sas_code: str,
    sample_data: dict,
    edited_markdown_context: str,
) -> str:
    """Agente 2 (v4.2): Usa call_llm, estrutura de dados e contexto MD."""
    if not sas_code.strip():
        return "# Código SAS necessário."
    if client is None:
        return f"# Erro: Cliente LLM não inicializado para Agente 2 ({provider})."
    if not sample_data:
        st.warning("Agente 2 (v4.2) chamado sem dados estruturados!")
        sample_data = {}
    if not edited_markdown_context:
        edited_markdown_context = "(Documentação de contexto não fornecida)"

    try:
        sample_data_json = json.dumps(sample_data, indent=2)
    except Exception as e:
        st.error(f"Erro ao serializar dados para JSON: {e}")
        sample_data_json = "{}"

    system_prompt_agent2 = "Você gera scripts PySpark. Usa DADOS JSON fornecidos para criar DataFrames. Usa DOCUMENTAÇÃO MARKDOWN fornecida apenas como CONTEXTO para entender a LÓGICA SAS."
    user_prompt_agent2 = f"""
    **Tarefa:** Gerar script PySpark completo e executável.\n
    **Instruções Cruciais:**\n
    1.  **Inicialização SparkSession:** Incluir imports e criar `spark`.\n
    2.  **Gerar DataFrames (USANDO APENAS JSON):** Para CADA dataset nos `Dados de Amostra Estruturados (JSON)` abaixo, gere `spark.createDataFrame(data, schema)`. Use os valores do JSON. **Defina o schema explicitamente com `StructType`/`StructField`** para garantir tipos corretos. **NÃO use o Markdown para esta etapa.**\n
    3.  **Transpilar Lógica SAS (USANDO MARKDOWN COMO CONTEXTO):** Converta a lógica SAS (PROCs, DATA steps sem DATALINES, etc.) para PySpark. Use a `Documentação de Contexto (Markdown Editado)` abaixo para entender o propósito e a lógica. **NÃO tente parsear tabelas do Markdown.**\n
    4.  **Saídas:** Usar `dataframe_name.show()`.\n
    5.  **Encerramento:** Usar `spark.stop()` em `finally`.\n
    **Diretrizes:** APENAS o código PySpark completo.\n\n
    **Código SAS Original:**\n```sas\n{sas_code}\n```\n\n
    **Dados de Amostra Estruturados (JSON) - Fonte OBRIGATÓRIA para createDataFrame:**\n```json\n{sample_data_json}\n```\n\n
    **Documentação de Contexto (Markdown Editado) - Use APENAS para entender a LÓGICA SAS:**\n```markdown\n{edited_markdown_context}\n```\n\n
    **Código PySpark Completo Gerado:**\n
    ```python
    # -*- coding: utf-8 -*-
    # Script PySpark completo gerado por IA (v4.2)
    # ... (imports, SparkSession, createDataFrames from JSON, logic using MD context, spark.stop)
    ```
    """

    # Chamar LLM via Wrapper
    pyspark_code = f"# Erro inicial na chamada do Agente 2 ({provider})"
    llm_response_text, error = call_llm(
        provider=provider,
        client=client,
        model_name=model_name,
        system_prompt=system_prompt_agent2,
        user_prompt=user_prompt_agent2,
        temperature=0.25,
        max_tokens=4000,  # Aumentar limite
    )
    pyspark_code = llm_response_text  # Pega texto mesmo se erro
    if error:
        st.error(f"Erro na chamada do Agente 2: {error}")
    else:
        st.success("Agente 2 LLM respondeu.")

    # Limpeza do código (extrair de ```python ... ```)
    if pyspark_code and "```python" in pyspark_code:
        code_match = re.search(r"```python\n(.*)```", pyspark_code, re.DOTALL)
        if code_match:
            pyspark_code = code_match.group(1).strip()
        else:  # Fallback se regex falhar
            lines = pyspark_code.split("\n")
            start_idx = -1
            end_idx = len(lines)
            for i, line in enumerate(lines):
                if line.strip() == "```python":
                    start_idx = i + 1
                    break
            if start_idx != -1:
                for i in range(len(lines) - 1, start_idx - 1, -1):
                    if lines[i].strip() == "```":
                        end_idx = i
                        break
                pyspark_code = "\n".join(lines[start_idx:end_idx]).strip()
    return pyspark_code


# --- Interface Streamlit (UI) ---
st.set_page_config(layout="wide", page_title="SAS -> PySpark Transpiler v4.2")
st.title("🤖 Transpilador Assistido por IA: SAS para PySpark (v4.2)")
st.caption(
    "Multi-LLM | Dados Estruturados | Contexto Editável | Transpilação | Execução"
)

# --- Inicialização do Estado da Sessão (com defaults para LLM) ---
if "sas_code" not in st.session_state:
    st.session_state.sas_code = "/* Cole seu código SAS aqui */"
if "markdown_documentation" not in st.session_state:
    st.session_state.markdown_documentation = ""
if "sample_data_structure" not in st.session_state:
    st.session_state.sample_data_structure = {}
if "pyspark_code" not in st.session_state:
    st.session_state.pyspark_code = ""
if "agent1_run_complete" not in st.session_state:
    st.session_state.agent1_run_complete = False
if "show_transpile_button" not in st.session_state:
    st.session_state.show_transpile_button = False
if "execution_stdout" not in st.session_state:
    st.session_state.execution_stdout = ""
if "execution_stderr" not in st.session_state:
    st.session_state.execution_stderr = ""
if "execution_ran" not in st.session_state:
    st.session_state.execution_ran = False
if "execution_return_code" not in st.session_state:
    st.session_state.execution_return_code = None

# LLM Config States
if "llm_provider" not in st.session_state:
    st.session_state.llm_provider = ALL_PROVIDERS[0]  # Default OpenAI
if "llm_api_key" not in st.session_state:
    st.session_state.llm_api_key = ""
if "llm_model" not in st.session_state:
    st.session_state.llm_model = DEFAULT_MODELS[st.session_state.llm_provider][0]

# Spark States
if "spark_found" not in st.session_state:
    st.session_state.spark_found = False
    st.session_state.spark_home_discovered = None
    st.session_state.spark_found_method = "Nenhum"
    if not st.session_state.spark_found:
        initialize_spark_environment()

st.sidebar.title("⚙️ Configurações")
st.sidebar.markdown("---")

st.sidebar.subheader("Modelo de Linguagem (LLM)")
selected_provider = st.sidebar.selectbox(
    "Provedor:",
    options=ALL_PROVIDERS,
    key="llm_provider_widget",  # Chave diferente para o widget
    index=ALL_PROVIDERS.index(st.session_state.llm_provider),  # Define valor inicial
    on_change=lambda: st.session_state.update(
        llm_provider=st.session_state.llm_provider_widget,
        # Resetar modelo ao trocar provedor para evitar incompatibilidade
        llm_model=DEFAULT_MODELS[st.session_state.llm_provider_widget][0],
    ),
)

api_key_label = f"{selected_provider} API Key:"
env_api_key = ""

if selected_provider == "OpenAI":
    env_api_key = os.environ.get("OPENAI_API_KEY", "")
elif selected_provider == "Google":
    env_api_key = os.environ.get("GOOGLE_API_KEY", "")  # Ou outro nome que usar
elif selected_provider == "Anthropic":
    env_api_key = os.environ.get("ANTHROPIC_API_KEY", "")

current_api_key = st.session_state.llm_api_key or env_api_key

api_key_input = st.sidebar.text_input(
    api_key_label,
    type="password",
    key="llm_api_key_widget",
    value=current_api_key,  # Mostra a chave atual (mascarada)
    help=f"Sua chave de API para {selected_provider}. Pode ser definida via variável de ambiente.",
    on_change=lambda: st.session_state.update(
        llm_api_key=st.session_state.llm_api_key_widget
    ),
)
if api_key_input != current_api_key:
    st.session_state.llm_api_key = api_key_input

available_models = DEFAULT_MODELS.get(selected_provider, ["(Nenhum modelo padrão)"])
if st.session_state.llm_model not in available_models:
    st.session_state.llm_model = available_models[0]

selected_model = st.sidebar.selectbox(
    "Modelo:",
    options=available_models,
    key="llm_model_widget",
    index=available_models.index(st.session_state.llm_model),  # Define valor inicial
    on_change=lambda: st.session_state.update(
        llm_model=st.session_state.llm_model_widget
    ),
)
if selected_model != st.session_state.llm_model:
    st.session_state.llm_model = selected_model

if st.session_state.llm_api_key:
    st.sidebar.success(f"API Key {selected_provider} fornecida.")
else:
    st.sidebar.warning(f"API Key {selected_provider} não configurada.")
st.sidebar.caption(f"Usando modelo: {st.session_state.llm_model}")


st.sidebar.markdown("---")
st.sidebar.subheader("Ambiente Spark")
if st.session_state.get("spark_found", False):
    st.sidebar.success(f"✅ Spark Encontrado!")
    st.sidebar.caption(f"Método: {st.session_state.spark_found_method}")
    st.sidebar.caption(f"SPARK_HOME: {st.session_state.spark_home_discovered}")
else:
    st.sidebar.error("❌ Spark Não Encontrado!")
    st.sidebar.caption("Verifique a instalação do Spark ou defina SPARK_HOME.")

st.sidebar.markdown("---")
st.sidebar.title("⚠️ Avisos Importantes")
st.sidebar.warning(
    """*   **RISCO DE EXECUÇÃO:** Experimental. **REVISE CÓDIGO.**\n*   **ADAPTAÇÃO:** Código PySpark gerado **pode precisar de ajustes**.\n*   **PRECISÃO IA:** Documentação e geração/parsing de amostras não são perfeitas.\n*   **AMOSTRAGEM:** Geração automática é **fictícia**.\n*   **CUSTOS API:** Uso das APIs LLM pode gerar custos."""
)
st.sidebar.info("Versão 4.2: Multi-LLM (OpenAI, Google, Anthropic).")


tab1, tab2, tab3 = st.tabs(
    [
        "1. SAS Input & Documentação",
        "2. Código PySpark Gerado",
        "3. Resultados da Execução",
    ]
)

with tab1:
    st.header("1. Código SAS")
    default_sas_code_v4_2 = """
/* Exemplo SAS v4.2: Inline + Referência Externa */
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
/* Objetivo: Relacionar vendas com detalhes de produto para clientes do Sudeste */
PROC SQL; CREATE TABLE WORK.RELATORIO_COMPLETO AS SELECT t1.NOME, t1.CIDADE, t2.PRODUTO, t2.VALOR, t3.CATEGORIA, t3.FORNECEDOR FROM WORK.CLIENTES t1 INNER JOIN WORK.VENDAS t2 ON t1.ID_CLIENTE = t2.ID_CLIENTE LEFT JOIN WORK.PRODUTOS_DETALHES t3 ON t2.PRODUTO = t3.NOME_PRODUTO WHERE t1.REGIAO = 'Sudeste' ORDER BY t1.NOME, t2.PRODUTO; QUIT;
PROC PRINT DATA=WORK.RELATORIO_COMPLETO NOOBS; TITLE "Relatório Completo Vendas Sudeste"; RUN;
/* Analisar margens da tabela externa */
PROC MEANS DATA=WORK.PRODUTOS_DETALHES NWAY NOPRINT; CLASS CATEGORIA; VAR MARGEM_LUCRO; OUTPUT OUT=WORK.MARGENS_POR_CATEGORIA (DROP=_TYPE_ _FREQ_) MEAN=MARGEM_MEDIA; RUN;
PROC PRINT DATA=WORK.MARGENS_POR_CATEGORIA NOOBS; TITLE "Margem Média por Categoria"; RUN;
"""
    if (
        "sas_code" not in st.session_state
        or not st.session_state.sas_code
        or st.session_state.sas_code == "/* Cole seu código SAS aqui */"
    ):
        st.session_state.sas_code = default_sas_code_v4_2

    sas_input = st.text_area(
        "Cole seu código SAS:",
        height=300,
        key="sas_code_input",
        value=st.session_state.sas_code,
    )
    if sas_input != st.session_state.sas_code:
        st.session_state.sas_code = sas_input
        st.session_state.agent1_run_complete = False
        st.session_state.show_transpile_button = False
        st.session_state.pyspark_code = ""
        st.session_state.execution_ran = False
        st.session_state.markdown_documentation = ""
        st.session_state.sample_data_structure = {}
        st.rerun()

    if st.button(
        f"📊 Analisar SAS, Gerar Dados Estruturados e Documentar (Agente 1 - {st.session_state.llm_provider})",
        disabled=not st.session_state.sas_code.strip(),
    ):
        llm_client, error_init = initialize_llm_client(
            st.session_state.llm_provider, st.session_state.llm_api_key
        )
        if llm_client:
            with st.spinner(
                f"🧠 Agente 1 ({st.session_state.llm_model}) Analisando, Processando Dados e Documentando..."
            ):
                md_doc, structured_data = agent_1_orchestrator(
                    client=llm_client,
                    provider=st.session_state.llm_provider,
                    model_name=st.session_state.llm_model,
                    sas_code=st.session_state.sas_code,
                )
                st.session_state.markdown_documentation = md_doc
                st.session_state.sample_data_structure = structured_data
                st.session_state.agent1_run_complete = True
                st.session_state.show_transpile_button = True
                st.session_state.pyspark_code = ""
                st.session_state.execution_ran = False
                st.rerun()
        else:
            st.error(f"Falha ao inicializar cliente LLM para Agente 1: {error_init}")

    if st.session_state.agent1_run_complete:
        st.header("2. Documentação Gerada (Editável para Contexto)")
        st.caption(
            f"Edite o texto abaixo (contexto para {st.session_state.llm_provider}). Tabelas são para visualização."
        )

        def update_doc_state():
            st.session_state.markdown_documentation = (
                st.session_state.doc_editor_context_v4_2
            )

        edited_doc_input = st.text_area(
            "Edite a documentação (Markdown - para CONTEXTO da lógica):",
            value=st.session_state.markdown_documentation,
            height=300,
            key="doc_editor_context_v4_2",
            on_change=update_doc_state,
        )

        if st.session_state.show_transpile_button:
            transpile_disabled = not st.session_state.sample_data_structure
            transpile_help = (
                "Transpilar usando dados estruturados e contexto MD editado."
                if not transpile_disabled
                else "Execute Agente 1."
            )
            if st.button(
                f"✨ Transpilar para PySpark (Agente 2 - {st.session_state.llm_provider})",
                type="primary",
                disabled=transpile_disabled,
                help=transpile_help,
            ):
                # Inicializa cliente ANTES de chamar o agente
                llm_client, error_init = initialize_llm_client(
                    st.session_state.llm_provider, st.session_state.llm_api_key
                )
                if llm_client:
                    with st.spinner(
                        f"🔄 Agente 2 ({st.session_state.llm_model}) Transpilando (dados estruturados + contexto MD)..."
                    ):
                        st.session_state.pyspark_code = agent_2_transpile_sas_using_structure_and_context(
                            client=llm_client,
                            provider=st.session_state.llm_provider,
                            model_name=st.session_state.llm_model,
                            sas_code=st.session_state.sas_code,
                            sample_data=st.session_state.sample_data_structure,
                            edited_markdown_context=st.session_state.markdown_documentation,
                        )
                        st.session_state.execution_ran = False
                        st.rerun()
                else:
                    st.error(
                        f"Falha ao inicializar cliente LLM para Agente 2: {error_init}"
                    )

    st.markdown("---")
    st.header("Pré-visualização da Documentação (Conforme Editada)")
    doc_to_show = st.session_state.markdown_documentation
    if doc_to_show:
        st.markdown(doc_to_show, unsafe_allow_html=True)
    else:
        st.info(
            "Documentação e tabelas de amostra (para visualização) aparecerão aqui."
        )
    if st.session_state.sample_data_structure:
        with st.expander("Ver Estrutura de Dados (Usada para Gerar DataFrames)"):
            st.json(st.session_state.sample_data_structure)

with tab2:
    if st.session_state.pyspark_code:
        st.header("3. Código PySpark Gerado")
        st.caption(
            f"Código gerado por {st.session_state.llm_provider} ({st.session_state.llm_model}) usando dados estruturados e contexto MD."
        )
        st.code(st.session_state.pyspark_code, language="python", line_numbers=True)
        st.warning("⚠️ **Aviso:** Verifique o código gerado. Execução experimental.")
        run_button_disabled = (
            not st.session_state.pyspark_code.strip()
            or not st.session_state.get("spark_found", False)
        )
        button_tooltip = (
            "Execução desabilitada: Verifique o código e o status do Spark."
            if run_button_disabled
            else "Tentar executar via spark-submit."
        )
        if st.button(
            "🚀 Tentar Executar PySpark (Agente 3)",
            disabled=run_button_disabled,
            help=button_tooltip,
        ):
            with st.spinner("⏳ Agente 3 tentando executar via spark-submit..."):
                stdout, stderr, retcode = agent_3_execute_pyspark(
                    st.session_state.pyspark_code
                )
                st.session_state.execution_stdout, st.session_state.execution_stderr = (
                    stdout,
                    stderr,
                )
                (
                    st.session_state.execution_return_code,
                    st.session_state.execution_ran,
                ) = (retcode, True)
                st.rerun()
    elif st.session_state.agent1_run_complete:
        st.info("Clique em 'Transpilar para PySpark (Agente 2)' na Aba 1.")
    else:
        st.info("Insira o código SAS e clique em 'Analisar...' na Aba 1 para começar.")

with tab3:
    st.header("4. Resultados da Execução (Agente 3)")
    if not st.session_state.execution_ran:
        st.info(
            "Resultados da execução (stdout/stderr) do `spark-submit` aparecerão aqui."
        )
    else:
        ret_code = st.session_state.execution_return_code
        status_msg = f"Status: {'Sucesso ✅' if ret_code == 0 else 'Falha ❌'} (Código de Retorno: {ret_code})"
        if ret_code == 0:
            st.success(status_msg)
        else:
            st.error(status_msg)
        if ret_code != 0:
            error_map = {
                -2: "Falha Crítica: 'spark-submit' não encontrado.",
                -3: "Falha Crítica: Erro interno no subprocess.",
                -4: "Falha: Timeout.",
                -5: "Falha Crítica: SPARK_HOME não encontrado.",
                -6: f"Falha Crítica: spark-submit não encontrado no SPARK_HOME.",
            }
            default_error = f"Processo spark-submit terminou com erro ({ret_code}). Verifique stderr."
            st.warning(error_map.get(ret_code, default_error))
        tab_stdout, tab_stderr = st.tabs(
            ["Standard Output (stdout)", "Standard Error (stderr)"]
        )
        with tab_stdout:
            st.caption("Saída padrão.")
            st.code(st.session_state.execution_stdout or "(Vazio)", language="log")
        with tab_stderr:
            st.caption("Saída de erro (logs Spark, tracebacks).")
            st.code(st.session_state.execution_stderr or "(Vazio)", language="log")
