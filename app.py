from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import psycopg
import requests
import streamlit as st
from databricks.sdk import WorkspaceClient
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


ENDPOINT_ENV = "SERVING_ENDPOINT"
LAKEBASE_ENDPOINT_ENV = "LAKEBASE_ENDPOINT_NAME"
REQUEST_FORMAT_ENV = "AGENT_REQUEST_FORMAT"

DEFAULT_REQUEST_FORMAT = "responses"
DEFAULT_LAKEBASE_SCHEMA = "silver_pharma_sales"
DEFAULT_LAKEBASE_TABLE = "synced_silver_pharma_sales"
DEFAULT_LAKEBASE_ROW_LIMIT = 50

DEFAULT_PROMPTS = [
    "Summarize current business performance.",
    "What should I look at first today?",
    "Find the biggest risks and recommended next actions.",
]


st.set_page_config(
    page_title="Supervisor Agent Chat",
    page_icon="💬",
    layout="wide",
)


# -----------------------------
# Auth helpers
# -----------------------------
def normalize_host(host: str) -> str:
    host = host.strip().rstrip("/")
    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


def get_forwarded_user_token() -> str:
    headers = getattr(st.context, "headers", None)
    if not headers:
        return ""

    token = (
        headers.get("x-forwarded-access-token", "")
        or headers.get("X-Forwarded-Access-Token", "")
    )
    return token.strip() if token else ""


def get_auth_mode() -> str:
    host = os.getenv("DATABRICKS_HOST", "").strip()
    user_token = get_forwarded_user_token()
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()
    token = os.getenv("DATABRICKS_TOKEN", "").strip()

    if host and user_token:
        return "user-authorization"
    if host and client_id and client_secret:
        return "app-authorization"
    if host and token:
        return "token-fallback"
    return "auto-detect"


def get_workspace_client() -> WorkspaceClient:
    host = normalize_host(os.getenv("DATABRICKS_HOST", ""))
    forwarded_user_token = get_forwarded_user_token()

    if host and forwarded_user_token:
        return WorkspaceClient(
            host=host,
            token=forwarded_user_token,
            auth_type="pat",
        )

    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()
    token = os.getenv("DATABRICKS_TOKEN", "").strip()

    if host and client_id and client_secret:
        return WorkspaceClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            auth_type="oauth-m2m",
        )

    if host and token:
        return WorkspaceClient(
            host=host,
            token=token,
            auth_type="pat",
        )

    return WorkspaceClient()


# -----------------------------
# Streamlit state
# -----------------------------
def initialize_state() -> None:
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("pending_prompt", None)
    st.session_state.setdefault("last_raw_response", None)


def reset_chat() -> None:
    st.session_state["messages"] = []
    st.session_state["pending_prompt"] = None
    st.session_state["last_raw_response"] = None


def queue_prompt(prompt: str) -> None:
    st.session_state["pending_prompt"] = prompt


# -----------------------------
# Serialization helpers
# -----------------------------
def serialize_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list):
        return [serialize_value(item) for item in value]

    if isinstance(value, tuple):
        return [serialize_value(item) for item in value]

    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}

    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, (str, int, float, bool)):
        return enum_value

    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        return serialize_value(as_dict())

    if hasattr(value, "__dict__"):
        return {
            key: serialize_value(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }

    return str(value)


# -----------------------------
# Serving endpoint helpers
# -----------------------------
def chat_history_for_agent() -> list[dict[str, str]]:
    history: list[dict[str, str]] = []

    for message in st.session_state["messages"]:
        role = message.get("role")
        content = message.get("content")

        if role in {"user", "assistant"} and isinstance(content, str) and content.strip():
            history.append({"role": role, "content": content.strip()})

    return history


def build_invocation_payload(messages: list[dict[str, str]]) -> dict[str, Any]:
    request_format = os.getenv(REQUEST_FORMAT_ENV, DEFAULT_REQUEST_FORMAT).strip().lower()
    latest_prompt = messages[-1]["content"] if messages else ""

    if request_format in {"responses", "response"}:
        return {"input": messages}

    if request_format in {"chat", "messages", "chat-completions"}:
        return {"messages": messages}

    if request_format in {"inputs", "prompt"}:
        return {"inputs": {"prompt": latest_prompt, "messages": messages}}

    raise RuntimeError(
        f"Unsupported {REQUEST_FORMAT_ENV} value `{request_format}`. "
        "Use `responses`, `chat`, or `inputs`."
    )


def invoke_serving_endpoint(endpoint_name: str, payload: dict[str, Any]) -> Any:
    host = normalize_host(os.getenv("DATABRICKS_HOST", ""))
    if not host:
        raise RuntimeError("DATABRICKS_HOST is not set.")

    token = get_forwarded_user_token()
    if not token:
        raise RuntimeError(
            "No forwarded user token was found. Enable Databricks Apps user "
            "authorization and grant the `model-serving` scope."
        )

    endpoint_path = quote(endpoint_name, safe="")
    url = f"{host}/serving-endpoints/{endpoint_path}/invocations"

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = response.text

    if response.ok:
        return response_payload

    raise RuntimeError(
        f"Serving endpoint request failed with HTTP {response.status_code}: "
        f"{response_payload}"
    )


def first_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, (int, float, bool)):
        return ""

    if isinstance(value, list):
        for item in value:
            text = first_text(item)
            if text:
                return text
        return ""

    if not isinstance(value, dict):
        return first_text(serialize_value(value))

    for key in ("output_text", "text", "content", "answer", "response", "prediction"):
        text = first_text(value.get(key))
        if text:
            return text

    choices = value.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            item = choice.get("message") if isinstance(choice, dict) else choice
            text = first_text(item)
            if text:
                return text

    output = value.get("output")
    if isinstance(output, list):
        output_parts: list[str] = []

        for item in output:
            if isinstance(item, dict):
                for content_item in item.get("content", []):
                    text = first_text(content_item)
                    if text:
                        output_parts.append(text)
            else:
                text = first_text(item)
                if text:
                    output_parts.append(text)

        if output_parts:
            return "\n\n".join(output_parts)

    predictions = value.get("predictions")
    if isinstance(predictions, list) and predictions:
        return first_text(predictions)

    return ""


def extract_suggestions(response: Any) -> list[str]:
    payload = serialize_value(response)
    if not isinstance(payload, dict):
        return []

    raw_suggestions = (
        payload.get("suggestions")
        or payload.get("suggested_questions")
        or payload.get("follow_up_questions")
        or []
    )

    if not isinstance(raw_suggestions, list):
        return []

    return [
        item.strip()
        for item in raw_suggestions
        if isinstance(item, str) and item.strip()
    ][:4]


def build_assistant_message(response: Any) -> dict[str, Any]:
    payload = serialize_value(response)
    content = first_text(payload)

    if not content:
        content = "The supervisor agent completed the request but did not return text."

    return {
        "role": "assistant",
        "content": content,
        "raw_response": payload,
        "suggestions": extract_suggestions(payload),
    }


def ask_agent() -> dict[str, Any]:
    endpoint_name = os.getenv(ENDPOINT_ENV, "").strip()
    if not endpoint_name:
        raise RuntimeError(
            f"{ENDPOINT_ENV} is not set. Add a serving endpoint resource to the "
            "Databricks app and expose it in app.yaml."
        )

    messages = chat_history_for_agent()
    payload = build_invocation_payload(messages)
    response = invoke_serving_endpoint(endpoint_name, payload)

    st.session_state["last_raw_response"] = serialize_value(response)

    return build_assistant_message(response)


# -----------------------------
# Lakebase helpers
# -----------------------------
class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo: str = "", **kwargs: Any) -> "OAuthConnection":
        password = os.getenv("PGPASSWORD", "").strip()

        if password:
            kwargs["password"] = password
            return super().connect(conninfo, **kwargs)

        endpoint_name = os.getenv(LAKEBASE_ENDPOINT_ENV, "").strip()
        if not endpoint_name:
            raise RuntimeError(
                f"PGPASSWORD is not set and {LAKEBASE_ENDPOINT_ENV} is not set. "
                "Attach a Lakebase/Postgres database resource to the Databricks App."
            )

        credential = get_workspace_client().postgres.generate_database_credential(
            endpoint=endpoint_name
        )

        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


def lakebase_connection_info() -> dict[str, str]:
    return {
        "endpoint": os.getenv(LAKEBASE_ENDPOINT_ENV, "").strip(),
        "host": os.getenv("PGHOST", "").strip(),
        "database": os.getenv("PGDATABASE", "").strip(),
        "user": os.getenv("PGUSER", "").strip(),
        "password": os.getenv("PGPASSWORD", "").strip(),
        "port": os.getenv("PGPORT", "5432").strip() or "5432",
        "sslmode": os.getenv("PGSSLMODE", "require").strip() or "require",
    }


def lakebase_is_configured() -> bool:
    info = lakebase_connection_info()

    has_connection = all(info[key] for key in ("host", "database", "user"))
    has_auth = bool(info["password"] or info["endpoint"])

    return has_connection and has_auth


@st.cache_resource(show_spinner=False)
def get_lakebase_pool(
    endpoint_name: str,
    host: str,
    database: str,
    user: str,
    port: str,
    sslmode: str,
) -> ConnectionPool:
    _ = endpoint_name

    conninfo = (
        f"dbname={database} "
        f"user={user} "
        f"host={host} "
        f"port={port} "
        f"sslmode={sslmode}"
    )

    return ConnectionPool(
        conninfo=conninfo,
        connection_class=OAuthConnection,
        kwargs={"row_factory": dict_row},
        min_size=1,
        max_size=5,
        open=True,
    )


def get_configured_lakebase_pool() -> ConnectionPool:
    info = lakebase_connection_info()

    required_keys = ["host", "database", "user"]
    if not info["password"]:
        required_keys.append("endpoint")

    missing = [key for key in required_keys if not info[key]]
    if missing:
        raise RuntimeError(
            "Lakebase is missing required environment variables: "
            + ", ".join(missing)
        )

    return get_lakebase_pool(
        endpoint_name=info["endpoint"],
        host=info["host"],
        database=info["database"],
        user=info["user"],
        port=info["port"],
        sslmode=info["sslmode"],
    )


def fetch_lakebase_schemas() -> list[str]:
    pool = get_configured_lakebase_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                  AND schema_name NOT LIKE 'pg_toast%'
                ORDER BY schema_name
                """
            )
            return [row["schema_name"] for row in cur.fetchall()]


def fetch_lakebase_tables() -> list[dict[str, Any]]:
    pool = get_configured_lakebase_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            )
            return list(cur.fetchall())


def fetch_synced_lakebase_tables() -> list[dict[str, Any]]:
    pool = get_configured_lakebase_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND table_name LIKE 'synced_%'
                ORDER BY table_schema, table_name
                """
            )
            return list(cur.fetchall())


def fetch_lakebase_columns(schema_name: str, table_name: str) -> list[dict[str, Any]]:
    pool = get_configured_lakebase_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            return list(cur.fetchall())


def fetch_filtered_lakebase_rows(
    schema_name: str,
    table_name: str,
    column_name: str | None,
    operator: str,
    filter_value: str,
    limit: int,
) -> list[dict[str, Any]]:
    pool = get_configured_lakebase_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )

            params: list[Any] = []

            if column_name and operator != "No filter":
                column = sql.Identifier(column_name)

                if operator == "equals":
                    where_clause = sql.SQL("{} = %s").format(column)
                    params.append(filter_value)

                elif operator == "not equals":
                    where_clause = sql.SQL("{} <> %s").format(column)
                    params.append(filter_value)

                elif operator == "contains":
                    where_clause = sql.SQL("{}::text ILIKE %s").format(column)
                    params.append(f"%{filter_value}%")

                elif operator == "starts with":
                    where_clause = sql.SQL("{}::text ILIKE %s").format(column)
                    params.append(f"{filter_value}%")

                elif operator == "greater than":
                    where_clause = sql.SQL("{} > %s").format(column)
                    params.append(filter_value)

                elif operator == "less than":
                    where_clause = sql.SQL("{} < %s").format(column)
                    params.append(filter_value)

                elif operator == "is null":
                    where_clause = sql.SQL("{} IS NULL").format(column)

                elif operator == "is not null":
                    where_clause = sql.SQL("{} IS NOT NULL").format(column)

                else:
                    raise RuntimeError(f"Unsupported filter operator: {operator}")

                query = sql.SQL("{} WHERE {}").format(query, where_clause)

            query = sql.SQL("{} LIMIT %s").format(query)
            params.append(limit)

            cur.execute(query, params)
            return list(cur.fetchall())


# -----------------------------
# UI renderers
# -----------------------------
def render_auth_debug() -> None:
    st.subheader("Auth Debug")

    forwarded_user_token = get_forwarded_user_token()

    st.write("Auth mode:", get_auth_mode())
    st.write("User token present:", bool(forwarded_user_token))

    try:
        me = get_workspace_client().current_user.me()
        st.write("Current caller:")
        st.json(serialize_value(me))
    except Exception as exc:
        st.error(f"Error fetching identity: {exc}")


def render_assistant_message(message: dict[str, Any], message_index: int) -> None:
    st.markdown(message["content"])

    suggestions = message.get("suggestions") or []
    if suggestions:
        st.caption("Suggested follow-up questions")

        for suggestion_index, suggestion in enumerate(suggestions):
            if st.button(
                suggestion,
                key=f"suggestion-{message_index}-{suggestion_index}",
                use_container_width=True,
            ):
                queue_prompt(suggestion)
                st.rerun()

    raw_response = message.get("raw_response")
    if raw_response:
        with st.expander("Raw endpoint response", expanded=False):
            st.json(raw_response)


def render_chat_history() -> None:
    for index, message in enumerate(st.session_state["messages"]):
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                render_assistant_message(message, index)
            else:
                st.markdown(message["content"])


def render_lakebase_browser() -> None:
    st.subheader("Lakebase Browser")

    info = lakebase_connection_info()

    connection_cols = st.columns(4)
    connection_cols[0].metric("Database", info["database"] or "Missing")
    connection_cols[1].metric("Host", info["host"] or "Missing")
    connection_cols[2].metric("User", info["user"] or "Missing")
    connection_cols[3].metric("Endpoint", "Ready" if info["endpoint"] else "Missing")

    if not lakebase_is_configured():
        st.warning(
            "Lakebase is not configured yet. Attach a Lakebase/Postgres database "
            "resource to this Databricks App and redeploy."
        )
        return

    try:
        schemas = fetch_lakebase_schemas()
        tables = fetch_lakebase_tables()
        synced_tables = fetch_synced_lakebase_tables()
    except Exception as exc:
        st.error(f"Could not fetch Lakebase metadata: {exc}")
        return

    with st.expander("Discovered synced tables", expanded=True):
        if synced_tables:
            st.dataframe(synced_tables, use_container_width=True, hide_index=True)
        else:
            st.info("No synced tables were found with table name LIKE 'synced_%'.")

    if not schemas:
        st.warning("No Lakebase schemas found.")
        return

    default_schema_index = 0
    if DEFAULT_LAKEBASE_SCHEMA in schemas:
        default_schema_index = schemas.index(DEFAULT_LAKEBASE_SCHEMA)

    selected_schema = st.selectbox(
        "Schema",
        schemas,
        index=default_schema_index,
    )

    schema_tables = [
        table
        for table in tables
        if table.get("table_schema") == selected_schema
    ]

    has_synced_tables = any(
        str(table["table_name"]).startswith("synced_")
        for table in schema_tables
    )

    show_synced_only = st.checkbox(
        "Show synced tables only",
        value=has_synced_tables,
        disabled=not has_synced_tables,
    )

    if show_synced_only:
        schema_tables = [
            table
            for table in schema_tables
            if str(table["table_name"]).startswith("synced_")
        ]

    table_options = [str(table["table_name"]) for table in schema_tables]

    if not table_options:
        st.info(f"No tables found in schema `{selected_schema}`.")
        selected_table = st.text_input("Table", value=DEFAULT_LAKEBASE_TABLE)
    else:
        default_table_index = 0
        if DEFAULT_LAKEBASE_TABLE in table_options:
            default_table_index = table_options.index(DEFAULT_LAKEBASE_TABLE)

        selected_table = st.selectbox(
            "Table",
            table_options,
            index=default_table_index,
        )

    if not selected_schema or not selected_table:
        return

    try:
        columns = fetch_lakebase_columns(selected_schema, selected_table)
    except Exception as exc:
        st.warning(f"Could not fetch columns: {exc}")
        columns = []

    st.markdown("#### Filter")

    filter_cols = st.columns([1.3, 1.1, 1.6])

    column_options = [""] + [
        str(column["column_name"])
        for column in columns
        if column.get("column_name")
    ]

    selected_column = filter_cols[0].selectbox(
        "Column",
        column_options,
        format_func=lambda value: "No filter" if not value else value,
    )

    operator = filter_cols[1].selectbox(
        "Operator",
        [
            "No filter",
            "equals",
            "not equals",
            "contains",
            "starts with",
            "greater than",
            "less than",
            "is null",
            "is not null",
        ],
        disabled=not selected_column,
    )

    value_required = operator not in {"No filter", "is null", "is not null"}

    filter_value = filter_cols[2].text_input(
        "Value",
        disabled=not selected_column or not value_required,
    )

    if columns:
        with st.expander("Columns", expanded=False):
            st.dataframe(columns, use_container_width=True, hide_index=True)

    row_limit = st.number_input(
        "Rows to fetch",
        min_value=1,
        max_value=500,
        value=DEFAULT_LAKEBASE_ROW_LIMIT,
        step=10,
    )

    query_preview = f'SELECT * FROM "{selected_schema}"."{selected_table}" LIMIT {row_limit}'
    st.code(query_preview, language="sql")

    if st.button("Fetch Lakebase rows", use_container_width=False):
        if selected_column and value_required and not filter_value:
            st.error("Enter a filter value.")
            return

        try:
            rows = fetch_filtered_lakebase_rows(
                schema_name=selected_schema,
                table_name=selected_table,
                column_name=selected_column or None,
                operator=operator,
                filter_value=filter_value,
                limit=int(row_limit),
            )
        except Exception as exc:
            st.error(
                f"Could not fetch rows from {selected_schema}.{selected_table}: {exc}"
            )
            return

        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info(f"{selected_schema}.{selected_table} returned no rows.")


# -----------------------------
# Main app
# -----------------------------
initialize_state()

endpoint_name = os.getenv(ENDPOINT_ENV, "").strip()
workspace_host = os.getenv("DATABRICKS_HOST", "Not set")
app_name = os.getenv("DATABRICKS_APP_NAME", "Local development")
request_format = (
    os.getenv(REQUEST_FORMAT_ENV, DEFAULT_REQUEST_FORMAT).strip()
    or DEFAULT_REQUEST_FORMAT
)
lakebase_endpoint_name = os.getenv(LAKEBASE_ENDPOINT_ENV, "").strip()

with st.sidebar:
    st.title("Supervisor Agent")
    st.caption("Streamlit app backed by Databricks serving endpoint + Lakebase.")

    if st.button("Start new conversation", use_container_width=True):
        reset_chat()
        st.rerun()

    st.subheader("Connection")
    st.write(f"App: `{app_name}`")
    st.write(f"Workspace: `{workspace_host}`")
    st.write(f"Auth mode: `{get_auth_mode()}`")
    st.write(f"Request format: `{request_format}`")

    if endpoint_name:
        st.success(f"Serving endpoint ready: `{endpoint_name}`")
    else:
        st.error(f"{ENDPOINT_ENV} is missing")

    if lakebase_endpoint_name:
        st.success(f"Lakebase endpoint ready: `{lakebase_endpoint_name}`")
    else:
        st.warning(f"{LAKEBASE_ENDPOINT_ENV} is missing")

    render_auth_debug()

    st.subheader("Try a prompt")
    for prompt in DEFAULT_PROMPTS:
        if st.button(prompt, key=f"default-prompt-{prompt}", use_container_width=True):
            queue_prompt(prompt)
            st.rerun()

chat_tab, lakebase_tab = st.tabs(["Supervisor Agent", "Lakebase"])

with lakebase_tab:
    render_lakebase_browser()

with chat_tab:
    st.title("Chat with Supervisor Agent")
    st.caption(
        "Ask questions through your Databricks serving endpoint. "
        "Use the Lakebase tab to inspect synced tables."
    )

    if not endpoint_name:
        st.error(
            "This app needs a Databricks Apps serving endpoint resource. "
            f"Expose it as `{ENDPOINT_ENV}` in app.yaml."
        )
        st.stop()

    render_chat_history()

    submitted_prompt = st.chat_input("Ask your supervisor agent")
    queued_prompt = st.session_state.pop("pending_prompt", None)
    prompt = queued_prompt or submitted_prompt

    if prompt:
        user_message = {"role": "user", "content": prompt}
        st.session_state["messages"].append(user_message)

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Supervisor agent is working on it..."):
                try:
                    assistant_message = ask_agent()
                except Exception as exc:
                    assistant_message = {
                        "role": "assistant",
                        "content": (
                            "I could not complete that request against the serving endpoint."
                        ),
                        "raw_response": {"error": str(exc)},
                        "suggestions": [],
                    }

            render_assistant_message(
                assistant_message,
                len(st.session_state["messages"]),
            )

        st.session_state["messages"].append(assistant_message)