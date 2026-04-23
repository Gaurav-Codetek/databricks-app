from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import streamlit as st
from databricks.sdk import WorkspaceClient


SPACE_ID_ENV = "GENIE_SPACE_ID"
REQUEST_TIMEOUT = timedelta(minutes=3)
PREVIEW_ROW_LIMIT = 50
DEFAULT_PROMPTS = [
    "What are the top 10 products by revenue this quarter?",
    "Show weekly sales trend for the last 8 weeks.",
    "Which regions are underperforming versus target this month?",
]


st.set_page_config(
    page_title="Genie Chat App",
    page_icon=":speech_balloon:",
    layout="wide",
)


def get_forwarded_user_token() -> str:
    headers = getattr(st.context, "headers", None)
    if not headers:
        return ""

    token = headers.get("x-forwarded-access-token", "")
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
    """
    Build a Databricks Workspace client.

    Priority:
    1. User authorization via forwarded access token
    2. App/service-principal authorization as fallback
    3. SDK default resolution as last fallback
    """
    host = os.getenv("DATABRICKS_HOST", "").strip()

    # Databricks Apps user authorization forwards the signed-in user's token.
    forwarded_user_token = get_forwarded_user_token()

    if host and forwarded_user_token:
        return WorkspaceClient(
            host=host,
            token=forwarded_user_token,
            auth_type="pat",
        )

    # Fallback to app identity if user token is unavailable.
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


def initialize_state() -> None:
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("conversation_id", None)
    st.session_state.setdefault("pending_prompt", None)


def reset_chat() -> None:
    st.session_state["messages"] = []
    st.session_state["conversation_id"] = None
    st.session_state["pending_prompt"] = None


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


def message_id_for(message: Any) -> str | None:
    return getattr(message, "message_id", None) or getattr(message, "id", None)


def conversation_id_for(message: Any) -> str | None:
    return getattr(message, "conversation_id", None)


def flatten_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    if isinstance(value, (int, float, bool)):
        return []
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            flattened.extend(flatten_strings(item))
        return flattened
    if isinstance(value, dict):
        flattened = []
        for key in (
            "questions",
            "question",
            "content",
            "items",
            "suggested_questions",
        ):
            if key in value:
                flattened.extend(flatten_strings(value[key]))
        return flattened

    serialized = serialize_value(value)
    if isinstance(serialized, (str, list, dict)):
        return flatten_strings(serialized)
    return []


def summarize_error(error: Any) -> str | None:
    payload = serialize_value(error)
    if isinstance(payload, str):
        text = payload.strip()
        return text or None

    if isinstance(payload, dict):
        for key in ("message", "error_message", "detail", "details"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def error_type(error: Any) -> str | None:
    payload = serialize_value(error)
    if isinstance(payload, dict):
        for key in ("error_type", "type", "error_code"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def deduplicate(items: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique_items.append(item)
    return unique_items


def build_row(columns: list[str], row: list[Any]) -> dict[str, Any]:
    row_dict: dict[str, Any] = {}
    for index, value in enumerate(row):
        column_name = columns[index] if index < len(columns) else f"column_{index + 1}"
        row_dict[column_name] = value

    for index in range(len(row), len(columns)):
        row_dict[columns[index]] = None

    return row_dict


def fetch_query_preview(
    client: WorkspaceClient,
    *,
    space_id: str,
    conversation_id: str,
    message_id: str,
    attachment_id: str,
) -> dict[str, Any]:
    try:
        response = client.genie.get_message_attachment_query_result(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id,
        )
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}

    statement = getattr(response, "statement_response", None)
    manifest = getattr(statement, "manifest", None)
    schema = getattr(manifest, "schema", None)
    column_defs = getattr(schema, "columns", None) or []
    columns = [
        getattr(column, "name", None) or f"column_{index + 1}"
        for index, column in enumerate(column_defs)
    ]

    result = getattr(statement, "result", None)
    data_array = getattr(result, "data_array", None) or []
    preview_rows = [build_row(columns, row) for row in data_array[:PREVIEW_ROW_LIMIT]]

    return {
        "rows": preview_rows,
        "chunk_row_count": getattr(result, "row_count", None),
        "total_row_count": getattr(manifest, "total_row_count", None),
        "truncated": bool(getattr(manifest, "truncated", False)),
        "has_more_rows": getattr(result, "next_chunk_index", None) is not None,
        "uses_external_links": bool(getattr(result, "external_links", None)),
        "statement_id": getattr(statement, "statement_id", None),
    }


def build_assistant_message(
    client: WorkspaceClient,
    *,
    space_id: str,
    response: Any,
) -> dict[str, Any]:
    attachments = getattr(response, "attachments", None) or []
    text_parts: list[str] = []
    sql_blocks: list[dict[str, Any]] = []
    suggested_questions: list[str] = []
    conversation_id = conversation_id_for(response)
    message_id = message_id_for(response)

    for attachment in attachments:
        text = getattr(getattr(attachment, "text", None), "content", None)
        if text:
            text_parts.append(text.strip())

        query_attachment = getattr(attachment, "query", None)
        query = getattr(query_attachment, "query", None)
        attachment_id = getattr(attachment, "attachment_id", None)
        if query:
            block: dict[str, Any] = {
                "title": getattr(query_attachment, "title", None) or "Generated SQL",
                "description": getattr(query_attachment, "description", None),
                "query": query,
                "attachment_id": attachment_id,
            }
            if conversation_id and message_id and attachment_id:
                block["preview"] = fetch_query_preview(
                    client,
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    attachment_id=attachment_id,
                )
            sql_blocks.append(block)

        suggested_questions.extend(
            flatten_strings(getattr(attachment, "suggested_questions", None))
        )

    content = "\n\n".join(part for part in text_parts if part).strip()
    error_summary = summarize_error(getattr(response, "error", None))
    error_kind = error_type(getattr(response, "error", None))

    if not content and error_summary:
        content = error_summary
    if not content and error_kind:
        content = f"Genie returned an error: {error_kind}"
    if not content and sql_blocks:
        content = (
            "Genie generated SQL for this request. Expand the section below to "
            "inspect the query and preview the result."
        )
    if not content:
        content = "Genie completed the request but did not return a text summary."

    return {
        "role": "assistant",
        "content": content,
        "status": serialize_value(getattr(response, "status", None)),
        "error": serialize_value(getattr(response, "error", None)),
        "error_type": error_kind,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "sql_blocks": sql_blocks,
        "suggestions": deduplicate(suggested_questions)[:4],
    }


def fetch_failed_message(
    client: WorkspaceClient,
    *,
    space_id: str,
    conversation_id: str | None,
    message_id: str | None,
) -> Any | None:
    if not conversation_id or not message_id:
        return None

    try:
        return client.genie.get_message(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )
    except Exception:  # noqa: BLE001
        return None


def ask_genie(prompt: str) -> dict[str, Any]:
    space_id = os.getenv(SPACE_ID_ENV, "").strip()
    if not space_id:
        raise RuntimeError(
            "GENIE_SPACE_ID is not set. Add a Genie space resource to the "
            "Databricks app and expose it in app.yaml."
        )

    client = get_workspace_client()
    conversation_id = st.session_state.get("conversation_id")

    if conversation_id:
        waiter = client.genie.create_message(
            space_id=space_id,
            conversation_id=conversation_id,
            content=prompt,
        )
    else:
        waiter = client.genie.start_conversation(
            space_id=space_id,
            content=prompt,
        )

    waiter_conversation_id = getattr(waiter, "conversation_id", None) or conversation_id
    waiter_message_id = getattr(waiter, "message_id", None)

    if waiter_conversation_id:
        st.session_state["conversation_id"] = waiter_conversation_id

    try:
        response = waiter.result(timeout=REQUEST_TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        failed_response = fetch_failed_message(
            client,
            space_id=space_id,
            conversation_id=waiter_conversation_id,
            message_id=waiter_message_id,
        )
        if failed_response is not None:
            return build_assistant_message(
                client,
                space_id=space_id,
                response=failed_response,
            )
        raise RuntimeError(str(exc)) from exc

    new_conversation_id = conversation_id_for(response)
    if new_conversation_id:
        st.session_state["conversation_id"] = new_conversation_id

    return build_assistant_message(client, space_id=space_id, response=response)


def queue_prompt(prompt: str) -> None:
    st.session_state["pending_prompt"] = prompt


def render_query_block(block: dict[str, Any], block_index: int) -> None:
    expander_label = block.get("title") or f"Generated SQL {block_index + 1}"
    with st.expander(expander_label, expanded=False):
        description = block.get("description")
        if description:
            st.caption(description)

        st.code(block.get("query", ""), language="sql")

        preview = block.get("preview")
        if not preview:
            return

        if preview.get("error"):
            st.warning(f"Could not load a result preview: {preview['error']}")
            return

        total_row_count = preview.get("total_row_count")
        preview_notes: list[str] = []
        if total_row_count is not None:
            preview_notes.append(f"{total_row_count} total row(s)")
        if preview.get("truncated"):
            preview_notes.append("Result is truncated")
        if preview.get("has_more_rows") or preview.get("uses_external_links"):
            preview_notes.append("Showing only the first available result chunk")
        if preview_notes:
            st.caption(" | ".join(preview_notes))

        rows = preview.get("rows") or []
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True)
            return

        if total_row_count == 0:
            st.caption("The query completed, but no rows were returned.")
            return

        st.caption(
            "A preview table is not available for this response. Open the SQL in "
            "Databricks if you need the full result."
        )


def render_assistant_message(message: dict[str, Any], message_index: int) -> None:
    st.markdown(message["content"])

    if message.get("status") and message.get("status") != "COMPLETED":
        st.caption(f"Message status: {message['status']}")

    if message.get("error_type"):
        st.caption(f"Error type: `{message['error_type']}`")

    error_message = summarize_error(message.get("error"))
    if error_message and error_message != message["content"]:
        st.error(error_message)

    for block_index, block in enumerate(message.get("sql_blocks", [])):
        render_query_block(block, block_index)

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


def render_chat_history() -> None:
    for index, message in enumerate(st.session_state["messages"]):
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                render_assistant_message(message, index)
            else:
                st.markdown(message["content"])


def render_auth_debug() -> None:
    st.subheader("Auth Debug")

    forwarded_user_token = get_forwarded_user_token()
    st.write("Auth mode:", get_auth_mode())
    st.write("User token present:", bool(forwarded_user_token))

    try:
        me = get_workspace_client().current_user.me()
        st.write("Current caller:")
        st.json(serialize_value(me))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error fetching identity: {exc}")


initialize_state()

space_id = os.getenv(SPACE_ID_ENV, "").strip()
workspace_host = os.getenv("DATABRICKS_HOST", "Not set")
app_name = os.getenv("DATABRICKS_APP_NAME", "Local development")

with st.sidebar:
    st.title("Genie Chat")
    st.caption("A Streamlit app for Databricks Apps backed by a Genie space resource.")

    if st.button("Start new conversation", use_container_width=True):
        reset_chat()
        st.rerun()

    st.subheader("Connection")
    st.write(f"App: `{app_name}`")
    st.write(f"Workspace: `{workspace_host}`")
    st.write(f"Auth mode: `{get_auth_mode()}`")
    if space_id:
        st.success(f"Genie space ready: `{space_id}`")
    else:
        st.error("GENIE_SPACE_ID is missing")

    render_auth_debug()

    st.subheader("Try a prompt")
    for prompt in DEFAULT_PROMPTS:
        if st.button(prompt, key=f"default-prompt-{prompt}", use_container_width=True):
            queue_prompt(prompt)
            st.rerun()

st.title("Chat with Genie")
st.caption(
    "Ask a business question, keep the conversation going, and inspect the SQL "
    "that Genie generates for each answer."
)

if not space_id:
    st.error(
        "This app needs a Databricks Apps Genie space resource. Add the resource "
        "in the app configuration and map it to `GENIE_SPACE_ID` in `app.yaml`."
    )
    st.info(
        "Resource key expected by this sample: `genie-space`. Make sure the app "
        "has user authorization enabled and that signed-in users have the required "
        "Unity Catalog privileges on the underlying data."
    )
    st.stop()

render_chat_history()

submitted_prompt = st.chat_input("Ask a question about your data")
queued_prompt = st.session_state.pop("pending_prompt", None)
prompt = queued_prompt or submitted_prompt

if prompt:
    user_message = {"role": "user", "content": prompt}
    st.session_state["messages"].append(user_message)

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Genie is working on it..."):
            try:
                assistant_message = ask_genie(prompt)
            except Exception as exc:  # noqa: BLE001
                assistant_message = {
                    "role": "assistant",
                    "content": (
                        "I could not complete that request against the Genie space."
                    ),
                    "status": "FAILED",
                    "error": str(exc),
                    "sql_blocks": [],
                    "suggestions": [],
                }

        render_assistant_message(
            assistant_message,
            len(st.session_state["messages"]),
        )

    st.session_state["messages"].append(assistant_message)
