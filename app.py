from __future__ import annotations

import os
from typing import Any

import streamlit as st
from databricks.sdk import WorkspaceClient


ENDPOINT_ENV = "SERVING_ENDPOINT"
REQUEST_FORMAT_ENV = "AGENT_REQUEST_FORMAT"
DEFAULT_REQUEST_FORMAT = "responses"
DEFAULT_PROMPTS = [
    "Summarize current business performance.",
    "What should I look at first today?",
    "Find the biggest risks and recommended next actions.",
]


st.set_page_config(
    page_title="Supervisor Agent Chat",
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


def initialize_state() -> None:
    st.session_state.setdefault("messages", [])
    st.session_state.setdefault("pending_prompt", None)
    st.session_state.setdefault("last_raw_response", None)


def reset_chat() -> None:
    st.session_state["messages"] = []
    st.session_state["pending_prompt"] = None
    st.session_state["last_raw_response"] = None


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


def queue_prompt(prompt: str) -> None:
    st.session_state["pending_prompt"] = prompt


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
    client = get_workspace_client()
    query_client = getattr(client, "serving_endpoints_data_plane", None)
    if query_client is None:
        query_client = client.serving_endpoints

    try:
        return query_client.query(name=endpoint_name, **payload)
    except TypeError:
        path = f"/serving-endpoints/{endpoint_name}/invocations"
        return client.api_client.do("POST", path, body=payload)


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
    if isinstance(choices, list) and choices:
        for choice in choices:
            text = first_text(choice.get("message") if isinstance(choice, dict) else choice)
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

    suggestions: list[str] = []
    for item in raw_suggestions:
        if isinstance(item, str) and item.strip():
            suggestions.append(item.strip())
    return suggestions[:4]


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

endpoint_name = os.getenv(ENDPOINT_ENV, "").strip()
workspace_host = os.getenv("DATABRICKS_HOST", "Not set")
app_name = os.getenv("DATABRICKS_APP_NAME", "Local development")
request_format = os.getenv(REQUEST_FORMAT_ENV, DEFAULT_REQUEST_FORMAT).strip() or DEFAULT_REQUEST_FORMAT

with st.sidebar:
    st.title("Supervisor Agent")
    st.caption("A Streamlit app for Databricks Apps backed by a serving endpoint resource.")

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

    render_auth_debug()

    st.subheader("Try a prompt")
    for prompt in DEFAULT_PROMPTS:
        if st.button(prompt, key=f"default-prompt-{prompt}", use_container_width=True):
            queue_prompt(prompt)
            st.rerun()

st.title("Chat with Supervisor Agent")
st.caption("Ask a question and keep the conversation going through your Databricks serving endpoint.")

if not endpoint_name:
    st.error(
        "This app needs a Databricks Apps serving endpoint resource. Add the resource "
        f"in the app configuration and map it to `{ENDPOINT_ENV}` in `app.yaml`."
    )
    st.info(
        "Resource key expected by this sample: `serving-endpoint`. Grant the app "
        "`Can query` on the endpoint."
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
            except Exception as exc:  # noqa: BLE001
                assistant_message = {
                    "role": "assistant",
                    "content": "I could not complete that request against the serving endpoint.",
                    "raw_response": {"error": str(exc)},
                    "suggestions": [],
                }

        render_assistant_message(
            assistant_message,
            len(st.session_state["messages"]),
        )

    st.session_state["messages"].append(assistant_message)
