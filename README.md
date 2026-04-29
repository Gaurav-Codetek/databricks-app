# Databricks Apps Supervisor Agent Chat

This repository contains a simple Streamlit chat application designed for deployment on the Databricks Apps platform through a GitHub repository. The app uses a Databricks model serving endpoint resource, such as a deployed supervisor agent endpoint.
It also includes a Lakebase browser tab for previewing rows from synced Postgres tables.

## Included files

- `app.py`: Streamlit chat UI with conversation memory and Databricks serving endpoint invocation.
- `app.yaml`: Databricks Apps runtime config that starts Streamlit and injects the serving endpoint and Lakebase resource names.
- `requirements.txt`: Python dependencies installed during deployment.

## How the app works

- You add a serving endpoint as an app resource in the Databricks Apps UI.
- You add a Lakebase database as a Database app resource with resource key `postgres`.
- `app.yaml` maps that resource to `SERVING_ENDPOINT` using `valueFrom: serving-endpoint`.
- `app.yaml` maps the Lakebase resource endpoint path to `LAKEBASE_ENDPOINT_NAME` using `valueFrom: postgres`.
- In Databricks Apps, the Streamlit app uses user authorization by reading the forwarded `x-forwarded-access-token` header.
- Serving endpoint calls use the signed-in user's forwarded Databricks Apps token directly. If no user token is present, the request fails instead of falling back to app credentials.
- The app posts chat history to `/serving-endpoints/<endpoint-name>/invocations`.
- The Lakebase tab uses the app service principal and Databricks-generated database credentials to list Postgres tables and preview rows.

By default, requests use the Databricks agent `ResponsesAgent` style payload:

```json
{
  "input": [
    {"role": "user", "content": "Your question"}
  ]
}
```

If your endpoint expects a different payload, set `AGENT_REQUEST_FORMAT` in `app.yaml`:

- `responses`: sends `{"input": messages}`.
- `chat`: sends `{"messages": messages}`.
- `inputs`: sends `{"inputs": {"prompt": latest_prompt, "messages": messages}}`.

## Local development

To run this outside Databricks Apps, provide the Databricks host and serving endpoint name, then choose one auth method:

- User-style token auth: set `DATABRICKS_TOKEN`
- App/service-principal auth: set `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:DATABRICKS_HOST="https://<your-workspace-host>"
$env:SERVING_ENDPOINT="<serving-endpoint-name>"
streamlit run app.py
```

Example using a local token:

```powershell
$env:DATABRICKS_TOKEN="<user-access-token-or-pat>"
```

Example using app credentials:

```powershell
$env:DATABRICKS_CLIENT_ID="<oauth-client-id>"
$env:DATABRICKS_CLIENT_SECRET="<oauth-client-secret>"
```

## Deploy on Databricks Apps

1. Push this folder to a GitHub repository.
2. In your Databricks workspace, open **Apps** and create or edit your app.
3. Choose **Git repository** as the source and select your GitHub repo and branch.
4. In the app configuration step, add a **Serving endpoint** resource.
5. Grant the app `Can query` on the endpoint.
6. Add a **Database** resource for your Lakebase database.
7. Use resource key `postgres` for the Lakebase database.
8. Grant the app `Can connect and create` on the database resource.
9. Use the default serving endpoint resource key `serving-endpoint`, or update `app.yaml` if you choose a different key.
10. Under **User authorization**, include the `model-serving` scope.
11. Deploy or restart the app after changing resource configuration.
12. Re-consent to the app if Databricks prompts after the scope change.

## Required permissions

The app service principal typically needs:

- `Can query` on the serving endpoint
- `Can connect and create` on the Lakebase database resource
- `SELECT` on any Lakebase synced tables the app should preview
- any downstream data or tool permissions required by the deployed supervisor agent itself

Signed-in users need access to the app, the `model-serving` user authorization scope, and permission to query the endpoint. If the agent uses downstream Databricks resources, users also need the relevant permissions for those resources.

Avoid manually setting `DATABRICKS_TOKEN`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, or `DATABRICKS_CONFIG_PROFILE` for user-authorized endpoint calls. The app reads the forwarded user token from Databricks Apps request headers.
