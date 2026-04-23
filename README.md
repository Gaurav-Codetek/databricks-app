# Databricks Apps Genie Chat Sample

This repository contains a simple Streamlit chat application designed for deployment on the Databricks Apps platform through a GitHub repository. The app uses a Databricks Genie space as an app resource and talks to it with the Databricks Python SDK.

## Included files

- `app.py`: Streamlit chat UI with conversation memory, SQL inspection, and query-result preview.
- `app.yaml`: Databricks Apps runtime config that starts Streamlit and injects the Genie space ID.
- `requirements.txt`: Python dependencies installed during deployment.

## How the app works

- Databricks Apps injects `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` for the app service principal automatically.
- You add a Genie space as an app resource in the Databricks Apps UI.
- `app.yaml` maps that resource to `GENIE_SPACE_ID` using `valueFrom: genie-space`.
- The Streamlit app uses `WorkspaceClient()` to start a Genie conversation and send follow-up messages in the same thread.

## Local development

To run this outside Databricks Apps, provide the Databricks host, OAuth client credentials, and the Genie space ID as environment variables.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:DATABRICKS_HOST="https://<your-workspace-host>"
$env:DATABRICKS_CLIENT_ID="<oauth-client-id>"
$env:DATABRICKS_CLIENT_SECRET="<oauth-client-secret>"
$env:GENIE_SPACE_ID="<genie-space-id>"
streamlit run app.py
```

## Push to GitHub

```powershell
git init
git add .
git commit -m "Add Databricks Apps Genie chat sample"
git branch -M main
git remote add origin https://github.com/<your-org>/<your-repo>.git
git push -u origin main
```

## Deploy on Databricks Apps

1. Push this folder to a GitHub repository.
2. In your Databricks workspace, open **Apps** and create a new app.
3. Choose **Git repository** as the source and select your GitHub repo and branch.
4. In the app configuration step, add a **Genie space** resource.
5. Use the default resource key `genie-space`, or update `app.yaml` if you choose a different key.
6. Grant the app `Can run` on the Genie space.
7. Make sure the app service principal also has the required data permissions on the underlying Unity Catalog objects.
8. Deploy the app.

## Required permissions

The app service principal typically needs:

- `Can run` on the Genie space
- `USE CATALOG` on the relevant catalog
- `USE SCHEMA` on the relevant schema
- `SELECT` on the tables or views queried by the Genie space

## Notes

- This sample uses app authorization, not per-user authorization.
- The app keeps the Genie `conversation_id` in Streamlit session state so follow-up questions stay in context.
- If Genie returns SQL, the UI shows the generated query and attempts to preview the first available result chunk.
