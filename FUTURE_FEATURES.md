# Future Features

## Worker service separation

Status: Consider later, not required for the current local workflow.

The current Streamlit app can start the prefetch worker in-process. This is convenient for local development, but a future long-running deployment could separate responsibilities:

- `srock-streamlit`: UI only, runs on port 8501.
- `srock-prefetch`: background data worker, runs independently.

Why consider it:

- Keeps data fetching alive even if the UI restarts.
- Avoids confusing in-memory worker state across different Python processes.
- Makes restart policies and logs easier to manage with systemd or another process supervisor.
- Reduces the chance of accidentally running multiple workers against the same SQLite database and FinMind quota.

Implementation notes for later:

- Add a cross-process lock or heartbeat to prevent duplicate workers.
- Persist worker status to SQLite so the UI reads status from the database instead of process memory.
- Remove automatic worker startup from `app.py` once an external worker service is configured.
- Add deployment docs and service files for Ubuntu/systemd.
