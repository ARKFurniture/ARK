
# ARK Scheduler — Full UI (no uploads)

This Streamlit app provides a CRUD UI (employees, jobs, special projects, time off, priorities)
and uses an **embedded Production Hour Dictionary**, so you don't need to upload files.

## Files
- `ark_app.py`           — main Streamlit app with the UI
- `ark_scheduler.py`     — core scheduling engine (already generated above)
- `ark_dictionary.py`    — embedded dictionary (auto-generated here)
- `requirements.txt`     — dependencies for Streamlit Cloud

## Run locally
pip install -r requirements.txt
streamlit run ark_app.py

## Deploy to Streamlit Cloud
Create a GitHub repo with the files above at the root.
In Streamlit Cloud, set **App file** to `ark_app.py`.

### Persistence
By default, data is stored in `ark_db.sqlite` in the app directory.
On Streamlit Cloud this is **ephemeral** across rebuilds. For durable persistence,
set `ARK_DB_PATH` to a mounted or managed path, or upgrade to a hosted DB (e.g., Postgres).
