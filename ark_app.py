
import streamlit as st
import pandas as pd
import json, os, sqlite3, tempfile, hashlib, binascii
from datetime import date
import ark_scheduler as ark
from ark_dictionary import DICT_CSV

st.set_page_config(page_title="ARK Production Scheduler", layout="wide", page_icon="🪑")

from pathlib import Path

def _apply_branding():
    css_path = Path("styles.css")
    try:
        css = css_path.read_text(encoding="utf-8")
    except Exception:
        css = ""
    st.markdown(f"""<style>{css}</style>""", unsafe_allow_html=True)
    # Header band with logo on every page load
    try:
        logo_svg = Path("ark_logo.svg").read_text(encoding="utf-8")
        st.markdown(f'<div class="brand-header"><div>{logo_svg}</div><div><p class="brand-header__title">ARK Production Scheduler</p><p class="brand-header__subtitle">Fast, clean, and on-brand scheduling</p></div></div>', unsafe_allow_html=True)
    except Exception:
        st.markdown('<div class="brand-header"><p class="brand-header__title">ARK Production Scheduler</p><p class="brand-header__subtitle">Fast, clean, and on-brand scheduling</p></div>', unsafe_allow_html=True)

_apply_branding()

# -------------------- Legacy-safe rendering for older Safari/WebKit --------------------
# Enable by adding ?safe=1 to the app URL or setting env ARK_SAFE_MARKDOWN=1
USE_SAFE = False
try:
    # Streamlit >=1.29 has st.query_params dict-like interface
    qp = getattr(st, "query_params", {}) or {}
    if isinstance(qp, dict):
        val = qp.get("safe", None)
        if isinstance(val, list):
            val = val[0] if val else None
        if str(val).lower() in ("1","true","yes"):
            USE_SAFE = True
except Exception:
    pass

import os as _os, re as _re
if _os.getenv("ARK_SAFE_MARKDOWN") in ("1","true","yes"):
    USE_SAFE = True

if USE_SAFE:
    def _strip_md(s):
        return _re.sub(r"[`*_>#]", "", str(s))
    # Keep original refs if needed later
    _orig_title = st.title
    _orig_header = st.header
    _orig_subheader = st.subheader
    _orig_caption = st.caption
    _orig_markdown = st.markdown
    _orig_info = st.info
    _orig_warning = st.warning
    _orig_success = st.success
    _orig_error = st.error

    st.title = lambda s: st.text(_strip_md(s))
    st.header = lambda s: st.text(_strip_md(s))
    st.subheader = lambda s: st.text(_strip_md(s))
    st.caption = lambda s: st.text(_strip_md(s))
    st.markdown = lambda s, **kwargs: st.text(_strip_md(s))
    st.info = lambda s: st.text(_strip_md(s))
    st.warning = lambda s: st.text(_strip_md(s))
    st.success = lambda s: st.text(_strip_md(s))
    st.error = lambda s: st.text(_strip_md(s))


# -------------------- Auth utilities --------------------
def hash_password(password: str, iterations: int = 200_000) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${binascii.hexlify(salt).decode()}${binascii.hexlify(dk).decode()}"

def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iter_str, salt_hex, hash_hex = stored.split("$", 3)
        iterations = int(iter_str)
        salt = binascii.unhexlify(salt_hex)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return binascii.hexlify(dk).decode() == hash_hex
    except Exception:
        return False

# -------------------- Dictionary helpers --------------------
def get_dict_struct():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tdf:
        tdf.write(DICT_CSV)
        dict_path = tdf.name
    service_blocks, service_stage_orders = ark.load_service_blocks(dict_path)
    piece_types = set()
    for sb in service_blocks.values():
        piece_types.update([x for x in sb["Piece Type"].tolist() if isinstance(x, str)])
    return service_blocks, service_stage_orders, sorted(piece_types)

SERVICE_BLOCKS, SERVICE_STAGE_ORDERS, PIECE_TYPES = get_dict_struct()
SERVICES = ["Restore", "3-Coat", "Resurface"]

# -------------------- Persistence (SQLite) --------------------
DB_PATH = os.environ.get("ARK_DB_PATH", "ark_db.sqlite")

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    # Employees
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        can_prep INTEGER NOT NULL DEFAULT 1,
        can_finish INTEGER NOT NULL DEFAULT 0
    );""")
    # Employee shifts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        weekday INTEGER NOT NULL,
        start TEXT NOT NULL,
        end   TEXT NOT NULL
    );""")
    # Employee days off
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_days_off(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        off_date TEXT NOT NULL
    );""")
    # Special projects
    cur.execute("""
    CREATE TABLE IF NOT EXISTS special_projects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        label TEXT NOT NULL,
        start_ts TEXT NOT NULL,
        end_ts   TEXT NOT NULL
    );""")
    # Jobs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        job TEXT NOT NULL,
        service TEXT NOT NULL,
        stage_completed TEXT NOT NULL,
        qty INTEGER NOT NULL DEFAULT 1
    );""")
    # Priorities
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_customers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT UNIQUE NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_targets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        stage TEXT NOT NULL,
        by_date TEXT NOT NULL
    );""")
    # Global settings
    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_settings(
        id INTEGER PRIMARY KEY CHECK(id=1),
        window_start TEXT NOT NULL,
        window_end   TEXT NOT NULL,
        gap_after_finish_hours REAL NOT NULL DEFAULT 2,
        gap_before_assembly_hours REAL NOT NULL DEFAULT 12,
        assembly_earliest_hour INTEGER NOT NULL DEFAULT 9
    );""")
    cur.execute("SELECT COUNT(*) FROM global_settings;")
    if cur.fetchone()[0] == 0:
        cur.execute("""INSERT INTO global_settings
            (id, window_start, window_end, gap_after_finish_hours, gap_before_assembly_hours, assembly_earliest_hour)
            VALUES (1, '2025-11-12 08:00', '2025-11-23 23:59', 2, 12, 9)""")
    # Users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('admin','employee')),
        password_hash TEXT NOT NULL,
        employee_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
        active INTEGER NOT NULL DEFAULT 1
    );""")
    # Pre-seed requested admin if not present
    cur.execute("SELECT id FROM users WHERE lower(email)=lower(?)", ('info@arkfurniture.ca',))
    row = cur.fetchone()
    if row is None:
        pwh = hash_password("password")
        cur.execute("""INSERT INTO users(name,email,role,password_hash,employee_id,active)
                       VALUES (?,?,?,?,?,1)""",
                    ("Kyle Babineau","info@arkfurniture.ca","admin",pwh,None))
    conn.commit()
    conn.close()

def fetch_df(query, params=()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def execute(query, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()

def users_exist() -> bool:
    """Return True if at least one user exists."""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        row = cur.fetchone(); conn.close()
        return bool(row and row[0] > 0)
    except Exception:
        return False

# -------------------- Session auth helpers --------------------
if "auth_user" not in st.session_state:
    st.session_state["auth_user"] = None  # dict with keys id,name,email,role,employee_id

def login_view():
    st.title("ARK Production Scheduler")
    st.subheader("Sign in")
    st.caption("Tip: For older iPhones/iPads, use iOS 16+/Safari 16+ or Chrome if you see a browser regex error.")
    email = st.text_input("Email", "")
    pw = st.text_input("Password", "", type="password")
    if st.button("Sign in", type="primary"):
        try:
            conn = get_conn(); cur = conn.cursor()
            cur.execute("SELECT id, name, email, role, password_hash, employee_id, active FROM users WHERE lower(email)=lower(?)", (email.strip(),))
            row = cur.fetchone()
            conn.close()
            if not row:
                st.error("Invalid email or password.")
                return
            uid, name, email2, role, pwh, emp_id, active = row
            if not active:
                st.error("Account is inactive. Contact admin.")
                return
            if not verify_password(pw, pwh):
                st.error("Invalid email or password.")
                return
            st.session_state["auth_user"] = {"id": uid, "name": name, "email": email2, "role": role, "employee_id": emp_id}
            st.success(f"Welcome, {name} ({role})")
            st.experimental_rerun()
        except Exception as e:
            st.exception(e)

def top_bar():
    au = st.session_state["auth_user"]
    if not au: return
    st.sidebar.markdown(f"**Signed in as:** {au['name']}  \n**Role:** {au['role']}")
    # Self-serve change password
    with st.sidebar.expander("Change password"):
        with st.form("change_pw_form", clear_on_submit=True):
            cur_pw = st.text_input("Current password", type="password", key="cp_cur")
            new_pw = st.text_input("New password", type="password", key="cp_new")
            new_pw2 = st.text_input("Confirm new password", type="password", key="cp_new2")
            submitted_cp = st.form_submit_button("Update password")
        if submitted_cp:
            if not new_pw or new_pw != new_pw2:
                st.warning("New passwords do not match.")
            elif len(new_pw) < 8:
                st.warning("Please choose a password with at least 8 characters.")
            else:
                try:
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("SELECT password_hash FROM users WHERE id=?", (au["id"],))
                    row = cur.fetchone()
                    if not row or not verify_password(cur_pw or "", row[0]):
                        st.error("Current password is incorrect.")
                    else:
                        pwh = hash_password(new_pw)
                        cur.execute("UPDATE users SET password_hash=? WHERE id=?", (pwh, au["id"]))
                        conn.commit(); conn.close()
                        st.success("Password updated. Please sign in again.")
                        st.session_state["auth_user"] = None
                        st.experimental_rerun()
                except Exception as e:
                    st.error(f"Could not change password: {e}")
    if st.sidebar.button("Sign out"):
        st.session_state["auth_user"] = None
        st.experimental_rerun()

# -------------------- Admin views --------------------
def admin_app():
    top_bar()
    st.title("ARK Production Scheduler — Admin")

    tabs = st.tabs([
        "Employees", "Jobs", "Special Projects", "Time Off",
        "Priorities & Rules", "Run Scheduler", "User Management"
    ])

    # Employees
    with tabs[0]:
        st.subheader("Employees")
        with st.form("add_employee"):
            c1, c2, c3 = st.columns(3)
            name = c1.text_input("Name")
            can_prep = c2.checkbox("Can do prep/assembly/scuffs", value=True)
            can_finish = c3.checkbox("Can do finishing (prime/paint/clear)", value=False)
            submitted = st.form_submit_button("Add Employee")
            if submitted and name.strip():
                try:
                    execute("INSERT INTO employees(name, can_prep, can_finish) VALUES (?,?,?)",
                            (name.strip(), 1 if can_prep else 0, 1 if can_finish else 0))
                    st.success(f"Added employee: {name}")
                except Exception as e:
                    st.error(f"Could not add employee: {e}")

        emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
        st.dataframe(emp_df)

        st.markdown("### Add Shift")
        if not emp_df.empty:
            with st.form("add_shift"):
                ec = st.selectbox("Employee", emp_df["name"].tolist(), key="shift_emp")
                weekday = st.selectbox("Weekday", ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], index=0, key="shift_wd")
                start_t = st.text_input("Start (HH:MM)", "08:00", key="shift_start")
                end_t   = st.text_input("End (HH:MM)", "16:00", key="shift_end")
                ok = st.form_submit_button("Add shift")
                if ok:
                    emp_id = int(emp_df.loc[emp_df["name"]==ec, "id"].values[0])
                    wd = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].index(weekday)
                    try:
                        execute("INSERT INTO employee_shifts(employee_id, weekday, start, end) VALUES (?,?,?,?)",
                                (emp_id, wd, start_t, end_t))
                        st.success("Shift added")
                    except Exception as e:
                        st.error(f"Could not add shift: {e}")

        st.markdown("**Shifts**")
        sh = fetch_df("""SELECT e.name, s.weekday, s.start, s.end, s.id
                         FROM employee_shifts s JOIN employees e ON e.id=s.employee_id
                         ORDER BY e.name, s.weekday""")
        if not sh.empty:
            sh["weekday"] = sh["weekday"].map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"})
        st.dataframe(sh)

        if not sh.empty:
            with st.form("del_shift"):
                st.markdown("**Delete a shift**")
                del_sid = st.selectbox("Shift ID", sh["id"].tolist())
                if st.form_submit_button("Delete shift"):
                    try:
                        execute("DELETE FROM employee_shifts WHERE id=?", (int(del_sid),))
                        st.success(f"Deleted shift {del_sid}")
                    except Exception as e:
                        st.error(f"Could not delete shift: {e}")

        if not emp_df.empty:
            with st.form("del_employee"):
                st.markdown("**Delete an employee** (cascades to shifts/days-off/projects)")
                # ID-safe: use employee id as the value; build a readable label from whatever columns exist
                if "id" in emp_df.columns:
                    emp_ids = emp_df["id"].astype(int).tolist()
                    _emap = {int(r["id"]): (r if isinstance(r, dict) else r.to_dict()) for _, r in emp_df.iterrows()}
                    def _emp_fmt(rid):
                        row = _emap.get(int(rid), {})
                        nm = row.get("name", "Employee")
                        rl = row.get("role", "")
                        em = row.get("email", "")
                        return f"{nm} · {rl} · {em}  (#{rid})"
                    sel_emp_id = st.selectbox("Employee", options=emp_ids, format_func=_emp_fmt, key="emp_delete_select")
                    confirm = st.checkbox("Type DELETE below and check this", value=False, key="emp_del_chk")
                    text_confirm = st.text_input("Type: DELETE to confirm", "", key="emp_del_text")
                    if st.form_submit_button("Delete employee"):
                        if confirm and text_confirm.strip().upper() == "DELETE":
                            try:
                                execute("DELETE FROM employees WHERE id=?", (int(sel_emp_id),))
                                st.success(f"Deleted employee id {int(sel_emp_id)}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Could not delete employee: {e}")
                        else:
                            st.warning("Please confirm deletion by typing DELETE and checking the box.")
                # Fallback if schema has no id (unlikely): delete by name exactly as selected
                else:
                    del_name = st.selectbox("Employee", emp_df.iloc[:,0].astype(str).tolist())
                    if st.form_submit_button("Delete employee by name"):
                        try:
                            execute("DELETE FROM employees WHERE name=?", (del_name,))
                            st.success(f"Deleted employee: {del_name}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not delete employee: {e}")
    with tabs[1]:
        st.subheader("Jobs")
        svc = st.selectbox("Service", SERVICES, index=0, key="job_service")
        stage_options = ["Not Started"] + SERVICE_STAGE_ORDERS[svc]
        piece = st.selectbox("Job Type (Piece)", PIECE_TYPES, index=PIECE_TYPES.index("Dining Table") if "Dining Table" in PIECE_TYPES else 0)
        c1,c2,c3 = st.columns(3)
        customer = c1.text_input("Customer")
        stage_completed = c2.selectbox("Stage completed", stage_options, index=0, key="job_stage")
        qty = c3.number_input("Quantity", value=1, min_value=1, step=1)
        if st.button("Add Job"):
            if customer.strip():
                try:
                    execute("INSERT INTO jobs(customer, job, service, stage_completed, qty) VALUES (?,?,?,?,?)",
                            (customer.strip(), piece, svc, stage_completed, int(qty)))
                    st.success(f"Added job: {customer} – {qty} x {piece} ({svc})")
                except Exception as e:
                    st.error(f"Could not add job: {e}")
            else:
                st.warning("Customer is required.")

        with st.expander("Bulk add jobs from CSV", expanded=False):
            up = st.file_uploader("Upload CSV of jobs", type=["csv"], key="jobs_csv_upload")
            if up is not None:
                try:
                    import pandas as pd
                    csv_df = pd.read_csv(up)
                except Exception as e:
                    st.error(f"Could not read CSV: {e}")
                    csv_df = None
                if csv_df is not None and not csv_df.empty:
                    st.caption(f"Loaded {len(csv_df)} rows")
                    cols = list(csv_df.columns)

                    def _guess(*cands):
                        lc = {c.lower().strip(): c for c in cols}
                        for nm in cands:
                            if nm in lc: return lc[nm]
                        return None

                    col_customer = st.selectbox("Customer column", ["<none>"] + cols, index=(["<none>"]+cols).index(_guess("customer","client","name")) if _guess("customer","client","name") else 0)
                    col_piece    = st.selectbox("Job/Piece column", ["<none>"] + cols, index=(["<none>"]+cols).index(_guess("job","piece","item","type")) if _guess("job","piece","item","type") else 0)
                    col_service  = st.selectbox("Service column", ["<none>"] + cols, index=(["<none>"]+cols).index(_guess("service")) if _guess("service") else 0)
                    col_stage    = st.selectbox("Stage column", ["<none>"] + cols, index=(["<none>"]+cols).index(_guess("stage","stage_completed","status")) if _guess("stage","stage_completed","status") else 0)
                    col_qty      = st.selectbox("Quantity column", ["<none>"] + cols, index=(["<none>"]+cols).index(_guess("qty","quantity","count")) if _guess("qty","quantity","count") else 0)

                    st.write("Defaults (used if the column is <none> or a value is missing):")
                    d1, d2, d3 = st.columns(3)
                    default_service = d1.selectbox("Default Service", SERVICES, index=0, key="csv_def_service")
                    default_stage   = d2.selectbox("Default Stage", ["Not Started"] + SERVICE_STAGE_ORDERS.get(default_service, []), index=0, key="csv_def_stage")
                    default_qty     = int(d3.number_input("Default Qty", value=1, min_value=1, step=1))

                    skip_dupes = st.checkbox("Skip rows that look like duplicates (same Customer + Piece + Service + Stage + Qty)", value=True)

                    # Build candidate rows
                    preview_rows = []
                    for _, r in csv_df.iterrows():
                        cust = str(r[col_customer]).strip() if col_customer != "<none>" else ""
                        piece = str(r[col_piece]).strip() if col_piece != "<none>" else ""
                        svc   = str(r[col_service]).strip() if col_service != "<none>" else default_service
                        stg   = str(r[col_stage]).strip() if col_stage != "<none>" else default_stage
                        try:
                            q     = int(r[col_qty]) if col_qty != "<none>" and str(r[col_qty]).strip() != "" else default_qty
                        except Exception:
                            q     = default_qty

                        if not cust or not piece:
                            # require at least customer and piece/job
                            continue

                        valid_stages = ["Not Started"] + SERVICE_STAGE_ORDERS.get(svc, [])
                        if stg not in valid_stages:
                            stg = "Not Started"

                        preview_rows.append({
                            "customer": cust, "job": piece, "service": svc, "stage_completed": stg, "qty": q
                        })

                    if not preview_rows:
                        st.warning("No valid rows found (need at least Customer and Job/Piece).")
                    else:
                        import pandas as pd
                        prev_df = pd.DataFrame(preview_rows)
                        st.dataframe(prev_df)

                        if st.button(f"Insert {len(preview_rows)} job(s)"):
                            inserted = 0
                            for row in preview_rows:
                                try:
                                    if skip_dupes:
                                        dup = fetch_df(
                                            "SELECT COUNT(*) as n FROM jobs WHERE customer=? AND job=? AND service=? AND stage_completed=? AND qty=?",
                                            (row["customer"], row["job"], row["service"], row["stage_completed"], int(row["qty"]))
                                        )
                                        if not dup.empty and int(dup.iloc[0]["n"]) > 0:
                                            continue
                                    execute("INSERT INTO jobs(customer, job, service, stage_completed, qty) VALUES (?,?,?,?,?)",
                                            (row["customer"], row["job"], row["service"], row["stage_completed"], int(row["qty"])))
                                    inserted += 1
                                except Exception as e:
                                    st.error(f"Row not inserted for {row['customer']} {row['job']}: {e}")
                            st.success(f"Inserted {inserted} job(s).")
                            st.rerun()
        try:
            jobs_df = fetch_df("SELECT * FROM jobs ORDER BY id ASC")
        except Exception:
            jobs_df = fetch_df("SELECT * FROM jobs")
        st.dataframe(jobs_df)
        if not jobs_df.empty:
            with st.form("del_job"):
                st.markdown("**Delete a job**")
                # ID-safe selection: value = job id
                if "id" in jobs_df.columns:
                    job_ids = jobs_df["id"].astype(int).tolist()
                    _jmap = {int(r["id"]): (r if isinstance(r, dict) else r.to_dict()) for _, r in jobs_df.iterrows()}
                    def _job_fmt(jid):
                        row = _jmap.get(int(jid), {})
                        return f"{row.get('customer', 'Customer')} | {row.get('qty', '')} x {row.get('job', '')} | {row.get('service', '')}  (#{jid})"
                    sel_job_id = st.selectbox("Job", options=job_ids, format_func=_job_fmt, key="job_delete_select")
                    if st.form_submit_button("Delete job"):
                        try:
                            execute("DELETE FROM jobs WHERE id=?", (int(sel_job_id),))
                            st.success(f"Deleted job id {int(sel_job_id)}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not delete job: {e}")
                else:
                    label_col = jobs_df.columns[0]
                    selection = st.selectbox("Job", jobs_df[label_col].astype(str).tolist())
                    if st.form_submit_button("Delete job by label"):
                        st.warning("Job delete requires an id column to be safe. Please ensure the schema includes an integer id.")
    with tabs[2]:
        st.subheader("Special Projects (blocks time)")
        emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
        if emp_df.empty:
            st.info("Add at least one employee first.")
        else:
            with st.form("add_sp"):
                c1,c2 = st.columns(2)
                who = c1.selectbox("Employee", emp_df["name"].tolist(), key="sp_emp")
                label = c2.text_input("Label", "Booth maintenance")
                c3,c4 = st.columns(2)
                sd = c3.date_input("Start date", value=date(2025,11,12))
                stime = c3.text_input("Start (HH:MM)", "13:00")
                ed = c4.date_input("End date", value=date(2025,11,12))
                etime = c4.text_input("End (HH:MM)", "14:00")
                ok = st.form_submit_button("Add block")
                if ok:
                    emp_id = int(emp_df.loc[emp_df["name"]==who, "id"].values[0])
                    start_ts = f"{sd} {stime}"
                    end_ts = f"{ed} {etime}"
                    try:
                        execute("INSERT INTO special_projects(employee_id,label,start_ts,end_ts) VALUES (?,?,?,?)",
                                (emp_id, label.strip(), start_ts, end_ts))
                        st.success("Special project added")
                    except Exception as e:
                        st.error(f"Could not add block: {e}")
        sp_df = fetch_df("""SELECT sp.id, e.name as employee, sp.label, sp.start_ts, sp.end_ts
                            FROM special_projects sp JOIN employees e ON e.id=sp.employee_id
                            ORDER BY sp.start_ts""")
        st.dataframe(sp_df)
        # Delete special project block (ID-safe)
        if not sp_df.empty and "id" in sp_df.columns:
            sp_ids = sp_df["id"].astype(int).tolist()
            _smap = {int(r["id"]): (r if isinstance(r, dict) else r.to_dict()) for _, r in sp_df.iterrows()}
            def _sp_fmt(sid):
                row = _smap.get(int(sid), {})
                return f"{row.get('employee', 'Employee')} | {row.get('label', '')} | {row.get('start_ts', '')} → {row.get('end_ts', '')}  (#{sid})"
            with st.form("del_sp"):
                sel_sp_id = st.selectbox("Special project", options=sp_ids, format_func=_sp_fmt, key="sp_delete_select")
                confirm_sp = st.checkbox("Confirm delete", value=False, key="sp_del_chk")
                if st.form_submit_button("Delete block"):
                    if confirm_sp:
                        try:
                            execute("DELETE FROM special_projects WHERE id=?", (int(sel_sp_id),))
                            st.success(f"Deleted special project id {int(sel_sp_id)}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not delete special project: {e}")
                    else:
                        st.warning("Please confirm deletion.")


    # Time Off
    with tabs[3]:
        st.subheader("Time Off (full days)")
        emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
        if emp_df.empty:
            st.info("Add at least one employee first.")
        else:
            with st.form("add_off"):
                who = st.selectbox("Employee", emp_df["name"].tolist(), key="off_emp")
                off = st.date_input("Off date")
                ok = st.form_submit_button("Add day off")
                if ok:
                    emp_id = int(emp_df.loc[emp_df["name"]==who, "id"].values[0])
                    try:
                        execute("INSERT INTO employee_days_off(employee_id, off_date) VALUES (?,?)",
                                (emp_id, str(off)))
                        st.success("Day off added")
                    except Exception as e:
                        st.error(f"Could not add day off: {e}")
        off_df = fetch_df("""SELECT d.id, e.name as employee, d.off_date
                              FROM employee_days_off d JOIN employees e ON e.id=d.employee_id
                              ORDER BY d.off_date, e.name""")
        st.dataframe(off_df)

    # Priorities & Rules
    with tabs[4]:
        st.subheader("Priorities & Rules")
        cust_list = fetch_df("SELECT DISTINCT customer FROM jobs ORDER BY customer")["customer"].tolist()
        with st.form("add_cprio"):
            c1,c2 = st.columns(2)
            cust = c1.selectbox("Customer", cust_list) if cust_list else c1.text_input("Customer")
            w = c2.number_input("Weight (lower = higher priority)", value=1.0, step=0.1, min_value=0.0)
            ok = st.form_submit_button("Add/Update priority")
            if ok and ((cust_list and cust) or (not cust_list and cust.strip())):
                val = cust if cust_list else cust.strip()
                conn = get_conn(); cur = conn.cursor()
                cur.execute("INSERT INTO priorities_customers(customer,weight) VALUES(?,?) ON CONFLICT(customer) DO UPDATE SET weight=excluded.weight",
                            (val, float(w)))
                conn.commit(); conn.close()
                st.success(f"Saved priority for {val}")
        st.dataframe(fetch_df("SELECT * FROM priorities_customers ORDER BY weight, customer"))

        all_stage_names = set(["Assembly"])
        for stages in SERVICE_STAGE_ORDERS.values():
            all_stage_names.update(stages)
        stage_list = sorted(all_stage_names)
        with st.form("add_target"):
            c1,c2,c3 = st.columns(3)
            cust2 = c1.selectbox("Customer", cust_list) if cust_list else c1.text_input("Customer")
            stage = c2.selectbox("Stage", stage_list, index=stage_list.index("Assembly") if "Assembly" in stage_list else 0)
            by_dt = c3.date_input("By date", value=date(2025,11,14))
            ok2 = st.form_submit_button("Add target")
            if ok2 and ((cust_list and cust2) or (not cust_list and str(cust2).strip())):
                val2 = cust2 if cust_list else str(cust2).strip()
                execute("INSERT INTO priorities_targets(customer, stage, by_date) VALUES (?,?,?)",
                        (val2, stage, str(by_dt)))
                st.success("Target added")
        st.dataframe(fetch_df("SELECT * FROM priorities_targets ORDER BY by_date, customer"))

        st.markdown("**Global rules & scheduling window**")
        gs = fetch_df("SELECT * FROM global_settings WHERE id=1")
        if not gs.empty:
            gs_row = gs.iloc[0].to_dict()
            with st.form("upd_rules"):
                c1,c2 = st.columns(2)
                ws = c1.text_input("Window start (YYYY-MM-DD HH:MM)", gs_row["window_start"])
                we = c2.text_input("Window end (YYYY-MM-DD HH:MM)", gs_row["window_end"])
                c3,c4,c5 = st.columns(3)
                gap2 = c3.number_input("Gap after finishing (hours)", value=float(gs_row["gap_after_finish_hours"]), step=0.5)
                gap12 = c4.number_input("Gap before assembly (hours)", value=float(gs_row["gap_before_assembly_hours"]), step=0.5)
                ae = c5.number_input("Assembly earliest hour (0–23)", value=int(gs_row["assembly_earliest_hour"]), min_value=0, max_value=23, step=1)
                ok3 = st.form_submit_button("Save window & rules")
                if ok3:
                    execute("""UPDATE global_settings SET window_start=?, window_end=?, gap_after_finish_hours=?, gap_before_assembly_hours=?, assembly_earliest_hour=? WHERE id=1""",
                            (ws, we, float(gap2), float(gap12), int(ae)))
                    st.success("Saved.")

    # Run Scheduler
    with tabs[5]:
        st.subheader("Generate Schedule")
        st.markdown("Uses the **embedded Production Hour Dictionary**; no upload needed.")
        if st.button("Run Scheduler", type="primary"):
            run_and_display_schedule()
    # User Management
    with tabs[6]:
        user_management_view()

# -------------------- Employee views (read-only) --------------------
def employee_app():
    top_bar()
    au = st.session_state["auth_user"]
    st.title("ARK — Employee Portal")

    emp = fetch_df("SELECT id, name FROM employees WHERE id=?", (au["employee_id"],)) if au.get("employee_id") else pd.DataFrame()
    emp_name = emp.iloc[0]["name"] if not emp.empty else None
    if not emp_name:
        st.warning("Your account is not linked to an employee record yet. Ask an admin to link it in User Management.")
    tabs = st.tabs(["My Availability", "My Schedule", "Active Jobs", "Master Schedule"])

    # My Availability
    with tabs[0]:
        if emp_name:
            st.subheader(f"Availability for {emp_name}")
            shifts = fetch_df("""SELECT weekday, start, end FROM employee_shifts WHERE employee_id=? ORDER BY weekday""", (au["employee_id"],))
            if not shifts.empty:
                shifts["weekday"] = shifts["weekday"].map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"})
            st.markdown("**Shifts**")
            st.dataframe(shifts)
            offs = fetch_df("SELECT off_date FROM employee_days_off WHERE employee_id=? ORDER BY off_date", (au["employee_id"],))
            st.markdown("**Days Off**")
            st.dataframe(offs)
        else:
            st.info("No linked employee record.")

    # My Schedule
    with tabs[1]:
        st.subheader("My Scheduled Tasks")
        df = run_scheduler_cached()
        if df is None or df.empty:
            st.info("No schedule generated yet. Ask an admin to run the scheduler.")
        else:
            wdf = df[df["Assigned To"]==emp_name] if emp_name else pd.DataFrame()
            st.dataframe(wdf)
            if not wdf.empty:
                wcsv = wdf.to_csv(index=False).encode("utf-8")
                st.download_button(f"Download my schedule.csv", data=wcsv, file_name=f"schedule_{emp_name or 'me'}.csv", mime="text/csv")

    # Active Jobs (read-only)
    with tabs[2]:
        st.subheader("Active Jobs")
        jobs = fetch_df("SELECT customer, job, service, stage_completed, qty FROM jobs ORDER BY id ASC")
        st.dataframe(jobs)

    # Master Schedule
    with tabs[3]:
        st.subheader("Master Schedule (read-only)")
        df = run_scheduler_cached()
        if df is None or df.empty:
            st.info("No schedule generated yet. Ask an admin to run the scheduler.")
        else:
            st.dataframe(df.head(500))
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download master schedule.csv", data=csv_bytes, file_name="schedule_master.csv", mime="text/csv")

# -------------------- Shared scheduling helpers --------------------
@st.cache_data(show_spinner=False, ttl=60)
def run_scheduler_cached():
    try:
        gs = fetch_df("SELECT * FROM global_settings WHERE id=1")
        if gs.empty:
            return None
        gs = gs.iloc[0].to_dict()
        emp = fetch_df("SELECT * FROM employees ORDER BY name").to_dict(orient="records")
        shifts = fetch_df("SELECT * FROM employee_shifts").to_dict(orient="records")
        offs = fetch_df("SELECT * FROM employee_days_off").to_dict(orient="records")
        sps = fetch_df("SELECT * FROM special_projects").to_dict(orient="records")
        cprio = fetch_df("SELECT * FROM priorities_customers").to_dict(orient="records")
        targets = fetch_df("SELECT * FROM priorities_targets").to_dict(orient="records")

        cfg = {
            "window": {"start": gs["window_start"], "end": gs["window_end"]},
            "rules": {
                "gap_after_finish_hours": float(gs["gap_after_finish_hours"]),
                "gap_before_assembly_hours": float(gs["gap_before_assembly_hours"]),
                "assembly_earliest_hour": int(gs["assembly_earliest_hour"]),
            },
            "employees": [],
            "priorities": {"customers": {}, "targets": []},
            "special_projects": []
        }
        for e in emp:
            abilities = []
            if e["can_prep"]: abilities.append("prep")
            if e["can_finish"]: abilities.append("finishing")
            my_shifts = [s for s in shifts if s["employee_id"]==e["id"]]
            shift_list = [{"days":[int(s["weekday"])],"start":str(s["start"]), "end":str(s["end"])} for s in my_shifts]
            my_offs = [o for o in offs if o["employee_id"]==e["id"]]
            days_off = [str(o["off_date"]) for o in my_offs]
            cfg["employees"].append({
                "name": e["name"],
                "abilities": abilities,
                "shifts": shift_list,
                "days_off": days_off
            })
        for p in cprio:
            cfg["priorities"]["customers"][p["customer"]] = float(p["weight"])
        for t in targets:
            cfg["priorities"]["targets"].append({"customer": t["customer"], "stage": t["stage"], "by": t["by_date"]+" 00:00"})
        for sp in sps:
            emp_name = fetch_df("SELECT name FROM employees WHERE id=?", (sp["employee_id"],)).iloc[0]["name"]
            cfg["special_projects"].append({"employee": emp_name, "start": sp["start_ts"], "end": sp["end_ts"], "label": sp["label"]})

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tdf:
            tdf.write(DICT_CSV)
            dict_path = tdf.name

        jobs = fetch_df("SELECT customer, job, service, stage_completed, qty FROM jobs ORDER BY id ASC")
        if jobs.empty:
            return pd.DataFrame()
        jobs["Job"] = jobs.apply(lambda r: (f"{int(r['qty'])} {r['job']}" if int(r["qty"])>1 else r["job"]), axis=1)
        jobs["Service"] = jobs["service"]
        jobs["Stage"] = jobs["stage_completed"]
        jobs["Customer"] = jobs["customer"]
        tmp_fore = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8")
        jobs[["Customer","Job","Service","Stage"]].to_csv(tmp_fore.name, index=False)

        service_blocks, service_stage_orders = ark.load_service_blocks(dict_path)
        job_instances, unsched = ark.build_job_instances(tmp_fore.name, service_blocks, service_stage_orders)
        df = ark.schedule_jobs(cfg, job_instances, service_stage_orders)
        return df
    except Exception:
        return None

def run_and_display_schedule():
    df = run_scheduler_cached()
    if df is None or df.empty:
        st.info("No schedule generated (check jobs/employees/shifts/rules).")
        return
    st.markdown("### Master Schedule")
    st.dataframe(df.head(500))

    st.markdown("### Hours by Worker")
    st.dataframe(df.groupby("Assigned To")["Hours"].sum().round(2).reset_index())

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download master schedule.csv", data=csv_bytes, file_name="schedule_master.csv", mime="text/csv")

    st.markdown("### Per‑Employee Schedules")
    workers = sorted(df["Assigned To"].dropna().unique().tolist())
    if workers:
        emp_tabs = st.tabs(workers)
        for i, w in enumerate(workers):
            with emp_tabs[i]:
                wdf = df[df["Assigned To"]==w].copy()
                st.dataframe(wdf)
                wcsv = wdf.to_csv(index=False).encode("utf-8")
                st.download_button(f"Download {w} schedule.csv", data=wcsv, file_name=f"schedule_{w}.csv", mime="text/csv")

# -------------------- User Management (admin) --------------------
def user_management_view():
    st.subheader("User Management")
    st.markdown("Create **admin** or **employee** accounts. Employees can be linked to an employee record for personalized schedules.")

    # Create user
    with st.form("create_user"):
        c1,c2 = st.columns(2)
        name = c1.text_input("Name")
        email = c2.text_input("Email")
        role = st.selectbox("Role", ["admin","employee"])
        emp_list = fetch_df("SELECT id, name FROM employees ORDER BY name")
        link_emp = st.selectbox("Link to employee (optional)", ["(none)"] + emp_list["name"].tolist())
        pw = st.text_input("Temp password", type="password")
        ok = st.form_submit_button("Create user")
        if ok:
            if not (name.strip() and email.strip() and pw.strip()):
                st.warning("Name, email, and password are required.")
            else:
                try:
                    pwh = hash_password(pw.strip())
                    emp_id = None
                    if link_emp != "(none)":
                        emp_id = int(emp_list.loc[emp_list["name"]==link_emp, "id"].values[0])
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("""INSERT INTO users(name,email,role,password_hash,employee_id,active)
                                   VALUES (?,?,?,?,?,1)""",
                                (name.strip(), email.strip().lower(), role, pwh, emp_id))
                    conn.commit(); conn.close()
                    st.success(f"Created {role} account for {name}.")
                except Exception as e:
                    st.error(f"Could not create user: {e}")

    # Users table
    users = fetch_df("""SELECT u.id, u.name, u.email, u.role, u.active, e.name as employee
                        FROM users u LEFT JOIN employees e ON e.id=u.employee_id
                        ORDER BY u.role, u.name""")
    st.dataframe(users)

    # Reset password / deactivate / relink
    if not users.empty:
        with st.form("manage_user"):
            uid = st.selectbox("User", users["id"].tolist())
            action = st.selectbox("Action", ["Reset password","Deactivate","Activate","Link to employee","Unlink employee"])
            emp_list = fetch_df("SELECT id, name FROM employees ORDER BY name")
            link_to = st.selectbox("Employee to link", ["(none)"] + emp_list["name"].tolist())
            new_pw = st.text_input("New password (for reset)", type="password")
            ok2 = st.form_submit_button("Apply")
            if ok2:
                try:
                    conn = get_conn(); cur = conn.cursor()
                    if action == "Reset password":
                        if not new_pw.strip():
                            st.warning("Enter a new password."); conn.close()
                        else:
                            pwh = hash_password(new_pw.strip())
                            cur.execute("UPDATE users SET password_hash=? WHERE id=?", (pwh, int(uid)))
                            conn.commit(); conn.close(); st.success("Password reset.")
                    elif action == "Deactivate":
                        cur.execute("UPDATE users SET active=0 WHERE id=?", (int(uid),))
                        conn.commit(); conn.close(); st.success("User deactivated.")
                    elif action == "Activate":
                        cur.execute("UPDATE users SET active=1 WHERE id=?", (int(uid),))
                        conn.commit(); conn.close(); st.success("User activated.")
                    elif action == "Link to employee":
                        if link_to == "(none)":
                            st.warning("Choose an employee to link."); conn.close()
                        else:
                            emp_id = int(emp_list.loc[emp_list["name"]==link_to, "id"].values[0])
                            cur.execute("UPDATE users SET employee_id=? WHERE id=?", (emp_id, int(uid)))
                            conn.commit(); conn.close(); st.success("Linked to employee.")
                    elif action == "Unlink employee":
                        cur.execute("UPDATE users SET employee_id=NULL WHERE id=?", (int(uid),))
                        conn.commit(); conn.close(); st.success("Unlinked from employee.")
                except Exception as e:
                    st.error(f"Action failed: {e}")

# -------------------- Main entry --------------------
def main():
    # Make sure DB schema exists and seed admin if needed
    try:
        init_db()
    except Exception:
        pass

    au = st.session_state["auth_user"]
    try:
        if not users_exist():
            init_db()
    except Exception:
        init_db()

    if not au:
        login_view()
        return

    if au["role"] == "admin":
        admin_app()
    else:
        employee_app()

if __name__ == "__main__":
    main()
