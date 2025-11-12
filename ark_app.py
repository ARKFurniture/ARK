
import streamlit as st
import pandas as pd
import json, os, sqlite3, tempfile, hashlib, base64
from datetime import datetime, date
import ark_scheduler as ark
from ark_dictionary import DICT_CSV

st.set_page_config(page_title="ARK Production Scheduler", layout="wide")

# -------------------- Security helpers --------------------
def pbkdf2_hash(password: str, salt_b: bytes=None, rounds: int=200_000) -> (str, str):
    """
    Returns (salt_b64, hash_b64). If salt_b is None, generates random salt.
    """
    if salt_b is None:
        salt_b = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_b, rounds)
    return base64.b64encode(salt_b).decode("utf-8"), base64.b64encode(dk).decode("utf-8")

def verify_password(password: str, salt_b64: str, hash_b64: str, rounds: int=200_000) -> bool:
    try:
        salt_b = base64.b64decode(salt_b64.encode("utf-8"))
        _, h2 = pbkdf2_hash(password, salt_b=salt_b, rounds=rounds)
        return h2 == hash_b64
    except Exception:
        return False

# -------------------- Dictionary helpers (service stages & piece types) --------------------
def get_dict_struct():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tdf:
        tdf.write(DICT_CSV)
        dict_path = tdf.name
    service_blocks, service_stage_orders = ark.load_service_blocks(dict_path)
    piece_types = set()
    for sb in service_blocks.values():
        piece_types.update([x for x in sb["Piece Type"].tolist() if isinstance(x, str)])
    piece_types = sorted(piece_types)
    return service_blocks, service_stage_orders, piece_types

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
    );
    """)
    # Employee shifts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        weekday INTEGER NOT NULL,
        start TEXT NOT NULL,
        end   TEXT NOT NULL
    );
    """)
    # Employee days off
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_days_off(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        off_date TEXT NOT NULL
    );
    """)
    # Special projects
    cur.execute("""
    CREATE TABLE IF NOT EXISTS special_projects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        label TEXT NOT NULL,
        start_ts TEXT NOT NULL,
        end_ts   TEXT NOT NULL
    );
    """)
    # Jobs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        job TEXT NOT NULL,
        service TEXT NOT NULL,
        stage_completed TEXT NOT NULL,
        qty INTEGER NOT NULL DEFAULT 1
    );
    """)
    # Priorities
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_customers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT UNIQUE NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_targets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        stage TEXT NOT NULL,
        by_date TEXT NOT NULL
    );
    """)
    # Global settings
    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_settings(
        id INTEGER PRIMARY KEY CHECK(id=1),
        window_start TEXT NOT NULL,
        window_end   TEXT NOT NULL,
        gap_after_finish_hours REAL NOT NULL DEFAULT 2,
        gap_before_assembly_hours REAL NOT NULL DEFAULT 12,
        assembly_earliest_hour INTEGER NOT NULL DEFAULT 9
    );
    """)
    cur.execute("SELECT COUNT(*) FROM global_settings;")
    if cur.fetchone()[0] == 0:
        cur.execute("""INSERT INTO global_settings
            (id, window_start, window_end, gap_after_finish_hours, gap_before_assembly_hours, assembly_earliest_hour)
            VALUES (1, '2025-11-12 08:00', '2025-11-23 23:59', 2, 12, 9)
        """)
    # Users (auth)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('admin','employee')),
        password_hash TEXT NOT NULL,
        employee_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)
    # Pre-seed admin if none exists
    cur.execute("SELECT COUNT(*) FROM users;")
    if cur.fetchone()[0] == 0:
        try:
            pwh = hash_password("password")
            cur.execute("""INSERT INTO users(name,email,role,password_hash,employee_id,active)
                           VALUES (?,?,?,?,?,1)""",
                        ("ARK Admin","info@arkfurniture.ca","admin",pwh,None))
        except Exception as e:
            pass
    conn.commit()
    conn.close()


init_db()
# -------------------- Auth UI --------------------
def users_exist() -> bool:
    return fetch_df("SELECT COUNT(*) as n FROM users")["n"].iloc[0] > 0

def bootstrap_admin():
    st.title("ARK Production Scheduler — Setup")
    st.info("Create the first **Admin** account.")
    with st.form("bootstrap_admin"):
        u = st.text_input("Admin username")
        p1 = st.text_input("Password", type="password")
        p2 = st.text_input("Confirm password", type="password")
        if st.form_submit_button("Create admin"):
            if not u.strip() or not p1:
                st.error("Username and password are required.")
                st.stop()
            if p1 != p2:
                st.error("Passwords do not match."); st.stop()
            salt, h = pbkdf2_hash(p1)
            conn = get_conn(); cur = conn.cursor()
            try:
                cur.execute("INSERT INTO users(username, role, employee_id, salt_b64, hash_b64) VALUES (?,?,?,?,?)",
                            (u.strip(), "admin", None, salt, h))
                conn.commit()
                st.success("Admin account created. Please log in.")
                st.experimental_set_query_params(_=str(datetime.utcnow().timestamp()))
            except Exception as e:
                st.error(f"Error: {e}")
            finally:
                conn.close()

def login_form():
    st.title("ARK Production Scheduler — Login")
    with st.form("login"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        ok = st.form_submit_button("Log in")
        if ok:
            row = fetch_df("SELECT * FROM users WHERE username=?", (u.strip(),))
            if row.empty:
                st.error("Invalid username or password."); st.stop()
            salt = row["salt_b64"].iloc[0]; h = row["hash_b64"].iloc[0]
            if not verify_password(p, salt, h):
                st.error("Invalid username or password."); st.stop()
            st.session_state["auth"] = {
                "username": row["username"].iloc[0],
                "role": row["role"].iloc[0],
                "employee_id": int(row["employee_id"].iloc[0]) if not pd.isna(row["employee_id"].iloc[0]) else None
            }
            st.experimental_set_query_params(_=str(datetime.utcnow().timestamp()))

def logout_button():
    with st.sidebar:
        st.markdown("---")
        if "auth" in st.session_state:
            st.caption(f"Logged in as **{st.session_state['auth']['username']}** ({st.session_state['auth']['role']})")
            if st.button("Log out"):
                st.session_state.pop("auth", None)
                st.experimental_set_query_params(_=str(datetime.utcnow().timestamp()))

# -------------------- Admin Screens --------------------
def admin_users_tab():
    st.subheader("Users (Accounts)")
    users = fetch_df("""SELECT u.id, u.username, u.role, e.name as employee, u.employee_id
                        FROM users u LEFT JOIN employees e ON e.id=u.employee_id
                        ORDER BY u.username""")
    st.dataframe(users)

    st.markdown("### Add User")
    emp_df = fetch_df("SELECT * FROM employees ORDER BY name")
    with st.form("add_user"):
        c1,c2,c3 = st.columns(3)
        u = c1.text_input("Username")
        role = c2.selectbox("Role", ["admin","employee"])
        link_emp = c3.selectbox("Link to employee (optional)", ["(none)"] + emp_df["name"].tolist()) if not emp_df.empty else c3.selectbox("Link to employee (optional)", ["(none)"])
        p1 = st.text_input("Password", type="password")
        p2 = st.text_input("Confirm password", type="password")
        if st.form_submit_button("Create user"):
            if not u.strip() or not p1:
                st.error("Username and password required."); st.stop()
            if p1 != p2:
                st.error("Passwords do not match."); st.stop()
            emp_id = None
            if link_emp != "(none)" and not emp_df.empty:
                emp_id = int(emp_df.loc[emp_df["name"]==link_emp, "id"].values[0])
            salt, h = pbkdf2_hash(p1)
            conn = get_conn(); cur = conn.cursor()
            try:
                cur.execute("INSERT INTO users(username, role, employee_id, salt_b64, hash_b64) VALUES (?,?,?,?,?)",
                            (u.strip(), role, emp_id, salt, h))
                conn.commit()
                st.success(f"User '{u.strip()}' created.")
            except Exception as e:
                st.error(f"Could not create user: {e}")
            finally:
                conn.close()

    if not users.empty:
        st.markdown("### Delete User")
        with st.form("del_user"):
            sel = st.selectbox("Select user", users["username"].tolist())
            if st.form_submit_button("Delete user"):
                try:
                    execute("DELETE FROM users WHERE username=?", (sel,))
                    st.success(f"Deleted user '{sel}'")
                except Exception as e:
                    st.error(f"Error deleting user: {e}")

def admin_app():
    st.title("ARK Production Scheduler — Admin")
    tabs = st.tabs([
        "Users", "Employees", "Jobs", "Special Projects", "Time Off",
        "Priorities & Rules", "Run Scheduler"
    ])

    # ----- Users -----
    with tabs[0]:
        admin_users_tab()

    # ----- Employees -----
    with tabs[1]:
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
                st.markdown("**Delete an employee** (cascades to shifts/days-off/projects/users link)")
                del_name = st.selectbox("Employee", emp_df["name"].tolist())
                confirm = st.checkbox("Type DELETE below and check this", value=False, key="emp_del_chk")
                text_confirm = st.text_input("Type: DELETE to confirm", "")
                if st.form_submit_button("Delete employee"):
                    if confirm and text_confirm.strip().upper() == "DELETE":
                        try:
                            execute("DELETE FROM employees WHERE name=?", (del_name,))
                            st.success(f"Deleted employee: {del_name}")
                        except Exception as e:
                            st.error(f"Could not delete employee: {e}")
                    else:
                        st.warning("Please confirm deletion by typing DELETE and checking the box.")

    # ----- Jobs -----
    with tabs[2]:
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
        jobs_df = fetch_df("SELECT * FROM jobs ORDER BY customer, job, service")
        st.dataframe(jobs_df)
        if not jobs_df.empty:
            with st.form("del_job"):
                st.markdown("**Delete a job**")
                jobs_df["label"] = jobs_df.apply(lambda r: f"{r['id']} – {r['customer']} | {r['qty']} x {r['job']} | {r['service']}", axis=1)
                selection = st.selectbox("Job", jobs_df["label"].tolist())
                if st.form_submit_button("Delete job"):
                    jid = int(selection.split(" – ")[0])
                    try:
                        execute("DELETE FROM jobs WHERE id=?", (jid,))
                        st.success(f"Deleted job id {jid}")
                    except Exception as e:
                        st.error(f"Could not delete job: {e}")

    # ----- Special Projects -----
    with tabs[3]:
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

    # ----- Time Off -----
    with tabs[4]:
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

    # ----- Priorities & Rules -----
    with tabs[5]:
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

    # ----- Run Scheduler -----
    with tabs[6]:
        run_scheduler_view(editable=True)

# -------------------- Employee Screens (read-only) --------------------
def build_runtime_cfg_and_schedule():
    # Build config from DB
    gs = fetch_df("SELECT * FROM global_settings WHERE id=1").iloc[0].to_dict()
    emp = fetch_df("SELECT * FROM employees ORDER BY name").to_dict(orient="records")
    shifts = fetch_df("SELECT * FROM employee_shifts").to_dict(orient="records")
    offs = fetch_df("SELECT * FROM employee_days_off").to_dict(orient="records")
    sps = fetch_df("SELECT * FROM special_projects").to_dict(orient="records")
    cprio = fetch_df("SELECT * FROM priorities_customers").to_dict(orient="records")
    targets = fetch_df("SELECT * FROM priorities_targets").to_dict(orient="records")
    jobs = fetch_df("SELECT id, customer, job, service, stage_completed, qty FROM jobs ORDER BY customer, job")

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
    # Employees
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

    # Dictionary temp path
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tdf:
        tdf.write(DICT_CSV)
        dict_path = tdf.name

    # Build forecast-like CSV
    jobs2 = jobs.copy()
    jobs2["Job"] = jobs2.apply(lambda r: (f"{int(r['qty'])} {r['job']}" if int(r["qty"])>1 else r["job"]), axis=1)
    jobs2["Service"] = jobs2["service"]
    jobs2["Stage"] = jobs2["stage_completed"]
    jobs2["Customer"] = jobs2["customer"]
    tmp_fore = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8")
    jobs2[["Customer","Job","Service","Stage"]].to_csv(tmp_fore.name, index=False)

    service_blocks, service_stage_orders = ark.load_service_blocks(dict_path)
    job_instances, unsched = ark.build_job_instances(tmp_fore.name, service_blocks, service_stage_orders)
    df = ark.schedule_jobs(cfg, job_instances, service_stage_orders)
    v2, v12 = ark.validate_schedule(df, cfg["rules"]["gap_after_finish_hours"], cfg["rules"]["gap_before_assembly_hours"])
    return df, v2, v12, cfg

def run_scheduler_view(editable: bool):
    st.subheader("Generate Schedule" if editable else "Schedule (Read‑only)")
    st.caption("Uses the embedded Production Hour Dictionary.")
    if st.button("Run Scheduler" if editable else "Refresh Schedule", type="primary"):
        df, v2, v12, cfg = build_runtime_cfg_and_schedule()
        st.success(f"Schedule built. 2h-gap violations={v2}, 12h-before-assembly violations={v12}")
        if not df.empty:
            st.markdown("### Master Schedule (first 300 rows)")
            st.dataframe(df.head(300))
            if editable:
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button("Download master schedule.csv", data=csv_bytes, file_name="schedule_master.csv", mime="text/csv")
            st.markdown("### Hours by Worker")
            st.dataframe(df.groupby("Assigned To")["Hours"].sum().round(2).reset_index())
            # Per-employee
            st.markdown("### Per‑Employee Schedules")
            workers = sorted(df["Assigned To"].dropna().unique().tolist())
            if workers:
                emp_tabs = st.tabs(workers)
                for i, w in enumerate(workers):
                    with emp_tabs[i]:
                        wdf = df[df["Assigned To"]==w].copy()
                        st.dataframe(wdf)
                        if editable:
                            wcsv = wdf.to_csv(index=False).encode("utf-8")
                            st.download_button(f"Download {w} schedule.csv", data=wcsv, file_name=f"schedule_{w}.csv", mime="text/csv")
        else:
            st.info("No rows scheduled yet.")

def employee_app(user):
    st.title("ARK Production Scheduler — Employee")
    emp_id = user.get("employee_id")
    if emp_id is None:
        st.warning("Your account is not linked to an employee record. Ask an admin to link your account.")
        return
    # Fetch employee name
    emp_row = fetch_df("SELECT * FROM employees WHERE id=?", (emp_id,))
    if emp_row.empty:
        st.warning("Linked employee record not found."); return
    emp_name = emp_row["name"].iloc[0]
    # Tabs
    tabs = st.tabs(["My Availability", "My Schedule", "Active Jobs", "Master Schedule (view)"])

    # My Availability
    with tabs[0]:
        st.subheader(f"Availability — {emp_name}")
        shifts = fetch_df("SELECT weekday, start, end FROM employee_shifts WHERE employee_id=? ORDER BY weekday", (emp_id,))
        if not shifts.empty:
            shifts["weekday"] = shifts["weekday"].map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"})
        st.markdown("**Shifts**")
        st.dataframe(shifts)
        offs = fetch_df("SELECT off_date FROM employee_days_off WHERE employee_id=? ORDER BY off_date", (emp_id,))
        st.markdown("**Days Off**")
        st.dataframe(offs)
        sps = fetch_df("SELECT label, start_ts, end_ts FROM special_projects WHERE employee_id=? ORDER BY start_ts", (emp_id,))
        st.markdown("**Special Projects**")
        st.dataframe(sps)

    # My Schedule (computed)
    with tabs[1]:
        st.subheader("My Schedule")
        df, v2, v12, cfg = build_runtime_cfg_and_schedule()
        my = df[df["Assigned To"]==emp_name].copy()
        if my.empty:
            st.info("No scheduled tasks yet.")
        else:
            st.dataframe(my)
            st.caption(f"Validation: 2h-gap violations={v2}, 12h-before-assembly violations={v12}")
            wcsv = my.to_csv(index=False).encode("utf-8")
            st.download_button(f"Download my schedule.csv", data=wcsv, file_name=f"schedule_{emp_name}.csv", mime="text/csv")

    # Active Jobs (all jobs; optionally filter to jobs where this employee has assignments)
    with tabs[2]:
        st.subheader("Active Jobs")
        jobs = fetch_df("SELECT customer, job, service, stage_completed, qty FROM jobs ORDER BY customer, job")
        # Mark jobs where this employee has assignments
        df, _, _, _ = build_runtime_cfg_and_schedule()
        assigned_jobs = set(df[df["Assigned To"]==emp_name].apply(lambda r: (r["Customer"], r["Job"], r["Service"]), axis=1).tolist())
        jobs["Assigned to me?"] = jobs.apply(lambda r: ("Yes" if ((r["customer"], r["job"] if int(r["qty"])==1 else f"{int(r['qty'])} {r['job']}", r["service"]) in assigned_jobs) else "No"), axis=1)
        st.dataframe(jobs)

    # Master Schedule (view only)
    with tabs[3]:
        st.subheader("Master Schedule (view only)")
        df, v2, v12, cfg = build_runtime_cfg_and_schedule()
        if df.empty:
            st.info("No scheduled tasks.")
        else:
            st.dataframe(df.head(500))
            st.caption(f"Validation: 2h-gap violations={v2}, 12h-before-assembly violations={v12}")

# -------------------- App router --------------------
def main():
    # Auth bootstrap or login
    if not users_exist():
        bootstrap_admin()
        return
    logout_button()
    if "auth" not in st.session_state:
        login_form()
        return

    user = st.session_state["auth"]
    role = user.get("role", "employee")

    if role == "admin":
        admin_app()
    else:
        employee_app(user)

if __name__ == "__main__":
    main()
