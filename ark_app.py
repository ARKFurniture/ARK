
import streamlit as st
import pandas as pd
import json, os, sqlite3, tempfile, time
from datetime import datetime, date
import bcrypt

import ark_scheduler as ark
from ark_dictionary import DICT_CSV

st.set_page_config(page_title="ARK Production Scheduler", layout="wide")

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
SERVICES = ["Restore","3-Coat","Resurface"]

# -------------------- Persistence (SQLite) --------------------
DB_PATH = os.environ.get("ARK_DB_PATH", "ark_db.sqlite")

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def execute(query, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()

def users_exist() -> bool:
    Return True if at least one user exists; initializes DB if needed.
    try:
        df = fetch_df("SELECT COUNT(*) as n FROM users")
        return int(df["n"].iloc[0]) > 0
    except Exception:
        # Ensure schema exists; then re-check
        try:
            init_db()
            df = fetch_df("SELECT COUNT(*) as n FROM users")
            return int(df["n"].iloc[0]) > 0
        except Exception:
            return False


def fetch_df(query, params=()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

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

    # Priorities by customer
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_customers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT UNIQUE NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0
    );
    """)

    # Stage targets
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
        username TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL,
        password_hash BLOB NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('admin','employee')),
        employee_id INTEGER NULL REFERENCES employees(id) ON DELETE SET NULL,
        created_at TEXT NOT NULL
    );
    """)

    # Schedule (published runs)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS schedule(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_ts TEXT NOT NULL,
        Customer TEXT, Job TEXT, Service TEXT, PieceType TEXT, Qty INTEGER,
        Stage TEXT, AssignedTo TEXT, Start TEXT, End TEXT, Hours REAL
    );
    """)

    conn.commit()

    # seed admin if none
    cur.execute("SELECT COUNT(*) FROM users;")
    if cur.fetchone()[0] == 0:
        pw = "admin"  # default; change after first login
        h = bcrypt.hashpw(pw.encode(), bcrypt.gensalt())
        cur.execute("""INSERT INTO users(username, display_name, password_hash, role, employee_id, created_at)
                       VALUES (?,?,?,?,?,?)""",
                    ("admin", "Administrator", h, "admin", None, datetime.utcnow().isoformat()))
        conn.commit()

    conn.close()

init_db()

# -------------------- Auth helpers --------------------
def login_form():
    with st.form("login_form"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        ok = st.form_submit_button("Sign in")
        if ok:
            df = fetch_df("SELECT * FROM users WHERE username=?", (u,))
            if df.empty:
                st.error("Invalid username or password")
                return False
            row = df.iloc[0]
            if bcrypt.checkpw(p.encode(), row["password_hash"]):
                st.session_state["auth"] = {
                    "user_id": int(row["id"]),
                    "username": row["username"],
                    "display_name": row["display_name"],
                    "role": row["role"],
                    "employee_id": int(row["employee_id"]) if row["employee_id"] is not None else None
                }
                return True
            else:
                st.error("Invalid username or password")
                return False
    return False

def require_auth():
    if "auth" not in st.session_state:
        st.title("Sign in")
        if login_form():
            st.experimental_rerun()
        st.stop()

def topbar():
    a = st.session_state.get("auth", {})
    st.caption(f"Signed in as **{a.get('display_name','')}** ({a.get('role','')})")
    if st.button("Sign out"):
        st.session_state.pop("auth", None)
        st.experimental_rerun()

# -------------------- Admin UI blocks --------------------
def admin_employees():
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
    st.dataframe(emp_df, use_container_width=True)

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
    st.dataframe(sh, use_container_width=True)

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

def admin_jobs():
    st.subheader("Jobs")
    svc = st.selectbox("Service", SERVICES, index=0, key="job_service")
    stage_options = ["Not Started"] + SERVICE_STAGE_ORDERS[svc]
    piece = st.selectbox("Job Type (Piece)", PIECE_TYPES, index=0)
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
    st.dataframe(jobs_df, use_container_width=True)

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

def admin_special_projects():
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
    st.dataframe(sp_df, use_container_width=True)

def admin_time_off():
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
    st.dataframe(off_df, use_container_width=True)

def admin_priorities_rules():
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

    st.dataframe(fetch_df("SELECT * FROM priorities_customers ORDER BY weight, customer"), use_container_width=True)

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
    st.dataframe(fetch_df("SELECT * FROM priorities_targets ORDER BY by_date, customer"), use_container_width=True)

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

def admin_scheduler():
    st.subheader("Run & Publish Schedule")
    st.markdown("Uses the **embedded Production Hour Dictionary**. Publishing stores the schedule so employees can view it.")
    if st.button("Generate & Publish", type="primary"):
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

        st.success(f"Schedule built. 2h-gap violations={v2}, 12h-before-assembly violations={v12}")

        if not df.empty:
            st.markdown("### Master Schedule (preview)")
            st.dataframe(df.head(300), use_container_width=True)

            # Publish to DB
            run_ts = datetime.utcnow().isoformat()
            conn = get_conn(); cur = conn.cursor()
            cur.execute("DELETE FROM schedule")  # keep only latest (simpler); use runs if you want history
            # bulk insert
            rows = df.to_dict(orient="records")
            for r in rows:
                cur.execute("""INSERT INTO schedule(run_ts, Customer, Job, Service, PieceType, Qty, Stage, AssignedTo, Start, End, Hours)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                            (run_ts, r["Customer"], r["Job"], r["Service"], r["Piece Type"], int(r["Qty"]),
                             r["Stage"], r["Assigned To"], str(r["Start"]), str(r["End"]), float(r["Hours"])))
            conn.commit(); conn.close()
            st.success(f"Published schedule at {run_ts} (UTC).")

            # Download master
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download master schedule.csv", data=csv_bytes, file_name="schedule_master.csv", mime="text/csv")

    # Account (employee)
    with tabs[4]:
        account_view()

            # Per-employee downloads
            st.markdown("### Per-Employee Schedules")
            workers = sorted(df["Assigned To"].dropna().unique().tolist())
            if workers:
                emp_tabs = st.tabs(workers)
                for i, w in enumerate(workers):
                    with emp_tabs[i]:
                        wdf = df[df["Assigned To"]==w].copy()
                        st.dataframe(wdf, use_container_width=True)
                        wcsv = wdf.to_csv(index=False).encode("utf-8")
                        st.download_button(f"Download {w} schedule.csv", data=wcsv, file_name=f"schedule_{w}.csv", mime="text/csv")
        if unsched:
            st.warning("Some rows were not schedulable (showing up to 10):")
            st.json(unsched[:10])

def admin_users():
    st.subheader("Users & Access")
    st.caption("Create admin or employee accounts. Employees can be linked to an employee record to see their own availability & schedule.")
    # Create user
    emp_df = fetch_df("SELECT id, name FROM employees ORDER BY name")
    emp_opts = ["(none)"] + emp_df["name"].tolist()
    with st.form("create_user"):
        c1,c2 = st.columns(2)
        username = c1.text_input("Username (unique)")
        display = c2.text_input("Display Name")
        c3,c4 = st.columns(2)
        role = c3.selectbox("Role", ["admin","employee"])
        emp_name = c4.selectbox("Link to Employee (optional for admin)", emp_opts)
        pw = st.text_input("Temporary Password", type="password")
        ok = st.form_submit_button("Create User")
        if ok:
            try:
                if not username.strip() or not display.strip() or not pw.strip():
                    st.error("Username, display name, and password are required.")
                else:
                    h = bcrypt.hashpw(pw.encode(), bcrypt.gensalt())
                    emp_id = None
                    if emp_name != "(none)":
                        emp_id = int(emp_df.loc[emp_df["name"]==emp_name, "id"].values[0])
                    conn = get_conn(); cur = conn.cursor()
                    cur.execute("""INSERT INTO users(username, display_name, password_hash, role, employee_id, created_at)
                                   VALUES (?,?,?,?,?,?)""",
                                (username.strip(), display.strip(), h, role, emp_id, datetime.utcnow().isoformat()))
                    conn.commit(); conn.close()
                    st.success(f"User {username} created.")
            except Exception as e:
                st.error(f"Could not create user: {e}")

    # Reset password
    users = fetch_df("""SELECT u.id, u.username, u.display_name, u.role, e.name as employee_name
                        FROM users u LEFT JOIN employees e ON e.id=u.employee_id
                        ORDER BY u.username""")
    st.dataframe(users, use_container_width=True)

    with st.form("reset_pw"):
        st.markdown("**Reset Password**")
        uname = st.selectbox("User", users["username"].tolist() if not users.empty else [])
        new_pw = st.text_input("New Password", type="password")
        ok2 = st.form_submit_button("Reset")
        if ok2 and uname and new_pw:
            try:
                h = bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt())
                execute("UPDATE users SET password_hash=? WHERE username=?", (h, uname))
                st.success(f"Password reset for {uname}")
            except Exception as e:
                st.error(f"Could not reset password: {e}")

    # Delete user
    with st.form("del_user"):
        st.markdown("**Delete User**")
        uname2 = st.selectbox("User", users["username"].tolist() if not users.empty else [])
        ok3 = st.form_submit_button("Delete")
        if ok3 and uname2:
            try:
                execute("DELETE FROM users WHERE username=?", (uname2,))
                st.success(f"Deleted user {uname2}")
            except Exception as e:
                st.error(f"Could not delete user: {e}")

# -------------------- Employee UI --------------------
def employee_dashboard():
    a = st.session_state.get("auth", {})
    emp_id = a.get("employee_id")
    emp_name = None
    if emp_id:
        df = fetch_df("SELECT name FROM employees WHERE id=?", (emp_id,))
        if not df.empty:
            emp_name = df.iloc[0]["name"]

    st.header("My Dashboard")

    colA, colB = st.columns(2)
    with colA:
        st.subheader("My Availability")
        if emp_id:
            shifts = fetch_df("""SELECT weekday, start, end FROM employee_shifts WHERE employee_id=? ORDER BY weekday""", (emp_id,))
            if not shifts.empty:
                shifts["weekday"] = shifts["weekday"].map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"})
            st.dataframe(shifts, use_container_width=True)
            offs = fetch_df("""SELECT off_date FROM employee_days_off WHERE employee_id=? ORDER BY off_date""", (emp_id,))
            st.dataframe(offs, use_container_width=True)
        else:
            st.info("Your account is not linked to an employee record yet.")

    with colB:
        st.subheader("Latest Master Schedule (summary)")
        ms = fetch_df("SELECT * FROM schedule WHERE run_ts=(SELECT MAX(run_ts) FROM schedule)")
        if ms.empty:
            st.info("No published schedule yet. Check back later.")
        else:
            by_worker = ms.groupby("AssignedTo")["Hours"].sum().round(2).reset_index()
            st.dataframe(by_worker, use_container_width=True)

    st.subheader("My Schedule")
    if emp_name:
        mine = fetch_df("""SELECT Customer, Job, Service, PieceType, Qty, Stage, Start, End, Hours
                           FROM schedule WHERE run_ts=(SELECT MAX(run_ts) FROM schedule) AND AssignedTo=?
                           ORDER BY Start""", (emp_name,))
        if mine.empty:
            st.info("No assignments yet in the latest published schedule.")
        else:
            st.dataframe(mine, use_container_width=True)
            st.download_button("Download my schedule.csv", data=mine.to_csv(index=False).encode("utf-8"),
                               file_name=f"schedule_{emp_name}.csv", mime="text/csv")

    st.subheader("My Active Jobs")
    if emp_name:
        jobs = fetch_df("""SELECT DISTINCT Customer, Job, Service FROM schedule
                           WHERE run_ts=(SELECT MAX(run_ts) FROM schedule) AND AssignedTo=?
                           ORDER BY Customer, Job""", (emp_name,))
        st.dataframe(jobs, use_container_width=True)

    st.subheader("Master Schedule (latest) — read‑only")
    ms2 = fetch_df("SELECT Customer, Job, Service, PieceType, Qty, Stage, AssignedTo, Start, End, Hours FROM schedule WHERE run_ts=(SELECT MAX(run_ts) FROM schedule) ORDER BY Start, AssignedTo")
    st.dataframe(ms2, use_container_width=True)

# -------------------- Main --------------------
def admin_ui():
    st.title("ARK Production Scheduler — Admin")
    topbar()
    tabs = st.tabs(["Employees","Jobs","Special Projects","Time Off","Priorities & Rules","Run & Publish","Users & Access"])
    with tabs[0]: admin_employees()
    with tabs[1]: admin_jobs()
    with tabs[2]: admin_special_projects()
    with tabs[3]: admin_time_off()
    with tabs[4]: admin_priorities_rules()
    with tabs[5]: admin_scheduler()
    with tabs[6]: admin_users()

def app_entry():
    require_auth()
    role = st.session_state["auth"]["role"]
    if role == "admin":
        admin_ui()
    else:
        topbar()
        employee_dashboard()

app_entry()
# -------------------- Account (self-service password change) --------------------
def account_view():
    au = st.session_state["auth_user"]
    st.subheader("Account")
    st.markdown(f"**Name:** {au['name']}  \n**Email:** {au['email']}  \n**Role:** {au['role']}")
    st.markdown("### Change Password")
    with st.form("change_pw"):
        cur_pw = st.text_input("Current password", type="password")
        new_pw = st.text_input("New password", type="password")
        new_pw2 = st.text_input("Confirm new password", type="password")
        ok = st.form_submit_button("Update password")
        if ok:
            if not new_pw.strip() or new_pw != new_pw2:
                st.warning("New passwords do not match or are empty.")
                return
            # verify current
            conn = get_conn(); cur = conn.cursor()
            cur.execute("SELECT password_hash FROM users WHERE id=?", (au["id"],))
            row = cur.fetchone()
            conn.close()
            if not row or not verify_password(cur_pw, row[0]):
                st.error("Current password is incorrect.")
                return
            # update
            pwh = hash_password(new_pw.strip())
            conn = get_conn(); cur = conn.cursor()
            cur.execute("UPDATE users SET password_hash=? WHERE id=?", (pwh, au["id"]))
            conn.commit(); conn.close()
            st.success("Password updated. Please use your new password next time you sign in.")

