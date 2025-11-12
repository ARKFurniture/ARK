
import streamlit as st
import pandas as pd
import json, io, os, sqlite3, tempfile
from datetime import datetime, date, time as dtime
import ark_scheduler as ark
from ark_dictionary import DICT_CSV

st.set_page_config(page_title="ARK Production Scheduler", layout="wide")
st.title("ARK Production Scheduler — Admin")

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
    # Employee shifts (weekday 0=Mon .. 6=Sun)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        weekday INTEGER NOT NULL,
        start TEXT NOT NULL,  -- "HH:MM"
        end   TEXT NOT NULL   -- "HH:MM"
    );
    """)
    # Employee days off (full day)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employee_days_off(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        off_date TEXT NOT NULL  -- "YYYY-MM-DD"
    );
    """)
    # Special projects (block time)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS special_projects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        label TEXT NOT NULL,
        start_ts TEXT NOT NULL,  -- ISO
        end_ts   TEXT NOT NULL   -- ISO
    );
    """)
    # Jobs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        job TEXT NOT NULL,
        service TEXT NOT NULL,           -- Restore | 3-Coat | Resurface
        stage_completed TEXT NOT NULL,   -- "Not Started" or a stage name
        qty INTEGER NOT NULL DEFAULT 1
    );
    """)
    # Priorities by customer (lower weight = higher priority)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_customers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT UNIQUE NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0
    );
    """)
    # Priority targets (force stage by date)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS priorities_targets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer TEXT NOT NULL,
        stage TEXT NOT NULL,
        by_date TEXT NOT NULL  -- "YYYY-MM-DD"
    );
    """)
    # Global rules & window
    cur.execute("""
    CREATE TABLE IF NOT EXISTS global_settings(
        id INTEGER PRIMARY KEY CHECK(id=1),
        window_start TEXT NOT NULL,  -- "YYYY-MM-DD HH:MM"
        window_end   TEXT NOT NULL,
        gap_after_finish_hours REAL NOT NULL DEFAULT 2,
        gap_before_assembly_hours REAL NOT NULL DEFAULT 12,
        assembly_earliest_hour INTEGER NOT NULL DEFAULT 9
    );
    """)
    # Seed default settings if empty
    cur.execute("SELECT COUNT(*) FROM global_settings;")
    if cur.fetchone()[0] == 0:
        cur.execute("""INSERT INTO global_settings
            (id, window_start, window_end, gap_after_finish_hours, gap_before_assembly_hours, assembly_earliest_hour)
            VALUES (1, '2025-11-12 08:00', '2025-11-23 23:59', 2, 12, 9)
        """)
    conn.commit()
    conn.close()

init_db()

# --------------- Helpers: DB IO ----------------
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

# -------------------- Tabs --------------------
tabs = st.tabs([
    "Employees", "Jobs", "Special Projects", "Time Off",
    "Priorities & Rules", "Run Scheduler"
])

# -------------------- Employees --------------------
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

    # List
    emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
    st.dataframe(emp_df)

    # Add shifts
    st.markdown("### Add Shift")
    if not emp_df.empty:
        with st.form("add_shift"):
            ec = st.selectbox("Employee", emp_df["name"].tolist())
            weekday = st.selectbox("Weekday", ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], index=0)
            start_t = st.text_input("Start (HH:MM)", "08:00")
            end_t   = st.text_input("End (HH:MM)", "16:00")
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

# -------------------- Jobs --------------------
with tabs[1]:
    st.subheader("Jobs")
    SERVICES = ["Restore","3-Coat","Resurface"]
    with st.form("add_job"):
        c1,c2,c3 = st.columns(3)
        customer = c1.text_input("Customer")
        jobname  = c2.text_input("Job (e.g., 'Dining Table' or '6 Chairs')")
        service  = c3.selectbox("Service", SERVICES, index=0)
        c4,c5 = st.columns(2)
        stage_completed = c4.text_input("Stage completed (or 'Not Started')", "Not Started")
        qty = c5.number_input("Quantity (if job name doesn't include it)", value=1, min_value=1, step=1)
        submit = st.form_submit_button("Add Job")
        if submit and customer.strip() and jobname.strip():
            try:
                execute("INSERT INTO jobs(customer, job, service, stage_completed, qty) VALUES (?,?,?,?,?)",
                        (customer.strip(), jobname.strip(), service, stage_completed.strip(), int(qty)))
                st.success(f"Added job: {customer} – {jobname}")
            except Exception as e:
                st.error(f"Could not add job: {e}")
    st.dataframe(fetch_df("SELECT * FROM jobs ORDER BY customer, job"))

# -------------------- Special Projects --------------------
with tabs[2]:
    st.subheader("Special Projects (blocks time)")
    emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
    if emp_df.empty:
        st.info("Add at least one employee first.")
    else:
        with st.form("add_sp"):
            c1,c2 = st.columns(2)
            who = c1.selectbox("Employee", emp_df["name"].tolist())
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
    st.dataframe(fetch_df("""SELECT sp.id, e.name as employee, sp.label, sp.start_ts, sp.end_ts
                              FROM special_projects sp JOIN employees e ON e.id=sp.employee_id
                              ORDER BY sp.start_ts"""))

# -------------------- Time Off --------------------
with tabs[3]:
    st.subheader("Time Off (full days)")
    emp_df = fetch_df("SELECT * FROM employees ORDER BY name ASC")
    if emp_df.empty:
        st.info("Add at least one employee first.")
    else:
        with st.form("add_off"):
            who = st.selectbox("Employee", emp_df["name"].tolist())
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
    st.dataframe(fetch_df("""SELECT d.id, e.name as employee, d.off_date
                              FROM employee_days_off d JOIN employees e ON e.id=d.employee_id
                              ORDER BY d.off_date, e.name"""))

# -------------------- Priorities & Rules --------------------
with tabs[4]:
    st.subheader("Priorities & Rules")
    st.markdown("**Customer priority weights** (lower = higher priority)")
    with st.form("add_cprio"):
        c1,c2 = st.columns(2)
        cust = c1.text_input("Customer")
        w = c2.number_input("Weight", value=1.0, step=0.1, min_value=0.0)
        ok = st.form_submit_button("Add/Update priority")
        if ok and cust.strip():
            # Upsert
            conn = get_conn(); cur = conn.cursor()
            cur.execute("INSERT INTO priorities_customers(customer,weight) VALUES(?,?) ON CONFLICT(customer) DO UPDATE SET weight=excluded.weight",
                        (cust.strip(), float(w)))
            conn.commit(); conn.close()
            st.success(f"Saved priority for {cust.strip()}")

    st.dataframe(fetch_df("SELECT * FROM priorities_customers ORDER BY weight, customer"))

    st.markdown("**Stage-by-date targets** (e.g., set 'Assembly' by a date for deliveries)")
    with st.form("add_target"):
        c1,c2,c3 = st.columns(3)
        cust2 = c1.text_input("Customer (target)")
        stage = c2.text_input("Stage", "Assembly")
        by_dt = c3.date_input("By date", value=date(2025,11,14))
        ok2 = st.form_submit_button("Add target")
        if ok2 and cust2.strip():
            execute("INSERT INTO priorities_targets(customer, stage, by_date) VALUES (?,?,?)",
                    (cust2.strip(), stage.strip(), str(by_dt)))
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

# -------------------- Run Scheduler --------------------
with tabs[5]:
    st.subheader("Generate Schedule")
    st.markdown("Uses the **embedded Production Hour Dictionary**; no upload needed.")

    if st.button("Run Scheduler", type="primary"):
        # Build config from DB
        gs = fetch_df("SELECT * FROM global_settings WHERE id=1").iloc[0].to_dict()
        # Employees with abilities
        emp = fetch_df("SELECT * FROM employees ORDER BY name").to_dict(orient="records")
        shifts = fetch_df("SELECT * FROM employee_shifts").to_dict(orient="records")
        offs = fetch_df("SELECT * FROM employee_days_off").to_dict(orient="records")
        sps = fetch_df("SELECT * FROM special_projects").to_dict(orient="records")
        cprio = fetch_df("SELECT * FROM priorities_customers").to_dict(orient="records")
        targets = fetch_df("SELECT * FROM priorities_targets").to_dict(orient="records")
        # Jobs -> Forecast-like dataframe
        jobs = fetch_df("SELECT customer, job as Job, service as Service, stage_completed as Stage FROM jobs ORDER BY customer, job")
        # Build config dict
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
        # employees + shifts + offs -> cfg
        for e in emp:
            abilities = []
            if e["can_prep"]: abilities.append("prep")
            if e["can_finish"]: abilities.append("finishing")
            # collect this employee's shifts
            my_shifts = [s for s in shifts if s["employee_id"]==e["id"]]
            # normalize shift structure
            shift_list = []
            for s in my_shifts:
                shift_list.append({"days":[int(s["weekday"])],"start":str(s["start"]), "end":str(s["end"])})
            # days off
            my_offs = [o for o in offs if o["employee_id"]==e["id"]]
            days_off = [str(o["off_date"]) for o in my_offs]
            cfg["employees"].append({
                "name": e["name"],
                "abilities": abilities,
                "shifts": shift_list,
                "days_off": days_off
            })
        # priorities
        for p in cprio:
            cfg["priorities"]["customers"][p["customer"]] = float(p["weight"])
        for t in targets:
            cfg["priorities"]["targets"].append({"customer": t["customer"], "stage": t["stage"], "by": t["by_date"]+" 00:00"})
        # special projects
        for sp in sps:
            emp_name = fetch_df("SELECT name FROM employees WHERE id=?", (sp["employee_id"],)).iloc[0]["name"]
            cfg["special_projects"].append({"employee": emp_name, "start": sp["start_ts"], "end": sp["end_ts"], "label": sp["label"]})

        # Prepare dictionary (embedded) via a temp file path the core can read
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tdf:
            tdf.write(DICT_CSV)
            dict_path = tdf.name

        # Build service blocks / schedule
        try:
            service_blocks, service_stage_orders = ark.load_service_blocks(dict_path)
            # Build job instances directly from DataFrame -> write to temp CSV for reuse of core function
            tmp_fore = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8")
            jobs.rename(columns={"Job":"Job","Service":"Service","Stage":"Stage","customer":"Customer"}, inplace=True)
            jobs.to_csv(tmp_fore.name, index=False)
            job_instances, unsched = ark.build_job_instances(tmp_fore.name, service_blocks, service_stage_orders)
            df = ark.schedule_jobs(cfg, job_instances, service_stage_orders)
            v2, v12 = ark.validate_schedule(df, cfg["rules"]["gap_after_finish_hours"], cfg["rules"]["gap_before_assembly_hours"])
        except Exception as e:
            st.exception(e)
            st.stop()

        st.success(f"Schedule built. 2h-gap violations={v2}, 12h-before-assembly violations={v12}")

        if not df.empty:
            st.markdown("### Hours by Worker")
            st.dataframe(df.groupby("Assigned To")["Hours"].sum().round(2).reset_index())

            st.markdown("### Preview")
            st.dataframe(df.head(300))

            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button("Download schedule.csv", data=csv_bytes, file_name="schedule.csv", mime="text/csv")

        if unsched:
            st.warning("Some rows were not schedulable (showing up to 10):")
            st.json(unsched[:10])
