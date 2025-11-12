
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
import json
import re

# --------------- Utilities ---------------
def parse_dt(val: str) -> datetime:
    """
    Accepts 'YYYY-MM-DD HH:MM[:SS]' or 'YYYY-MM-DD'
    """
    val = str(val).strip()
    if len(val) <= 10:
        return datetime.strptime(val, "%Y-%m-%d")
    # allow seconds optional
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(val, fmt)
        except:
            pass
    raise ValueError(f"Unrecognized datetime format: {val}")

def daterange(start: datetime, end: datetime):
    d = start
    while d.date() <= end.date():
        yield d
        d += timedelta(days=1)

def to_hours(td: timedelta) -> float:
    return round(td.total_seconds()/3600.0, 6)

def num(val) -> Optional[float]:
    try:
        return float(val)
    except:
        return None

# --------------- Data structures ---------------
from dataclasses import dataclass, field

@dataclass
class StageTask:
    customer: str
    job: str
    service: str
    piece_type: str
    qty: int
    stage: str
    hours: float

@dataclass
class JobInstance:
    idx: int                   # index from the forecast CSV row (unique per row)
    customer: str
    job: str
    service: str
    piece_type: str
    qty: int
    remaining: List[StageTask] = field(default_factory=list)

@dataclass
class Interval:
    start: datetime
    end: datetime

# --------------- Dictionary parsing ---------------
def load_service_blocks(dict_path: str):
    """
    Split a multi-table CSV into service blocks and stage orders.
    Detect blocks by blank rows, then infer which block corresponds to which service.
    Returns (service_blocks, service_stage_orders).
    """
    raw = pd.read_csv(dict_path, header=None)
    nan_rows = raw.isna().all(axis=1)
    block_ranges, start = [], 0
    for i, is_nan in enumerate(nan_rows):
        if is_nan:
            if i > start:
                block_ranges.append((start, i))
            start = i + 1
    if start < len(raw):
        block_ranges.append((start, len(raw)))

    blocks = []
    for a,b in block_ranges:
        block = raw.iloc[a:b].reset_index(drop=True)
        block.columns = [str(x).strip() for x in block.iloc[0].tolist()]
        block = block.iloc[1:].reset_index(drop=True)
        # drop empty columns
        block = block.loc[:, [c for c in block.columns if c!='nan' and not pd.isna(c)]]
        if "Piece Type" in block.columns:
            block = block[block["Piece Type"].notna()]
        # convert numeric columns
        for c in block.columns:
            if c != "Piece Type":
                block[c] = pd.to_numeric(block[c], errors="coerce")
        blocks.append(block.reset_index(drop=True))

    # Infer services by presence of key columns
    def find_block(names: List[str]) -> Optional[int]:
        for i, blk in enumerate(blocks[:4]):
            cols = set([c.lower() for c in blk.columns])
            if all(n.lower() in cols for n in names):
                return i
        return None

    idx_3coat = find_block(["Priming","paint1","paint2"])    # 3-Coat = Priming/Paint
    idx_restore = find_block(["Staining","clear1","clear2"]) # Restore = Staining/Clears
    idx_resurface = find_block(["Touch Up","Clear"])         # Resurface = Touch Up/Clear

    if any(v is None for v in [idx_3coat, idx_restore, idx_resurface]):
        raise RuntimeError("Could not infer all service blocks from dictionary CSV. "
                           "Please ensure tables contain expected columns.")

    block_3coat = blocks[idx_3coat]
    block_restore = blocks[idx_restore]
    block_resurface = blocks[idx_resurface]

    def stage_order(df: pd.DataFrame) -> List[str]:
        return [c for c in df.columns if c not in ["Piece Type","Total"]]

    service_blocks = {
        "3-Coat": block_3coat,
        "Restore": block_restore,
        "Resurface": block_resurface,
    }
    service_stage_orders = {
        "3-Coat":   stage_order(block_3coat),
        "Restore":  stage_order(block_restore),
        "Resurface":stage_order(block_resurface),
    }
    return service_blocks, service_stage_orders

# --------------- Forecast parsing ---------------
def normalize_piece(job_name: str, dict_types: List[str]):
    s = str(job_name).strip().lower()
    qty = 1
    tokens = s.split()
    if tokens and tokens[0].isdigit():
        qty = int(tokens[0])
        s = " ".join(tokens[1:])
    mapping = {
        "chair": "Dining Chair",
        "chairs": "Dining Chair",
        "dining chair": "Dining Chair",
        "side table": "Sidetable",
        "sidetable": "Sidetable",
        "coffee table": "Coffee Table",
        "dining table": "Dining Table",
        "sideboard": "Sideboard",
        "dresser": "Dresser",
        "cabinet": "Cabinet",
        "hutch": "Hutch",
        "desk": "Desk",
        "armoire": "Armoire",
        "bed frame": "Bed Frame",
        "chest": "Chest",
        "couch set": None,  # not in dictionary by default
    }
    if s in mapping:
        return mapping[s], qty
    for t in dict_types:
        if t.lower() in s or s in t.lower():
            return t, qty
    return None, qty

def map_completed_stage(service: str, stage_text: str, service_stage_orders):
    if stage_text is None or (isinstance(stage_text, float) and np.isnan(stage_text)):
        return None
    st = str(stage_text).strip().lower()
    if st in ["not started","none",""]:
        return None

    order = service_stage_orders[service]

    # Direct match
    for s in order:
        if s.lower() == st:
            return s

    # Synonyms to reconcile mislabels
    if service == "3-Coat":  # Priming & Paint
        synonyms = {
            "clear1":"paint1","clear 1":"paint1","clear2":"paint2","clear 2":"paint2","clear3":"paint2",
            "prime":"Priming","stain":"Priming","staining":"Priming",
        }
    elif service == "Restore":  # Staining & Clears
        synonyms = {
            "paint1":"clear1","paint 1":"clear1","paint2":"clear2","paint 2":"clear2",
            "prime":"Staining","priming":"Staining","stain":"Staining",
        }
    else:  # Resurface
        synonyms = {"clear1":"Clear","paint1":"Clear","paint2":"Clear","prime":"Touch Up"}

    if st in synonyms:
        return synonyms[st]

    # Heuristic contains
    for s in order:
        if st in s.lower() or s.lower() in st:
            return s
    return None

def build_job_instances(forecast_path: str, service_blocks, service_stage_orders):
    df = pd.read_csv(forecast_path)
    # collect dictionary piece types
    dict_types = set()
    for sb in service_blocks.values():
        dict_types.update([x for x in sb["Piece Type"].tolist() if isinstance(x, str)])
    dict_types = list(dict_types)

    jobs = []
    unsched = []

    for idx, row in df.iterrows():
        customer, job, service, stage_done = row["Customer"], row["Job"], row["Service"], row["Stage"]
        if service not in service_blocks:
            unsched.append({**row.to_dict(), "Reason": "Unsupported service"})
            continue
        piece, qty = normalize_piece(job, dict_types)
        if piece is None:
            unsched.append({**row.to_dict(), "Reason": "Unknown piece type"})
            continue
        block = service_blocks[service]
        if piece not in set(block["Piece Type"]):
            unsched.append({**row.to_dict(), "Reason": f"Piece '{piece}' not in dictionary for {service}"})
            continue

        completed = map_completed_stage(service, stage_done, service_stage_orders)
        order = service_stage_orders[service]
        if completed is None:
            remaining = order[:]
        else:
            remaining = order[order.index(completed)+1:] if completed in order else order[:]

        row_dict = block[block["Piece Type"]==piece].iloc[0]

        stages = []
        for stg in remaining:
            if stg in ["Piece Type","Total"]:
                continue
            hrs = row_dict[stg]
            if pd.isna(hrs):
                continue
            stages.append({
                "customer": customer,
                "job": job,
                "service": service,
                "piece_type": piece,
                "qty": int(qty),
                "stage": stg,
                "hours": float(hrs)*int(qty),
            })

        jobs.append({
            "idx": int(idx),
            "customer": customer,
            "job": job,
            "service": service,
            "piece_type": piece,
            "qty": int(qty),
            "remaining": stages
        })

    return jobs, unsched

# --------------- Scheduling ---------------
def is_finishing(stage_name: str) -> bool:
    n = str(stage_name).lower()
    return ("prim" in n) or ("paint" in n) or ("clear" in n)

class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end

def build_calendars(cfg: dict, window_start: datetime, window_end: datetime):
    cals = {e["name"]: [] for e in cfg["employees"]}
    # days off
    day_off_dates = {e["name"]: set() for e in cfg["employees"]}
    for e in cfg["employees"]:
        for dstr in e.get("days_off", []):
            day_off_dates[e["name"]].add(parse_dt(dstr).date())

    for d in daterange(window_start, window_end):
        weekday = d.weekday()  # Mon=0
        for e in cfg["employees"]:
            if d.date() in day_off_dates[e["name"]]:
                continue
            for s in e["shifts"]:
                if weekday in s["days"]:
                    st = datetime.combine(d.date(), datetime.strptime(s["start"], "%H:%M").time())
                    en = datetime.combine(d.date(), datetime.strptime(s["end"], "%H:%M").time())
                    st = max(st, window_start); en = min(en, window_end)
                    if en > st:
                        cals[e["name"]].append(Interval(st, en))

    # Special projects blockouts
    for block in cfg.get("special_projects", []):
        who = block["employee"]
        st = parse_dt(block["start"]); en = parse_dt(block["end"])
        new_intervals = []
        for iv in cals[who]:
            if en <= iv.start or st >= iv.end:
                new_intervals.append(iv)
            else:
                if iv.start < st: new_intervals.append(Interval(iv.start, st))
                if en < iv.end:   new_intervals.append(Interval(en, iv.end))
        cals[who] = [iv for iv in new_intervals if iv.end > iv.start]
    return cals

def to_hours_td(start, end):
    return round((end-start).total_seconds()/3600.0, 6)

def allocate_contiguous(cals, worker, earliest, hours_needed):
    for i, iv in enumerate(cals[worker]):
        if iv.end <= earliest: continue
        eff_s = max(iv.start, earliest)
        if eff_s >= iv.end: continue
        avail = to_hours_td(eff_s, iv.end)
        if avail + 1e-6 >= hours_needed:
            seg_end = eff_s + timedelta(hours=hours_needed)
            new = []
            if iv.start < eff_s: new.append(Interval(iv.start, eff_s))
            if seg_end < iv.end: new.append(Interval(seg_end, iv.end))
            cals[worker][i:i+1] = new
            return [(eff_s, seg_end)], seg_end, 0.0
    future_starts = [max(iv.start, earliest) for iv in cals[worker] if iv.end > earliest]
    if not future_starts:
        return [], earliest, hours_needed
    new_earliest = min(future_starts)
    return allocate_contiguous(cals, worker, new_earliest, hours_needed)

def allocate_splittable(cals, worker, earliest, hours_needed):
    segs = []; remaining = hours_needed; i=0
    while i < len(cals[worker]) and remaining > 1e-6:
        iv = cals[worker][i]
        if iv.end <= earliest: i+=1; continue
        eff_s = max(iv.start, earliest)
        if eff_s >= iv.end: i+=1; continue
        avail = to_hours_td(eff_s, iv.end)
        use = min(avail, remaining)
        seg_end = eff_s + timedelta(hours=use)
        segs.append((eff_s, seg_end))
        remaining -= use
        new = []
        if iv.start < eff_s: new.append(Interval(iv.start, eff_s))
        if seg_end < iv.end: new.append(Interval(seg_end, iv.end))
        cals[worker][i:i+1] = new
        if new: i += 1
        earliest = seg_end
    finish = segs[-1][1] if segs else earliest
    return segs, finish, remaining

def simulate(cals, worker, earliest, hours_needed, contiguous):
    backup = {w: [Interval(iv.start, iv.end) for iv in ivs] for w, ivs in cals.items()}
    if contiguous:
        segs, finish, rem = allocate_contiguous(cals, worker, earliest, hours_needed)
    else:
        segs, finish, rem = allocate_splittable(cals, worker, earliest, hours_needed)
    # restore
    for w in cals:
        cals[w] = backup[w]
    return segs, finish, rem

def choose_worker(cfg, cals, stage, earliest, hours_needed):
    contiguous = is_finishing(stage)
    # candidates per abilities
    candidates = []
    for e in cfg["employees"]:
        name = e["name"]
        can = True
        if is_finishing(stage) and ("finishing" not in e.get("abilities", [])):
            can = False
        if (not is_finishing(stage)) and ("prep" not in e.get("abilities", [])):
            can = False
        if can:
            candidates.append(name)
    if not candidates:
        candidates = [e["name"] for e in cfg["employees"]]
    best = None
    for w in candidates:
        segs, finish, rem = simulate(cals, w, earliest, hours_needed, contiguous)
        key = (0 if rem <= 1e-6 else 1, finish)
        if best is None or key < best[0]:
            best = (key, w, segs, finish, rem)
    w = best[1]
    if contiguous:
        segs, finish, rem2 = allocate_contiguous(cals, w, earliest, hours_needed)
    else:
        segs, finish, rem2 = allocate_splittable(cals, w, earliest, hours_needed)
    assert rem2 <= 1e-6 + 1e-8
    return w, segs, finish

def schedule_jobs(cfg, jobs, service_stage_orders):
    start = parse_dt(cfg["window"]["start"])
    end   = parse_dt(cfg["window"]["end"])
    cals = build_calendars(cfg, start, end)

    customer_priority = cfg.get("priorities", {}).get("customers", {})
    def job_priority_key(j):
        w = customer_priority.get(j["customer"], 1.0)
        return (w, j["customer"], j["job"], j["idx"])
    jobs_sorted = sorted(jobs, key=job_priority_key)

    rows = []
    for job in jobs_sorted:
        remaining = job["remaining"]
        if not remaining:
            continue
        order = service_stage_orders[job["service"]]
        stages_sorted = sorted(remaining, key=lambda t: order.index(t["stage"]))

        last_finish = start
        last_finishing_end = None
        prev_stage = None

        # Optional target assembly day
        target_assembly_date = None
        for rule in cfg.get("priorities", {}).get("targets", []):
            if rule.get("customer")==job["customer"] and rule.get("stage","").lower()=="assembly":
                target_assembly_date = parse_dt(rule["by"]).date()

        for t in stages_sorted:
            earliest = last_finish

            # 2h after finishing before next *different* stage
            if prev_stage is not None and is_finishing(prev_stage) and t["stage"].lower() != prev_stage.lower():
                earliest = max(earliest, last_finishing_end + timedelta(hours=cfg["rules"]["gap_after_finish_hours"]))

            # 12h before assembly
            if t["stage"].lower() == "assembly" and last_finishing_end is not None:
                earliest = max(earliest, last_finishing_end + timedelta(hours=cfg["rules"]["gap_before_assembly_hours"]))

            # force assembly day if targeted
            if t["stage"].lower()=="assembly" and target_assembly_date is not None:
                earliest = max(earliest, datetime.combine(target_assembly_date, time(cfg["rules"].get("assembly_earliest_hour",9),0)))

            worker, segs, finish = choose_worker(cfg, cals, t["stage"], earliest, t["hours"])
            for s,e in segs:
                rows.append({
                    "Customer": job["customer"], "Job": job["job"], "Service": job["service"],
                    "Piece Type": job["piece_type"], "Qty": job["qty"], "Stage": t["stage"],
                    "Assigned To": worker, "Start": s, "End": e, "Hours": round(to_hours_td(s,e), 3),
                })
            last_finish = max(last_finish, finish)
            if is_finishing(t["stage"]):
                last_finishing_end = finish
            prev_stage = t["stage"]

    df = pd.DataFrame(rows).sort_values(by=["Start","Assigned To"]).reset_index(drop=True)
    return df

def validate_schedule(df, gap_after_finish_hours, gap_before_assembly_hours):
    def finishing(s):
        s=str(s).lower()
        return ("prim" in s) or ("paint" in s) or ("clear" in s)
    viol_2h=0; viol_12h=0
    for key, sub in df.groupby(["Customer","Job","Service"]):
        sub=sub.sort_values("Start").reset_index(drop=True)
        prev_stage=None
        last_fin_end=None
        for _, r in sub.iterrows():
            stage=r["Stage"]; s=r["Start"]; e=r["End"]
            if isinstance(s, str): s=pd.to_datetime(s)
            if isinstance(e, str): e=pd.to_datetime(e)
            if prev_stage is not None and finishing(prev_stage) and stage.lower()!=prev_stage.lower() and stage.lower()!="assembly":
                if s < last_fin_end + pd.Timedelta(hours=gap_after_finish_hours):
                    viol_2h += 1
            if stage.lower()=="assembly" and last_fin_end is not None:
                if s < last_fin_end + pd.Timedelta(hours=gap_before_assembly_hours):
                    viol_12h += 1
            if finishing(stage):
                last_fin_end = e
            prev_stage = stage
    return viol_2h, viol_12h

def main():
    ap = argparse.ArgumentParser(description="ARK Production Scheduler")
    ap.add_argument("--dict", required=True, help="Path to Production Hour Dictionary CSV (multi-table)")
    ap.add_argument("--forecast", required=True, help="Path to Forecast CSV")
    ap.add_argument("--config", required=True, help="Path to JSON config describing employees, abilities, priorities, rules, window")
    ap.add_argument("--out", required=True, help="Output CSV path for the schedule")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = json.load(f)

    service_blocks, service_stage_orders = load_service_blocks(args.dict)
    jobs, unsched = build_job_instances(args.forecast, service_blocks, service_stage_orders)
    df = schedule_jobs(cfg, jobs, service_stage_orders)
    df.to_csv(args.out, index=False)

    v2, v12 = validate_schedule(df, cfg["rules"]["gap_after_finish_hours"], cfg["rules"]["gap_before_assembly_hours"])

    print(f"Saved schedule -> {args.out}")
    print(f"Jobs parsed: {len(jobs)} | Unschedulable rows: {len(unsched)}")
    if unsched:
        print("Unschedulable examples:", unsched[:3])
    if not df.empty:
        summary = df.groupby("Assigned To")["Hours"].sum().round(2)
        print("\nHours by worker:")
        print(summary.to_string())
    print(f"\nValidation: 2h-gap violations={v2}, 12h-before-assembly violations={v12}")

if __name__ == "__main__":
    main()
