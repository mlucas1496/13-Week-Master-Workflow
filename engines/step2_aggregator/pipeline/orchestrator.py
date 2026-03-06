"""
Pipeline orchestrator: coordinates all stages with progress callbacks.
"""
import os

from pipeline.stage1_inputs import load_inputs
from pipeline.stage6_fx import fetch_fx_rates
from pipeline.stage2a_workday import enrich_workday
from pipeline.stage2b_bth import enrich_bth
from pipeline.stage_rcf import process_rcf
from pipeline.stage3_matching import substring_match
from pipeline.stage4_fuzzy import fuzzy_match_workday
from pipeline.stage4_lukka import lukka_match
from pipeline.stage4_arap import arap_match
from pipeline.stage5_output import generate_output


def run_pipeline(file_paths, output_dir, log, set_stage):
    """
    Run the full pipeline.

    file_paths: dict with keys:
        prev_week, bank_statements, all_transactions, loan_report,
        search_strings, static_mapping
    output_dir: directory for output files
    log: callable(str) for logging messages
    set_stage: callable(stage_id, status) for progress tracking
        status: "run", "ok", "err"
    """
    os.makedirs(output_dir, exist_ok=True)

    # Stage 1: Load inputs
    set_stage("1", "run")
    inputs = load_inputs(file_paths, log)
    set_stage("1", "ok")

    # Stage FX: Fetch FX rates
    set_stage("FX", "run")
    fetch_fx_rates(inputs["fx_map"], log)
    set_stage("FX", "ok")

    # Pre-filter: Remove carry-over bank statement batches.
    # The bank statement file may include lines from small batches already
    # captured in the previous week. These have Bank Statement dates before
    # the main batch window, and are NOT on a Saturday (AW start day).
    from collections import Counter as _Counter
    from datetime import datetime as _dt
    _bs_date_counts = _Counter()
    _bs_date_strs = {}
    for _r in inputs["bank_rows"]:
        _bs = str(_r.get("Bank Statement") or "")
        _bs_d = _bs.split(": ")[-1] if ": " in _bs else ""
        if _bs_d:
            _bs_date_counts[_bs_d] += 1
            try:
                _bs_date_strs[_bs_d] = _dt.strptime(_bs_d, "%m/%d/%Y")
            except Exception:
                pass

    _all_bs_dates = sorted(_bs_date_strs.keys(), key=lambda d: _bs_date_strs[d])
    _first_large = None
    for _d in _all_bs_dates:
        if _bs_date_counts[_d] >= 10:
            _first_large = _d
            break

    # Only apply carry-over filtering when a Saturday (AW start) batch exists.
    # If all dates are weekdays, the bank statement pull is self-contained and
    # no carry-over filtering is needed.
    _has_saturday = any(dt.weekday() == 5 for dt in _bs_date_strs.values())

    _excluded_dates = set()
    if _first_large and _has_saturday:
        _large_dt = _bs_date_strs[_first_large]
        for _d in _all_bs_dates:
            _d_dt = _bs_date_strs.get(_d)
            if _d_dt and _d_dt < _large_dt and _bs_date_counts[_d] < 10:
                if _d_dt.weekday() != 5:  # 5 = Saturday (AW start)
                    _excluded_dates.add(_d)

    if _excluded_dates:
        _pre = len(inputs["bank_rows"])
        inputs["bank_rows"] = [
            r for r in inputs["bank_rows"]
            if (str(r.get("Bank Statement") or "").split(": ")[-1]
                if ": " in str(r.get("Bank Statement") or "") else "") not in _excluded_dates
        ]
        _deduped = _pre - len(inputs["bank_rows"])
        if _deduped:
            log(f"  -> Filtered {_deduped} carry-over bank rows (excluded BS dates: {sorted(_excluded_dates)})")

    # Stage 2a: Enrich Workday
    set_stage("2a", "run")
    wd_rows = enrich_workday(
        inputs["bank_rows"],
        inputs["wd_bank_acct_map"],
        inputs["calendar_map"],
        inputs["calendar_mapping_map"],
        inputs["fx_map"],
        inputs["wd_acct_flag_map"],
        log,
    )
    set_stage("2a", "ok")

    # Stage 2b: Enrich BTH
    set_stage("2b", "run")
    bth_rows = enrich_bth(
        inputs["all_txns"],
        inputs["wallet_map"],
        inputs["legal_entity_map"],
        inputs["lukka_ref_map"],
        inputs["calendar_map"],
        inputs["calendar_mapping_map"],
        log,
    )
    # Exclude Bitgo_2025_v2 rows entirely — not present in Alteryx target output
    pre_filter = len(bth_rows)
    bth_rows = [r for r in bth_rows if r.get("Account Name") != "Bitgo_2025_v2"]
    filtered = pre_filter - len(bth_rows)
    if filtered:
        log(f"  -> Filtered out {filtered} Bitgo_2025_v2 rows")
    set_stage("2b", "ok")

    # Stage RCF: determine current Activity Week from bank statement dates
    set_stage("RCF", "run")
    current_activity_week = 0
    for r in wd_rows:
        aw = r.get("Activity Week")
        if isinstance(aw, (int, float)) and aw > current_activity_week:
            current_activity_week = aw
    log(f"  Current Activity Week: {current_activity_week}")

    rcf_rows = process_rcf(
        inputs["loan_rows"],
        inputs["calendar_map"],
        inputs["calendar_mapping_map"],
        current_activity_week,
        log,
    )
    set_stage("RCF", "ok")

    # Stage 3: Substring matching
    # Only Include WD rows go into Alteryx_Output (Alteryx Tool 51 filter)
    set_stage("3", "run")
    wd_rows_include = [r for r in wd_rows if r.get("Inc/Excl") != "Exclude"]
    wd_rows_exclude_count = len(wd_rows) - len(wd_rows_include)
    log(f"  WD rows for Alteryx_Output: {len(wd_rows_include)} ({wd_rows_exclude_count} Exclude rows filtered)")
    all_rows = substring_match(wd_rows_include, bth_rows, rcf_rows, inputs["search_strings"], log)
    set_stage("3", "ok")

    # Stage 4a: Fuzzy matching
    set_stage("4a", "run")
    fuzzy_match_workday(all_rows, inputs["historicals"], log)
    set_stage("4a", "ok")

    # Stage 4b: Lukka matching
    set_stage("4b", "run")
    lukka_match(all_rows, log)
    set_stage("4b", "ok")

    # Stage 4c: AR/AP matching
    set_stage("4c", "run")
    arap_match(all_rows, inputs["supplier_rows"], inputs["customer_rows"], log)
    set_stage("4c", "ok")

    # Stage 5: Output
    set_stage("5", "run")
    output = generate_output(
        all_rows, bth_rows, wd_rows,
        inputs["prev_wb"], inputs["prev_sheet_names"],
        file_paths["prev_week"],
        output_dir, log,
    )
    set_stage("5", "ok")

    log("=" * 50)
    log("Pipeline complete! Download your file below.")

    return output
