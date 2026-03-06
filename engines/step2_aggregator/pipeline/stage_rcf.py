"""
Stage RCF: Airtable RCF Loan Processing
Filter LoanMasterReport for USDC + Complete + trigger date in current Activity Week.
"""
from pipeline.helpers import parse_date_str, parse_amount


def _parse_date_only(s):
    """Parse a date string that may include time (e.g., '02/26/2026 11:00')."""
    if not s:
        return None
    date_only = str(s).strip().split(" ")[0]
    return parse_date_str(date_only)


def process_rcf(loan_rows, calendar_map, calendar_mapping_map, current_activity_week, log):
    """
    Process RCF loans: filter USDC Complete, match to current Activity Week.
    Returns list of RCF row dicts formatted for Alteryx_Output.
    """
    log("Stage RCF: Processing RCF loans...")

    # Extrapolate Activity Week for dates beyond calendar_map.
    # The Calendar sheet uses Saturday-to-Friday 7-day weeks.
    _last_aw1_start = None
    for _serial in sorted(calendar_map.keys()):
        if calendar_map[_serial]["activityWeek"] == 1:
            _last_aw1_start = _serial
            break
    if _last_aw1_start is not None:
        _next_year_start = _last_aw1_start + 364
    else:
        _next_year_start = None

    # Filter for Borrowed Asset = "USDC" AND Loan Status = "Complete"
    usdc_complete = [
        r for r in loan_rows
        if str(r.get("Borrowed Asset") or "").strip().upper() == "USDC"
        and str(r.get("Loan Status") or "").strip() == "Complete"
    ]
    log(f"  -> {len(usdc_complete)} USDC Complete loans found")

    rcf_rows = []

    for loan in usdc_complete:
        # Parse trigger date and payment date
        trig_serial = _parse_date_only(loan.get("Trigger Date_Format"))
        pay_serial = _parse_date_only(loan.get("Final Loan Payment Date_Format"))
        if trig_serial is None:
            continue

        # Use Payment Date for Activity Week matching (matches Alteryx behavior).
        # Fall back to Trigger Date if Payment Date is not available.
        aw_date_serial = pay_serial if pay_serial is not None else trig_serial

        # Determine Activity Week for the matching date.
        cal_entry_aw = calendar_map.get(aw_date_serial)
        if cal_entry_aw:
            date_aw = cal_entry_aw["activityWeek"]
        elif _next_year_start is not None and aw_date_serial >= _next_year_start:
            date_aw = (aw_date_serial - _next_year_start) // 7 + 1
        else:
            continue

        if date_aw is None or date_aw != current_activity_week:
            continue

        # Calendar joins (from trigger date for enrichment fields)
        activity_week = date_aw
        cal_entry = calendar_map.get(trig_serial)
        cm_entry = calendar_mapping_map.get(trig_serial)
        week_ending = cm_entry["weekEnding"] if cm_entry else ""
        actuals_week = ""
        if cal_entry:
            actuals_week = cal_entry.get("actualsWeekEnd")
            if actuals_week is None or actuals_week == "":
                actuals_week = cal_entry.get("actualsWeekBeg", "")

        # Net Activity = Actual Interest Income (rounded to 2 decimal places)
        net_usd = round(parse_amount(loan.get("Actual Interest Income")), 2)

        rcf_rows.append({
            "Account Name": "Airtable",
            "Transaction Date": pay_serial or trig_serial,
            "Asset Code": "USDC",
            "Notes": str(loan.get("ETF_Type") or "").strip(),
            "Net Activity - USD": net_usd,
            "Activity Week": activity_week,
            "Week Ending": week_ending,
            "Actuals Week": actuals_week,
            "Consolidated Entity": "Bullish",
            "Entity/Enterprise": "BTH",
            "13WCF Ref #": "47",
            "Inc/Excl": "Include",
            "Provider Name": "N/A - Airtable",
            "Sub Account Name": "N/A - Airtable",
            "From Address": "N/A - Airtable",
            "To Address": "N/A - Airtable",
            "Tags": "N/A - Airtable",
            "House/Custodial": "N/A - Airtable",
            "Vendor": "N/A - Airtable",
            "Account Type": "N/A - Airtable",
            "Source": "Airtable",
            "Type": "N/A - Airtable",
            "Sub Type": "N/A - Airtable",
            "13WCF Line Item Mapping": "RCFs",
            "Manual User Check": "Mapped",

            # RCF-specific loan fields
            "Repayment Asset": loan.get("Repayment Asset") or "",
            "user app": True,
            "Loan ID": loan.get("Loan ID") or "",
            "Est. Total BPS": loan.get("Est. Total BPS") or "",
            "Origination: Tx ID": loan.get("Origination: Tx ID") or "",
            "Final Interest Mode (Calc) (from Approved Loans)": loan.get("Final Interest Mode (Calc) (from Approved Loans)") or "",
            "Final Principal Mode (Calc) (from Approved Loans)": loan.get("Final Principal Mode (Calc) (from Approved Loans)") or "",
            "Interest Type": loan.get("Interest Type") or "",
            "Loan Type": loan.get("Loan Type") or "",
            "Submission Date": loan.get("Submission Date") or "",
            "Trigger Date_Format": loan.get("Trigger Date_Format") or "",
            "Asset Count": loan.get("Asset Count") or "",
            "Implied Price": loan.get("Implied Price") or "",
            "Calculated Interest Income": loan.get("Calculated Interest Income") or "",
            "Interest Income Rate (BPS) Calculated*": loan.get("Interest Income Rate (BPS) Calculated*") or "",
            "Actual Interest Income": loan.get("Actual Interest Income") or "",
            "Principal Outstanding": loan.get("Principal Outstanding") or "",
            "Final Principal Date (Calc) (from Approved Loans)": loan.get("Final Principal Date (Calc) (from Approved Loans)") or "",
            "Final Interest Date (Calc) (from Approved Loans)": loan.get("Final Interest Date (Calc) (from Approved Loans)") or "",
            "Final Principal TX ID (Calc) (from Approved Loans)": loan.get("Final Principal TX ID (Calc) (from Approved Loans)") or "",
            "Final Interest TX ID (Calc) (from Approved Loans) 2": loan.get("Final Interest TX ID (Calc) (from Approved Loans) 2") or "",
            "Origination: Tx Date": loan.get("Origination: Tx Date") or "",
        })

    log(f"  -> Generated {len(rcf_rows)} RCF rows (Activity Week {current_activity_week})")
    log("Stage RCF complete")

    return rcf_rows
