"""
Stage 2a: Enrich Workday Bank Statement Lines
Join to WD Bank acct map, Calendar, FX. Compute derived fields.
"""
from pipeline.helpers import fx_lookup, serial_to_ymd, parse_date_str, clean_notes


def enrich_workday(bank_rows, wd_bank_acct_map, calendar_map, calendar_mapping_map,
                   fx_map, wd_acct_flag_map, log):
    """
    Enrich bank statement rows with calendar, FX, and entity mappings.
    Returns list of enriched WD row dicts.
    """
    log("Stage 2a: Enriching bank statement lines...")

    # Extrapolate Activity Week for dates beyond calendar_map.
    # The Calendar sheet uses Saturday-to-Friday 7-day weeks.
    # Find the start of AW=1 in the latest year and extrapolate.
    _aw_year_starts = {}  # year_start_serial -> True
    _last_aw1_start = None
    for _serial in sorted(calendar_map.keys()):
        if calendar_map[_serial]["activityWeek"] == 1:
            _last_aw1_start = _serial
            break
    # Each subsequent year's AW=1 starts 364 days (52 weeks) later
    if _last_aw1_start is not None:
        _next_year_start = _last_aw1_start + 364
    else:
        _next_year_start = None

    enriched = []
    no_map_count = 0
    no_fx_count = 0

    for row in bank_rows:
        bank_acct = str(row.get("Bank Account") or "").strip()
        date_val = row.get("Statement Line Date")
        try:
            amount = float(row.get("Statement Line Amount") or 0)
        except (ValueError, TypeError):
            amount = 0.0
        dr_cr = str(row.get("Debit/Credit") or "").strip().upper()
        ccy = str(row.get("Currency") or "").strip().upper()
        type_code = str(row.get("Type Code") or "").strip()
        addenda = str(row.get("Addenda") or "").strip()
        ref_num = str(row.get("Reference Number") or "").strip()

        # Parse date -> serial
        date_serial = None
        if isinstance(date_val, (int, float)):
            date_serial = int(date_val)
        else:
            date_serial = parse_date_str(date_val)

        # Join to WD Bank acct map
        acct_info = wd_bank_acct_map.get(bank_acct)
        flag_info = wd_acct_flag_map.get(bank_acct)
        entity = ""
        if acct_info:
            entity = acct_info.get("entity", "")
        elif flag_info:
            entity = flag_info.get("consolEntity", "")
        ref = ""
        if acct_info:
            ref = acct_info.get("ref", "")
        elif flag_info:
            ref = flag_info.get("ref", "")

        # Calendar join
        activity_week = ""
        week_ending = ""
        actuals_week = ""
        if date_serial is not None:
            cal_entry = calendar_map.get(date_serial)
            if cal_entry:
                activity_week = cal_entry["activityWeek"]
                actuals_week = cal_entry.get("actualsWeekEnd") or cal_entry.get("actualsWeekBeg") or ""
            cm_entry = calendar_mapping_map.get(date_serial)
            if cm_entry:
                week_ending = cm_entry.get("weekEnding", "")
                # Fallback: if calendar_map didn't have this date,
                # extrapolate Activity Week from the 7-day Saturday-Friday pattern.
                if activity_week == "" and _next_year_start is not None and date_serial >= _next_year_start:
                    activity_week = (date_serial - _next_year_start) // 7 + 1

        # FX lookup — Alteryx adjusts date by -1 day for FX rate lookup (Tools 232/236)
        # First try original date, then try date-1 as Alteryx does
        fx_date_serial = date_serial - 1 if date_serial is not None else None
        date_ymd = serial_to_ymd(fx_date_serial) if fx_date_serial is not None else ""
        fx_rate = 1.0
        if ccy and ccy != "USD" and date_ymd:
            rate = fx_lookup(fx_map, date_ymd, ccy)
            if rate is None and date_serial is not None:
                # Fallback to original date
                orig_ymd = serial_to_ymd(date_serial)
                rate = fx_lookup(fx_map, orig_ymd, ccy)
            if rate is not None:
                fx_rate = rate
            else:
                no_fx_count += 1

        # Derived fields
        net_native = -amount if dr_cr == "DR" else amount
        net_usd = net_native * fx_rate

        # Consolidated Entity
        consol_entity = "Bullish"
        if entity in ("CoinDesk Indices Inc", "CoinDesk Inc"):
            consol_entity = "CoinDesk"

        # UID = Bank Account + Type Code + Addenda
        uid = bank_acct + type_code + addenda

        # Inc/Excl — Alteryx Tool 187 formula:
        # IF [13WCF Ref #] IN ("Exclude", "TBD", "RCF") AND [13WCF Ref #] != "0" THEN "Exclude"
        # ELSEIF [13WCF Ref #] != "0" THEN "Include"
        # ELSE "Exclude" ENDIF
        # Note: In Alteryx, empty string "" != "0" evaluates to True → Include.
        # Python's truthy check differs, so we must NOT use `ref_str and ...`
        ref_str = str(ref).strip()
        if ref_str in ("Exclude", "TBD", "RCF"):
            incl_excl = "Exclude"
        elif ref_str != "0":
            incl_excl = "Include"
        else:
            incl_excl = "Exclude"

        # Rec/Disb
        rec_disb = "Disbursement" if net_usd < 0 else "Receipt"

        # Bank Acct Flag
        bank_acct_flag = "Not Mapped"
        if flag_info or acct_info:
            bank_acct_flag = "Mapped"

        if not acct_info and not flag_info:
            no_map_count += 1

        enriched.append({
            # Original fields
            "Bank Statement Line": row.get("Bank Statement Line") or "",
            "Bank Account": bank_acct,
            "Bank Statement": row.get("Bank Statement") or "",
            "Statement Line Date": date_serial,
            "Type Code": type_code,
            "Statement Line Amount": amount,
            "Debit/Credit": dr_cr,
            "Currency": ccy,
            "Reference Number": ref_num,
            "Addenda": addenda,
            "Reconciliation Status": row.get("Reconciliation Status") or "",
            "Auto Reconciled by Rule": row.get("Auto Reconciled by Rule") or "",
            "Last Updated Date Time": row.get("Last Updated Date Time") or "",

            # Enriched fields
            "Net Activity - Native": net_native,
            "FX": fx_rate,
            "Net Activity - USD": net_usd,
            "Activity Week": activity_week,
            "Week Ending": week_ending,
            "Actuals Week": actuals_week,
            "Consolidated Entity": consol_entity,
            "Entity": entity,
            "13WCF Ref #": ref,
            "UID": uid,
            "Inc/Excl": incl_excl,
            "Rec/Disb": rec_disb,
            "Bank Acct Flag": bank_acct_flag,

            # Matching stage fields
            "Source": "Workday",
            "Notes": addenda,
            "Account Name": bank_acct,
            "Asset Code": ccy,
            "Transaction Date": date_serial,
            "Provider Name": "N/A - Workday",
            "Sub Account Name": "N/A - Workday",
            "From Address": "",
            "To Address": "",
            "Tags": "",
            "House/Custodial": "",
            "Vendor": "",
            "Account Type": "",
            "Type": type_code,
            "Sub Type": "",
            "Notes_Clean": "",
            "Matched_Substring": "",
            "R/D": rec_disb,
            "13WCF Line Item Mapping": "",
            "Manual User Check": "",
            "Counter_Asset_Exists": 0,
            "Counter Asset Base Code": "",
        })

    log(f"  -> Enriched {len(enriched)} bank statement lines")
    if no_map_count > 0:
        log(f"  Warning: {no_map_count} unmapped bank accounts")
    if no_fx_count > 0:
        log(f"  Warning: {no_fx_count} missing FX rates")
    log("Stage 2a complete")

    return enriched
