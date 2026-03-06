"""
Stage 2b: Enrich BTH Trade Log (All Transactions / Lukka CSV)
Data cleanse, counter-asset rows, wallet mapping, ref mapping.
"""
from pipeline.helpers import parse_amount, is_stablecoin, serial_to_ymd, parse_date_str, parse_iso_date


def enrich_bth(all_txns, wallet_map, legal_entity_map, lukka_ref_map,
               calendar_map, calendar_mapping_map, log):
    """
    Enrich BTH rows: wallet mapping, ref mapping, calendar, counter-asset generation.
    Processes ALL rows (matching Alteryx behavior — no account pre-filtering).
    Returns list of enriched BTH row dicts.
    """
    log("Stage 2b: Enriching BTH Trade Log...")

    enriched = []
    counter_count = 0

    for i, txn in enumerate(all_txns):
        base_asset_code = str(txn.get("Base Asset Code") or "").strip()
        counter_asset_code = str(txn.get("Counter Asset Code") or "").strip()
        acct_name = str(txn.get("Account Name") or "").strip()
        sub_acct_name = str(txn.get("Sub Account Name") or "").strip()

        # Parse amounts
        base_amount = parse_amount(txn.get("Base Asset Amount"))
        counter_amount = parse_amount(txn.get("Counter Asset Amount"))
        fee_amount = parse_amount(txn.get("Fee Asset Amount"))
        rebate_amount = parse_amount(txn.get("Rebate Amount"))
        base_value = parse_amount(txn.get("Base Asset Value"))
        counter_value = parse_amount(txn.get("Counter Asset Value"))
        fee_value = parse_amount(txn.get("Fee Asset Value"))
        rebate_value = parse_amount(txn.get("Rebate Asset Value"))
        fiat_cash = parse_amount(txn.get("Fiat Cash Impact"))
        price = parse_amount(txn.get("Price"))

        # Counter-asset detection: both base AND counter are stablecoins
        # Alteryx also requires non-empty Counter Asset Code, Counter Asset Name, and Price
        counter_asset_name = str(txn.get("Counter Asset Name") or "").strip()
        is_counter_pair = (
            bool(counter_asset_code) and bool(counter_asset_name) and bool(price) and
            is_stablecoin(base_asset_code) and is_stablecoin(counter_asset_code)
        )

        # Parse date
        txn_date_str = str(txn.get("Transaction Date") or "").strip()
        date_serial = parse_date_str(txn_date_str)
        if date_serial is None:
            iso_date = parse_iso_date(txn_date_str)
            if iso_date:
                date_serial = parse_date_str(iso_date)

        # Build base row — original always has CE=0 (Alteryx behavior)
        base_row = _build_row(
            txn, acct_name, sub_acct_name, base_asset_code, base_amount,
            counter_asset_code, counter_amount, price, fee_amount, rebate_amount,
            base_value, counter_value, fee_value, rebate_value, fiat_cash,
            0, date_serial,
            wallet_map, legal_entity_map, lukka_ref_map, calendar_map, calendar_mapping_map
        )
        enriched.append(base_row)

        # Counter-asset row: mirror base<->counter — only mirror gets CE=1
        if is_counter_pair:
            counter_count += 1
            counter_row = _build_row(
                txn, acct_name, sub_acct_name,
                counter_asset_code, counter_amount,   # swap: counter becomes base
                base_asset_code, base_amount,          # swap: base becomes counter
                price, fee_amount, rebate_amount,
                counter_value, base_value, fee_value, rebate_value, fiat_cash,
                1, date_serial,
                wallet_map, legal_entity_map, lukka_ref_map, calendar_map, calendar_mapping_map
            )
            counter_row["Counter Asset Base Code"] = base_asset_code
            enriched.append(counter_row)

    log(f"  -> {len(enriched)} BTH rows ({counter_count} counter-asset rows added)")
    log("Stage 2b complete")

    return enriched


def _build_row(txn, acct_name, sub_acct_name, asset_code, amount,
               counter_asset_code, counter_amount, price, fee_amount, rebate_amount,
               base_value, counter_value, fee_value, rebate_value, fiat_cash,
               counter_asset_exists, date_serial,
               wallet_map, legal_entity_map, lukka_ref_map, calendar_map, calendar_mapping_map):
    """Build a single enriched BTH row dict."""

    # Wallet mapping: Alteryx uses AppendFields (cross-join) + exact match only
    # Match logic (Tool 202):
    #   For CB_ accounts with "Trading Balance" suffix: match on Account Name = Wallet Name
    #   For CB_ accounts otherwise: match on Concat(Account+Sub) = Wallet Name
    #   For non-CB accounts: match on Sub Account = Wallet Name OR Concat = Wallet Name
    #   If no Sub Account: match on Account Name = Wallet Name
    enterprise = ""
    custodial_house = ""
    vendor = ""

    w_match = None
    concat_sub = f"{acct_name}{sub_acct_name}" if sub_acct_name else acct_name

    if acct_name.startswith("CB"):
        if sub_acct_name and sub_acct_name.endswith("Trading Balance"):
            w_match = wallet_map.get(acct_name)
        else:
            w_match = wallet_map.get(concat_sub)
    else:
        if not sub_acct_name:
            w_match = wallet_map.get(acct_name)
        else:
            w_match = wallet_map.get(sub_acct_name) or wallet_map.get(concat_sub)

    # Prefix fallback: Alteryx AppendFields cross-join tries ALL wallet entries.
    # If exact match fails, find the first wallet whose name starts with Account Name.
    # This handles sub-accounts like "Spark.fi DAI" under BTH_Fireblocks where only
    # "BTH_FireblocksFunding" exists in the wallet map.
    if not w_match and acct_name:
        for wname, wval in wallet_map.items():
            if wname.startswith(acct_name) and len(wname) > len(acct_name):
                w_match = wval
                break

    if w_match:
        enterprise = w_match.get("enterprise", "")
        custodial_house = w_match.get("custodialHouse", "")
        vendor = w_match.get("vendor", "")

    # Account Type for Ref ID composition: pattern is "{Enterprise} Enterprise"
    account_type = f"{enterprise} Enterprise" if enterprise else ""

    # Build Ref ID
    ref_id = f"{acct_name}{sub_acct_name or ''}{asset_code}{enterprise}{custodial_house}{vendor}{account_type}"

    # Lukka Ref join
    ref = lukka_ref_map.get(ref_id, "")

    # Fallback: if no ref found, try Legal Entity Map cross-reference.
    # Some sub-accounts (e.g. PROD-GIB-BGI-BTG) use a different enterprise
    # than the wallet map's prefix match provides.
    if not ref and enterprise and enterprise in legal_entity_map:
        lem = legal_entity_map[enterprise]
        alt_ent = lem.get("enterprise", "")
        alt_at = lem.get("accountType", "")
        if alt_ent and alt_at and alt_ent != enterprise:
            alt_ref_id = f"{acct_name}{sub_acct_name or ''}{asset_code}{alt_ent}{custodial_house}{vendor}{alt_at}"
            alt_ref = lukka_ref_map.get(alt_ref_id, "")
            if alt_ref:
                ref = alt_ref
                enterprise = alt_ent
                account_type = alt_at
                ref_id = alt_ref_id

    # Special cases (Tool 208)
    concat_sub_acct = f"{acct_name}{sub_acct_name}"
    if acct_name == "Bullish - BitGo (20231009)":
        special_ref_ids = [
            "RLUSDBGIBitGoBGI EnterpriseHouse",
            "USDCBGIBitGoBGI EnterpriseHouse",
            "USDTBGIBitGoBGI EnterpriseHouse",
        ]
        if any(s in ref_id for s in special_ref_ids):
            ref = "38"

    if concat_sub_acct == "CB_Prime_BGI_CCI (House Trsy)USD Trading Balance":
        ref = "43"

    if not ref or ref == "null" or ref == "undefined":
        ref = "0"

    # Inc/Excl
    try:
        ref_num = int(ref)
    except (ValueError, TypeError):
        ref_num = 0
    incl_excl = "Include" if ref_num > 0 else "Exclude"

    # Consolidated Entity: always Bullish for BTH
    consol_entity = "Bullish"

    # Calendar joins
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
            week_ending = cm_entry["weekEnding"]

    # New Wallet Check
    new_wallet_check = "Pass"
    sub_upper = str(sub_acct_name or "").upper()
    if ("PROD" in sub_upper and
            ("REVENUE" in sub_upper or "MARGIN" in sub_upper) and
            is_stablecoin(asset_code)):
        new_wallet_check = "Check"

    # Net Activity - USD
    net_usd = amount

    # Build row
    row = {
        "Account Name": acct_name,
        "Sub Account Name": sub_acct_name,
        "Base Asset Code": asset_code,
        "Base Asset Amount": amount,
        "Counter Asset Code": counter_asset_code,
        "Counter Asset Amount": counter_amount,
        "Ref ID": ref_id,
        "Ref": ref,
        "13WCF Ref #": ref,
        "13WCFIncl/Excl": incl_excl,
        "Inc/Excl": incl_excl,
        "Consolidated Entity": consol_entity,
        "Counter_Asset_Exists": counter_asset_exists,
        "Counter Asset Base Code": counter_asset_code if counter_asset_exists else "",
        "New Wallet Check": new_wallet_check,
        "Source": "Lukka",
        "Asset Code": asset_code,
        "Net Activity - USD": net_usd,
    }

    # Conditionally assign non-empty values
    if txn.get("Entity Name"):
        row["Entity Name"] = txn["Entity Name"]
    if txn.get("Provider Name"):
        row["Provider Name"] = txn["Provider Name"]
    if txn.get("Account Number"):
        row["Account Number"] = txn["Account Number"]
    if txn.get("Transaction ID"):
        row["Transaction ID"] = txn["Transaction ID"]
    if txn.get("Blockchain Transaction ID"):
        row["Blockchain Transaction ID"] = txn["Blockchain Transaction ID"]
    if txn.get("From Address"):
        row["From Address"] = txn["From Address"]
    if txn.get("To Address"):
        row["To Address"] = txn["To Address"]
    if txn.get("Order ID"):
        row["Order ID"] = txn["Order ID"]
    if date_serial is not None:
        row["Transaction Date"] = date_serial
    if txn.get("Type"):
        row["Type"] = txn["Type"]
    if txn.get("Sub Type"):
        row["Sub Type"] = txn["Sub Type"]
    if txn.get("Cr/Dr"):
        row["Cr/Dr"] = txn["Cr/Dr"]
    if txn.get("Base Asset Name"):
        row["Base Asset Name"] = txn["Base Asset Name"]
    if price:
        row["Price"] = price
    if txn.get("Counter Asset Name"):
        row["Counter Asset Name"] = txn["Counter Asset Name"]
    if txn.get("Fee Asset Code"):
        row["Fee Asset Code"] = txn["Fee Asset Code"]
    if txn.get("Fee Asset Name"):
        row["Fee Asset Name"] = txn["Fee Asset Name"]
    if fee_amount:
        row["Fee Asset Amount"] = fee_amount
    if txn.get("Rebate Asset Code"):
        row["Rebate Asset Code"] = txn["Rebate Asset Code"]
    if txn.get("Rebate Asset Name"):
        row["Rebate Asset Name"] = txn["Rebate Asset Name"]
    if rebate_amount:
        row["Rebate Amount"] = rebate_amount
    if txn.get("Reference Currency"):
        row["Reference Currency"] = txn["Reference Currency"]
    if base_value:
        row["Base Asset Value"] = base_value
    if counter_value:
        row["Counter Asset Value"] = counter_value
    if fee_value:
        row["Fee Asset Value"] = fee_value
    if rebate_value:
        row["Rebate Asset Value"] = rebate_value
    if fiat_cash:
        row["Fiat Cash Impact"] = fiat_cash
    if txn.get("Source"):
        row["Source_CSV"] = txn["Source"]
    if txn.get("Process"):
        row["Process"] = txn["Process"]
    if txn.get("Counterparty"):
        row["Counterparty"] = txn["Counterparty"]
    if txn.get("Tags"):
        row["Tags"] = txn["Tags"]
    if txn.get("Notes"):
        row["Notes"] = txn["Notes"]
    if enterprise:
        row["Enterprise"] = enterprise
    if custodial_house:
        row["House/Custodial"] = custodial_house
    if vendor:
        row["Vendor"] = vendor
    if account_type:
        row["Account Type"] = account_type
    if activity_week:
        row["Activity Week"] = activity_week
    if actuals_week:
        row["Actuals Week"] = actuals_week
    if week_ending:
        row["Week Ending"] = week_ending

    return row
