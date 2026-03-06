"""
Stage 3: Substring Matching + 13WCF Line Mapping
Union WD + BTH + RCF, clean notes, build UID, substring match, line mapping.
"""
from pipeline.helpers import clean_notes


def substring_match(wd_rows, bth_rows, rcf_rows, search_strings, log):
    """
    Union all rows, apply substring matching and 13WCF line mapping.
    Returns the combined list of all rows with matches applied.
    """
    log("Stage 3: Substring matching + 13WCF line mapping...")

    # Union all streams
    all_rows = wd_rows + bth_rows + rcf_rows
    log(f"  -> Union: {len(wd_rows)} WD + {len(bth_rows)} BTH + {len(rcf_rows)} RCF = {len(all_rows)} total")

    # Pre-clean all substrings (maintain priority order from file)
    cleaned_substrings = []
    for idx, ss in enumerate(search_strings):
        original = str(ss.get("Substrings") or ss.get("Substring") or "").strip()
        cleaned = clean_notes(original)
        if not cleaned:
            continue
        cleaned_substrings.append({
            "original": original,
            "clean": cleaned,
            "rd": str(ss.get("R/D") or "").strip(),
            "consolEntity": str(ss.get("Consolidated Entity") or "").strip(),
            "mapping": str(ss.get("13WCF Line Item Mapping") or "").strip(),
            "priority": idx,
        })

    log(f"  -> {len(cleaned_substrings)} search strings loaded")

    # Build 13WCF Line Mapping lookup: "CleanSubstring|ConsolEntity|R/D" -> mapping
    line_mapping_lookup = {}
    for ss in cleaned_substrings:
        key = f"{ss['clean']}|{ss['consolEntity']}|{ss['rd']}"
        if key not in line_mapping_lookup:
            line_mapping_lookup[key] = ss["mapping"]

    matched_count = 0
    already_mapped = 0

    for row in all_rows:
        # Skip RCF rows (already mapped)
        if row.get("Source") == "Airtable":
            already_mapped += 1
            continue

        # Clean notes for matching
        notes = row.get("Notes") or row.get("Addenda") or ""
        notes_clean = clean_notes(notes)
        row["Notes_Clean"] = notes_clean

        # Build UID
        asset_code = row.get("Asset Code") or row.get("Base Asset Code") or ""
        txn_date = row.get("Transaction Date") or ""
        acct_name = row.get("Account Name") or ""
        row["UID"] = str(notes_clean) + str(asset_code) + str(txn_date) + str(acct_name)

        # R/D assignment based on Net Activity
        net_usd = row.get("Net Activity - USD") or 0
        rd = "Disbursement" if net_usd < 0 else "Receipt"
        row["R/D"] = rd

        # Skip if already has a mapping
        if row.get("13WCF Line Item Mapping"):
            already_mapped += 1
            continue

        # Substring match: scan all cleaned substrings, first match wins
        best_match = None
        uid = row["UID"]
        for ss in cleaned_substrings:
            if ss["clean"] in uid:
                best_match = ss
                break

        if best_match:
            row["Matched_Substring"] = best_match["clean"]

            # 13WCF Line Mapping join
            consol_entity = row.get("Consolidated Entity") or ""
            lookup_key = f"{best_match['clean']}|{consol_entity}|{rd}"
            mapping = line_mapping_lookup.get(lookup_key)

            if mapping:
                row["13WCF Line Item Mapping"] = mapping
            else:
                row["13WCF Line Item Mapping"] = best_match.get("mapping", "")
            matched_count += 1

        # Special cases
        _apply_special_cases(row)

    log(f"  -> Substring matched: {matched_count}, already mapped: {already_mapped}")
    log("Stage 3 complete")

    return all_rows


def _apply_special_cases(row):
    """Apply special formula rules from Tool 43."""
    sub_acct = str(row.get("Sub Account Name") or "").strip()
    acct_name = str(row.get("Account Name") or "").strip()
    rd = row.get("R/D")
    ref = str(row.get("13WCF Ref #") or "").strip()
    notes_clean = row.get("Notes_Clean") or ""

    # Formula 1: PROD-GIB-BGI-BTG-ETH-REVENUE-20211015 + Receipt
    if sub_acct == "PROD-GIB-BGI-BTG-ETH-REVENUE-20211015" and rd == "Receipt":
        row["13WCF Line Item Mapping"] = "Spot Trading Revenue Take"

    # Formula 2: PROD-GIB-BGI-BTG-ETH-MARGIN-20230209 + Receipt
    if sub_acct == "PROD-GIB-BGI-BTG-ETH-MARGIN-20230209" and rd == "Receipt":
        row["13WCF Line Item Mapping"] = "LOC Interest"

    # Formula 3: CB_Prime_BGI_CCI (House Trsy) + USDC Trading Balance + Receipt
    if (acct_name == "CB_Prime_BGI_CCI (House Trsy)" and
            sub_acct == "USDC Trading Balance" and rd == "Receipt"):
        row["13WCF Line Item Mapping"] = "Spot Trading Revenue Take"

    # Formula 4: 13WCF Ref# = "92" routing
    # Alteryx formula for ref=92:
    # IF Contains(Notes_Clean, "FUNDSTRANSFER") THEN Intercompany Inflow/Outflow by R/D
    # ELSE Nonco Inflow/Outflow by R/D (Receipt = Inflow, Disbursement = Outflow)
    if ref == "92":
        if "FUNDSTRANSFER" in notes_clean:
            row["13WCF Line Item Mapping"] = "Intercompany Inflow" if rd == "Receipt" else "Intercompany Outflow"
        elif not row.get("13WCF Line Item Mapping"):
            row["13WCF Line Item Mapping"] = "Nonco Inflow" if rd == "Receipt" else "Nonco Outflow"
