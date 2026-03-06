"""
Stage 4b: Lukka Rule-Based Matching + Cross-Source Intercompany Detection
UIDNEW rules, intercompany detection (ALL sources), entity-name intercompany,
counter-asset intercompany.
"""
import re
from pipeline.helpers import clean_notes

# Known Bullish Group / CoinDesk / related entity names for intercompany detection.
# Wire transfers to/from these entities are intercompany flows.
# This mirrors Alteryx's FuzzyMatch-based propagation from historical data.
_INTERCOMPANY_ENTITIES = [
    "BULLISH",          # Matches any Bullish subsidiary
    "COINDESK",         # CoinDesk entities
    "BLOCK ONE",        # Block.one (parent)
    "BLOCK.ONE",
]

# More specific patterns that should NOT be treated as intercompany
# (e.g., "COINBASE" contains "COIN" but is NOT intercompany)
_INTERCOMPANY_EXCLUSIONS = [
    "COINBASE",         # External exchange, not intercompany
]


def _is_intercompany_entity(notes_clean):
    """
    Check if cleaned notes contain a known intercompany entity name.
    Returns True if an intercompany entity is found and no exclusion applies.
    """
    if not notes_clean:
        return False
    for excl in _INTERCOMPANY_EXCLUSIONS:
        # Check exclusions first — if COINBASE appears before COIN check
        pass  # We'll handle this below

    for entity in _INTERCOMPANY_ENTITIES:
        if entity in notes_clean:
            # Check that this isn't actually an excluded entity
            excluded = False
            for excl in _INTERCOMPANY_EXCLUSIONS:
                if excl in notes_clean:
                    # Check if the entity match is really an exclusion
                    # e.g., "COINBASE" contains "COIN" — but "COINDESK" is separate
                    pos_entity = notes_clean.find(entity)
                    pos_excl = notes_clean.find(excl)
                    # If the exclusion starts at same position or contains the entity, skip
                    if pos_excl <= pos_entity < pos_excl + len(excl):
                        excluded = True
                        break
            if not excluded:
                return True
    return False


def lukka_match(all_rows, log):
    """
    Apply Lukka-specific matching rules to unmapped Lukka Include rows,
    then run cross-source intercompany detection on ALL Include rows.
    Returns total count of matches applied.
    """
    log("Stage 4b: Lukka rule-based matching + intercompany detection...")

    # Filter for unmapped Lukka Include records
    lukka_unmapped = [
        r for r in all_rows
        if r.get("Source") == "Lukka"
        and r.get("Inc/Excl") == "Include"
        and not r.get("13WCF Line Item Mapping")
    ]

    log(f"  -> {len(lukka_unmapped)} unmapped Lukka records")

    rule_count = 0

    # Rule-based matching
    for row in lukka_unmapped:
        acct_name = str(row.get("Account Name") or "").strip()
        asset_code = str(row.get("Asset Code") or row.get("Base Asset Code") or "").strip()
        counter_code = str(row.get("Counter Asset Base Code") or row.get("Counter Asset Code") or "").strip()
        uid_new = acct_name + asset_code + counter_code

        sub_type = str(row.get("Sub Type") or "").strip()
        type_val = str(row.get("Type") or "").strip()

        # Rule 1: Specific UIDNEW match
        if uid_new == "Binance-BTH (Adam B)USDTETHFI":
            row["13WCF Line Item Mapping"] = "EtherFi Selldown"
            rule_count += 1
            continue

        # Rule 2: Staking Income
        if sub_type == "Staking" and type_val == "Income":
            row["13WCF Line Item Mapping"] = "Rewards and Other Interest"
            rule_count += 1
            continue

    log(f"  -> Rule-based matches: {rule_count}")

    # ── Intercompany detection: ref-group sum = 0 ──
    # Alteryx (Tools 325-329): filters ALL Include rows (WD + Lukka),
    # groups by ref, sums ALL amounts, rounds to integer, checks if = 0,
    # then only assigns mapping to unmapped rows.
    # NOTE: Airtable/RCF rows are excluded from the sum calculation because
    # in the Alteryx workflow, RCF rows are appended AFTER intercompany detection.
    ref_groups = {}
    all_include = [
        r for r in all_rows
        if r.get("Inc/Excl") == "Include"
        and r.get("Source") != "Airtable"
    ]

    for row in all_include:
        ref = str(row.get("13WCF Ref #") or "").strip()
        if ref == "0":
            continue
        if ref not in ref_groups:
            ref_groups[ref] = {"sum": 0.0, "rows": []}
        g = ref_groups[ref]
        g["sum"] += (row.get("Net Activity - USD") or 0)
        g["rows"].append(row)

    interco_count = 0
    for ref, group in ref_groups.items():
        rounded_sum = round(group["sum"])
        if rounded_sum == 0:
            for row in group["rows"]:
                if not row.get("13WCF Line Item Mapping"):
                    rd = row.get("R/D") or ("Disbursement" if (row.get("Net Activity - USD") or 0) < 0 else "Receipt")
                    row["13WCF Line Item Mapping"] = "Intercompany Inflow" if rd == "Receipt" else "Intercompany Outflow"
                    interco_count += 1

    log(f"  -> Intercompany detection (ref-group sum): {interco_count} matches")

    # ── Entity-name intercompany detection (WD only) ──
    # Catches WD rows where notes mention a known Bullish/CoinDesk entity.
    # This mirrors Alteryx's FuzzyMatch propagation from historical data where
    # similar wire transfers to Bullish entities were previously mapped as Intercompany.
    entity_interco_count = 0
    for row in all_rows:
        if row.get("Source") != "Workday":
            continue
        if row.get("Inc/Excl") != "Include":
            continue
        if row.get("13WCF Line Item Mapping"):
            continue

        notes = row.get("Notes") or row.get("Addenda") or ""
        notes_upper = clean_notes(notes)

        if _is_intercompany_entity(notes_upper):
            rd = row.get("R/D") or ("Disbursement" if (row.get("Net Activity - USD") or 0) < 0 else "Receipt")
            row["13WCF Line Item Mapping"] = "Intercompany Inflow" if rd == "Receipt" else "Intercompany Outflow"
            row["Matched_Substring"] = "EntityIntercompany"
            entity_interco_count += 1

    log(f"  -> Entity-name intercompany: {entity_interco_count} matches")

    # ── FX transaction intercompany detection (WD only) ──
    # FX conversion transactions (e.g., "FX [ref] HKD [amount] RATE: [rate]")
    # on CoinDesk accounts with specific refs are intercompany flows.
    fx_interco_count = 0
    for row in all_rows:
        if row.get("Source") != "Workday":
            continue
        if row.get("Inc/Excl") != "Include":
            continue
        if row.get("13WCF Line Item Mapping"):
            continue

        notes_clean = row.get("Notes_Clean") or clean_notes(row.get("Notes") or "")
        if notes_clean.startswith("FX") and ("HKD" in notes_clean or "CNH" in notes_clean):
            acct = str(row.get("Account Name") or "").strip()
            if "CoinDesk" in acct or "Silicon Valley" in acct:
                rd = row.get("R/D") or ("Disbursement" if (row.get("Net Activity - USD") or 0) < 0 else "Receipt")
                row["13WCF Line Item Mapping"] = "Intercompany Inflow" if rd == "Receipt" else "Intercompany Outflow"
                row["Matched_Substring"] = "FXIntercompany"
                fx_interco_count += 1

    log(f"  -> FX intercompany: {fx_interco_count} matches")

    # ── Counter-asset intercompany (Lukka only) ──
    counter_interco_count = 0
    for row in all_rows:
        if row.get("Source") != "Lukka":
            continue
        if row.get("Inc/Excl") != "Include":
            continue
        if row.get("13WCF Line Item Mapping"):
            continue
        if row.get("Counter_Asset_Exists") != 1:
            continue

        rd = row.get("R/D") or ("Disbursement" if (row.get("Net Activity - USD") or 0) < 0 else "Receipt")
        row["13WCF Line Item Mapping"] = "Intercompany Inflow" if rd == "Receipt" else "Intercompany Outflow"
        counter_interco_count += 1

    log(f"  -> Counter-asset intercompany: {counter_interco_count} matches")

    total_matches = rule_count + interco_count + entity_interco_count + fx_interco_count + counter_interco_count
    log(f"  -> Total matches: {total_matches}")
    log("Stage 4b complete")

    return total_matches
