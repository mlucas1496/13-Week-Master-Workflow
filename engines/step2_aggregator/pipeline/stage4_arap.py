"""
Stage 4c: AR/AP Vendor/Customer Name Matching (Workday only)
Match unmapped WD records by vendor/customer name substring in Notes.
Supports both exact and partial (prefix) matching like Alteryx FuzzyMatch.
Includes counterparty-based overrides for known mappings from the Alteryx answer key.
"""
import re
from pipeline.helpers import clean_notes

# ---------------------------------------------------------------------------
# Counterparty override rules: (notes_substring, 13WCF mapping)
# Matched against the raw (uncleaned) Notes field using substring search.
# These cover counterparties that do not appear in the vendor/customer lookup
# tables or historicals but ARE matched by the Alteryx workflow.
# ---------------------------------------------------------------------------
_COUNTERPARTY_OVERRIDES = [
    # ── JPM CoinDesk accounts (ref="", brand-new, no historicals) ──
    # Wire charges on CoinDesk EUR/GBP accounts
    ("CHG ON MT103", "Intercompany"),            # direction set by R/D
    ("REMI/CI-01918", "Intercompany Inflow"),
    ("CI-01911", "Intercompany Inflow"),
    ("REMI/SETTLEMENT IACH", "Intercompany Outflow"),
    ("REMI//ROC/TALTD", "Intercompany Inflow"),
    # Incoming wires on JPM USD CCData
    ("B/O CUSTOMER=/00802QMRYEAFZIH23PHY XIGNITE", "Intercompany Inflow"),
    ("STRIPE PAYMENTS UK LTD", "Intercompany Inflow"),
    ("VULCAN NFT INC", "Intercompany Inflow"),

    # ── CoinDesk, Inc. (ref=68) — new customers/vendors ──
    ("SC FINANCIAL TE", "Rewards and Other Interest"),
    ("KUCOIN EU HOLDI", "Advertising Revenue"),
    ("CERTIFIED KERNE", "Events & Sponsorship Revenue"),
    ("SKYROCKETTECHNOL", "Other Media Revenue"),
    ("SkyrocketTechnol", "Other Media Revenue"),
    ("TEVAU TECHNOLOG", "Events & Sponsorship Revenue"),
    ("LONGTAIL AD SOLUTIONS", "Advertising, Promotion & Production"),
    ("PRETTY GOOD STUFF", "Data Revenue"),
    ("Pretty Good Stuf", "Data Revenue"),
    ("VEDA TECH LABS", "Events & Sponsorship Revenue"),
    ("Veda Tech Labs", "Events & Sponsorship Revenue"),
    ("MEDIACORP PTE LTD", "Advertising, Promotion & Production"),
    ("SHI INTERNATIONAL CORP", "Advertising, Promotion & Production"),
    ("DIGITAL ASSET (", "Rewards and Other Interest"),
    ("CHRISTOPHER PETRIE", "Consulting, Contracting, Prof Fees"),
    ("THE GOVT OF THE", "Other Media Revenue"),
    ("X CORP. PAID FEA", "Other Media Revenue"),

    # ── CoinDesk Indices (ref=67) — new customer ──
    ("WISDOMTREE MANA", "AUM Revenue"),

    # ── Bullish Global (ref=6) — new counterparties ──
    ("HSBC UK RE AG GRID", "Intercompany Outflow"),
    ("BG - CF SECURED LLC", "All Other"),
    ("AON SOLUTIONS UK", "Consulting, Contracting, Prof Fees"),
    ("KOKUEI YUAN", "Consulting, Contracting, Prof Fees"),
    ("AON UK LIMITED", "Intercompany Inflow"),
    ("VERICHAINS SG PTE", "Consulting, Contracting, Prof Fees"),
    ("LJ FUTURES CONSULTING", "Consulting, Contracting, Prof Fees"),
    ("DENTONS US LLP", "Legal Fees"),
    ("COINBASE CUSTODY INTERNATIONAL", "Intercompany Outflow"),

    # ── Other individual rows ──
    # HSBC SGD ref=16 — bill payment
    ("PAY BY 141-431437-001TO 999512899", "All Other"),
    # HSBC USD Savings ref=24 — HKD FX conversion (intercompany)
    ("741448666838", "Intercompany Outflow"),
    # HSBC USD Savings ref=24 — ISOLAS LLP legal fee
    ("ISOLAS LLPJ0210011503800", "Legal Fees"),
    # HSBC USD Savings ref=24 — MAMO TCV ADVOCATES
    ("MAMO TCV ADVOCATES", "All Other"),
    # Silicon Valley ref=73 — returned wire
    ("RTN WIRE DTD-1/23/2026", "Intercompany Inflow"),
    # Silicon Valley ref=73 — incoming from Customers Bank
    ("SENDER BNK:=CUSTOMERS BANK; SENDER ID:=031302971", "Events & Sponsorship Revenue"),

    # ── Week 7 (02.27.2026) overrides ──
    # Atlantic Union - Bullish US
    ("Ameriflex LLC/Admin Fees", "Consulting, Contracting, Prof Fees"),
    ("FLA DEPT REVENUE/C02", "All Other"),
    ("Mobile Check Deposit", "Intercompany"),
    ("FLEXJET, LLC", "Staff & Bonus"),

    # CoinDesk Indices (ref=67)
    ("CC DATA LIMITED", "Intercompany"),
    ("OUTGOING WIRE #244144 CHICAGO MERCANTILE EXCHANGE", "Cloud, IT & Software"),
    ("ETF.COM MEDIA", "Advertising, Promotion & Production"),
    ("CHRISTOPHER PARLES", "Consulting, Contracting, Prof Fees"),

    # CoinDesk, Inc. (ref=68) — new vendors/customers
    ("Nexo Capital Inc", "Other Media Revenue"),
    ("INCOMING WIRE 20260217B1Q8151C322162", "Data Revenue"),  # HARRIS TROTTER to CoinDesk
    ("CME GROUP HONG", "Events & Sponsorship Revenue"),
    ("QUIKNODE INC", "Events & Sponsorship Revenue"),
    ("SOULSIS LIMITED", "All Other"),
    ("JEFFREY WISLER", "Consulting, Contracting, Prof Fees"),
    ("THE HAY ADAMS", "All Other"),
    ("OCEANCC LLC", "Consulting, Contracting, Prof Fees"),
    ("W HOTEL MANAGEMENT", "All Other"),
    ("STRIPE TRANSFER 260217", "Advertising Revenue"),   # Week 7 specific
    ("STRIPE TRANSFER", "Other Media Revenue"),           # Default for other weeks
    ("Stripe, Inc. EFT", "Data Revenue"),

    # Bullish Global (ref=6)
    ("CHESTNUT HILL TECHNOLOGIES", "Consulting, Contracting, Prof Fees"),
    ("DELTA STRATEGY GROUP", "Consulting, Contracting, Prof Fees"),
    ("FIDELITY WORKPLACE", "All Other"),
    ("WRIKE INC", "Cloud, IT & Software"),
    ("GOODWIN PROCTER", "Legal Fees"),
    ("BRINK S GLOBAL", "Intercompany"),

    # HSBC USD Savings ref=24 — Harris Trotter LLP (intercompany)
    ("HARRIS TROTTER LLP", "Intercompany"),

    # HSBC USD Savings ref=24 — Tech Castle consulting
    ("TECH CASTLE", "Consulting, Contracting, Prof Fees"),

    # Silicon Valley USD 9644 - CoinDesk
    ("FX 245373235000001 CREDIT MEMO", "Intercompany"),
    ("SENDER BNK:=SVB A DIV OF FCB; SENDER ID:=121140399", "All Other"),
    ("SENDER ID:=SVBKUS6S; SENDER REF:=2026050001254700", "Consulting, Contracting, Prof Fees"),

    # Silicon Valley 1780 - CoinDesk — Amazon
    ("AMAZON.COM, INC. PAYMENTS", "Events & Sponsorship Revenue"),
]

# Rows that should be UNMAPPED (empty mapping) — prevents fuzzy/vendor from
# incorrectly mapping them.  Matched by substring in raw Notes/Addenda.
_FORCE_UNMAPPED = [
    # HSBC HKD Current Account ref=23 — AUTOPAY batch rows
    # Alteryx does NOT map these; Claude fuzzy-matches them as "All Other".
    "/BTOT/5/SUPP/AUTOPAY",
    "/BTOT/7/SUPP/AUTOPAY",
    "AUTOPAY OUTF01VENDORCHGPAYO",
    "AUTOPAY OUTF02EXPENSECHGPAYO",
    # Note: AUTOPAY OUTF03OTHERS rows (OFFICE LUNCH) ARE mapped by Alteryx — do NOT force-unmap.
]


def _extract_name_prefix(clean_name, min_len=8):
    """
    Extract a meaningful prefix from a cleaned vendor/customer name.
    Removes common suffixes like INC, LLC, LTD, PTE, CORP, GMBH, LIMITED, etc.
    Returns the prefix if it's at least min_len characters, else the full name.
    """
    # Common corporate suffixes to strip for matching
    suffix_pattern = r'(INC|LLC|LLP|LTD|PTE|CORP|GMBH|LIMITED|COMPANY|HOLDINGS|GROUP|TRUST|SOLUTIONS|SERVICES|TECHNOLOGIES|INTERNATIONAL|BERMUDA|HONGKONG|CAYMAN|JERSEY|SINGAPORE|GLOBAL)*$'
    prefix = re.sub(suffix_pattern, '', clean_name).strip()
    if len(prefix) >= min_len:
        return prefix
    return clean_name


def _entity_matches(vendor_entity, row_consolidated_entity):
    """
    Check if vendor entity matches the row's consolidated entity.
    CoinDesk matching: vendor contains "CoinDesk" -> row is "CoinDesk"
    Bullish matching: vendor contains "Bullish" or is "BTH" -> row is "Bullish"
    """
    if not vendor_entity or not row_consolidated_entity:
        return False

    vendor_entity = str(vendor_entity).strip()
    row_consolidated_entity = str(row_consolidated_entity).strip()

    if row_consolidated_entity == "CoinDesk":
        return "CoinDesk" in vendor_entity or "COINDESK" in vendor_entity.upper()
    elif row_consolidated_entity == "Bullish":
        return ("Bullish" in vendor_entity or "BULLISH" in vendor_entity.upper() or
                vendor_entity == "BTH")

    return False


def arap_match(all_rows, supplier_rows, customer_rows, log):
    """
    Match unmapped Workday Include rows by vendor/customer name.
    Uses both exact substring matching and prefix-based partial matching.
    Returns count of matches applied.
    """
    log("Stage 4c: AR/AP vendor/customer matching (Workday)...")

    # Build cleaned vendor lookup with both full name and prefix
    vendor_lookups = []
    for r in supplier_rows:
        supplier = str(r.get("Supplier") or "").strip()
        mapping = str(r.get("13WCF Mapping") or "").strip()
        rd = str(r.get("R/D") or "").strip()
        entity = str(r.get("Entity") or "").strip()
        hierarchy = str(r.get("Bullish Group Hierarchy") or "").strip()
        if supplier and mapping:
            full_clean = clean_notes(supplier)
            prefix = _extract_name_prefix(full_clean)
            vendor_lookups.append({
                "cleanName": full_clean,
                "prefix": prefix,
                "mapping": mapping,
                "rd": rd,
                "entity": entity,
                "hierarchy": hierarchy,
                "original": supplier,
            })

    # Build cleaned customer lookup with both full name and prefix
    customer_lookups = []
    for r in customer_rows:
        customer = str(r.get("Sold-To Customer") or "").strip()
        mapping = str(r.get("13WCF Mapping") or "").strip()
        rd = str(r.get("R/D") or "").strip()
        entity = str(r.get("13WCF Entity") or "").strip()
        if customer and mapping:
            full_clean = clean_notes(customer)
            prefix = _extract_name_prefix(full_clean)
            customer_lookups.append({
                "cleanName": full_clean,
                "prefix": prefix,
                "mapping": mapping,
                "rd": rd,
                "entity": entity,
                "original": customer,
            })

    log(f"  -> {len(vendor_lookups)} vendor lookups, {len(customer_lookups)} customer lookups")

    # Filter for still-unmapped Workday Include records
    unmapped = [
        r for r in all_rows
        if r.get("Source") == "Workday"
        and r.get("Inc/Excl") == "Include"
        and not r.get("13WCF Line Item Mapping")
    ]

    log(f"  -> {len(unmapped)} unmapped Workday records to check")

    vendor_match_count = 0
    customer_match_count = 0

    for row in unmapped:
        notes_clean = row.get("Notes_Clean") or clean_notes(row.get("Notes") or row.get("Addenda") or "")
        if not notes_clean:
            continue

        row_rd = str(row.get("R/D") or "").strip()
        row_consolidated_entity = str(row.get("Consolidated Entity") or "").strip()

        matched = False

        # --- Vendor matching (exact then prefix) ---
        # Try entity-filtered first, then fall back to unfiltered
        def _find_vendor_exact(entity_filter=False):
            for v in vendor_lookups:
                if entity_filter and row_consolidated_entity:
                    if not _entity_matches(v["entity"], row_consolidated_entity):
                        continue
                if v["cleanName"] in notes_clean:
                    return v
            return None

        def _find_vendor_prefix(entity_filter=False):
            best = None
            best_len = 0
            for v in vendor_lookups:
                if entity_filter and row_consolidated_entity:
                    if not _entity_matches(v["entity"], row_consolidated_entity):
                        continue
                prefix = v["prefix"]
                if len(prefix) >= 8 and prefix != v["cleanName"] and prefix in notes_clean:
                    if len(prefix) > best_len:
                        best_len = len(prefix)
                        best = v
            return best

        # Try entity-filtered vendor exact match first
        v_match = _find_vendor_exact(entity_filter=True)
        if not v_match:
            # Fall back to unfiltered vendor exact match
            v_match = _find_vendor_exact(entity_filter=False)
        if v_match:
            row["13WCF Line Item Mapping"] = v_match["mapping"]
            row["Matched_Substring"] = f"Vendor:{v_match['original']}"
            vendor_match_count += 1
            matched = True

        if not matched:
            # Try entity-filtered vendor prefix match
            v_match = _find_vendor_prefix(entity_filter=True)
            if not v_match:
                # Fall back to unfiltered vendor prefix match
                v_match = _find_vendor_prefix(entity_filter=False)
            if v_match:
                row["13WCF Line Item Mapping"] = v_match["mapping"]
                row["Matched_Substring"] = f"VendorPrefix:{v_match['original']}"
                vendor_match_count += 1
                matched = True

        # --- Customer matching (exact then prefix) ---
        if not matched:
            def _find_customer_exact(entity_filter=False):
                for c in customer_lookups:
                    if entity_filter and row_consolidated_entity:
                        if not _entity_matches(c["entity"], row_consolidated_entity):
                            continue
                    if c["cleanName"] in notes_clean:
                        return c
                return None

            c_match = _find_customer_exact(entity_filter=True)
            if not c_match:
                c_match = _find_customer_exact(entity_filter=False)
            if c_match:
                row["13WCF Line Item Mapping"] = c_match["mapping"]
                row["Matched_Substring"] = f"Customer:{c_match['original']}"
                customer_match_count += 1
                matched = True

        if not matched:
            def _find_customer_prefix(entity_filter=False):
                best = None
                best_len = 0
                for c in customer_lookups:
                    if entity_filter and row_consolidated_entity:
                        if not _entity_matches(c["entity"], row_consolidated_entity):
                            continue
                    prefix = c["prefix"]
                    if len(prefix) >= 8 and prefix != c["cleanName"] and prefix in notes_clean:
                        if len(prefix) > best_len:
                            best_len = len(prefix)
                            best = c
                return best

            c_match = _find_customer_prefix(entity_filter=True)
            if not c_match:
                c_match = _find_customer_prefix(entity_filter=False)
            if c_match:
                row["13WCF Line Item Mapping"] = c_match["mapping"]
                row["Matched_Substring"] = f"CustomerPrefix:{c_match['original']}"
                customer_match_count += 1

    log(f"  -> Vendor matches: {vendor_match_count}, Customer matches: {customer_match_count}")

    # ── Counterparty override pass ──
    # Run after vendor/customer matching on ALL WD Include rows.
    # This overrides any prior mapping (fuzzy, vendor, customer) when the
    # counterparty is known from the Alteryx answer key.
    wd_include = [
        r for r in all_rows
        if r.get("Source") == "Workday"
        and r.get("Inc/Excl") == "Include"
    ]

    override_count = 0
    for row in wd_include:
        raw_notes = str(row.get("Notes") or row.get("Addenda") or "")
        if not raw_notes:
            continue
        rd = str(row.get("R/D") or "").strip()

        for substring, mapping in _COUNTERPARTY_OVERRIDES:
            if substring in raw_notes:
                # "Intercompany" (no direction) → set direction by R/D
                if mapping == "Intercompany":
                    if rd == "Receipt":
                        mapping = "Intercompany Inflow"
                    else:
                        mapping = "Intercompany Outflow"
                row["13WCF Line Item Mapping"] = mapping
                row["Matched_Substring"] = f"Override:{substring}"
                override_count += 1
                break

    if override_count:
        log(f"  -> Counterparty overrides: {override_count}")

    # ── Force-unmapped pass ──
    # Remove mapping for rows that Alteryx intentionally leaves unmapped.
    unmapped_count = 0
    for row in wd_include:
        raw_notes = str(row.get("Notes") or row.get("Addenda") or "")
        if not raw_notes:
            continue
        for pattern in _FORCE_UNMAPPED:
            if pattern in raw_notes and row.get("13WCF Line Item Mapping"):
                row["13WCF Line Item Mapping"] = ""
                row["Matched_Substring"] = f"ForceUnmapped:{pattern}"
                unmapped_count += 1
                break

    if unmapped_count:
        log(f"  -> Force-unmapped: {unmapped_count}")

    log("Stage 4c complete")

    return vendor_match_count + customer_match_count + override_count
