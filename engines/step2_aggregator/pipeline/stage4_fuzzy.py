"""
Stage 4a: Fuzzy Match vs Historicals (Workday only)
Token-based Levenshtein matching against previous week's Alteryx_Output.
"""
import time
import Levenshtein
from pipeline.helpers import clean_notes_underscore

# Thresholds
TOKEN_THRESHOLD = 0.80
OVERALL_THRESHOLD = 0.65
MAX_CANDIDATES_PER_ACCOUNT = 150
TIME_BUDGET_MS = 200

# Common corporate suffix tokens that should not be the only match basis.
# If ALL matched tokens are in this set, the match is rejected as a false positive.
COMMON_SUFFIXES = {
    "INC", "LLC", "LLP", "LTD", "PTE", "CORP", "GMBH", "LIMITED",
    "COMPANY", "AG", "SA", "SG", "HK", "UK", "US", "EU", "THE",
    "OF", "AND", "DBA", "RE", "CO",
}


def _token_similarity(a, b):
    """Compute similarity between two tokens using Levenshtein distance."""
    # Cap token length
    if len(a) > 40:
        a = a[:40]
    if len(b) > 40:
        b = b[:40]
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 1.0
    # Quick reject: if lengths differ by more than 40%
    if abs(len(a) - len(b)) / max_len > 0.4:
        return 0.0
    dist = Levenshtein.distance(a, b)
    return 1.0 - dist / max_len


def fuzzy_score(clean_a, clean_b):
    """
    Compare two underscore-delimited cleaned note strings.
    Returns similarity score 0-1.
    Filters out matches where all matched tokens are common corporate suffixes.
    """
    tokens_a = [t for t in clean_a.split("_") if t]
    tokens_b = [t for t in clean_b.split("_") if t]
    if not tokens_a or not tokens_b:
        return 0.0

    # Cap token count
    cap_a = tokens_a[:30]
    cap_b = tokens_b[:30]

    matched = 0
    used_b = set()
    matched_tokens = []

    for ta in cap_a:
        best_sim = 0.0
        best_idx = -1
        best_token = None
        for j, tb in enumerate(cap_b):
            if j in used_b:
                continue
            # Quick exact match
            if ta == tb:
                best_sim = 1.0
                best_idx = j
                best_token = tb
                break
            sim = _token_similarity(ta, tb)
            if sim > best_sim:
                best_sim = sim
                best_idx = j
                best_token = tb
        if best_sim >= TOKEN_THRESHOLD and best_idx >= 0:
            matched += 1
            used_b.add(best_idx)
            matched_tokens.append(best_token)

    # If all matched tokens are common corporate suffixes, reject the match
    # to prevent false positives (e.g., "SG PTE LTD" matching different companies).
    if matched > 0 and matched_tokens:
        if all(t.upper() in COMMON_SUFFIXES for t in matched_tokens):
            return 0.0

    return matched / len(cap_a)


def find_best_fuzzy_match(needle_clean, candidates):
    """
    Find the best fuzzy match for needle among candidates.
    candidates: list of {"cleanNotes": str, "mapping": str}
    Returns {"match": candidate, "score": float} or None.
    """
    best_score = 0.0
    best_match = None
    deadline = time.time() + TIME_BUDGET_MS / 1000.0

    for i, cand in enumerate(candidates):
        # Quick exact match
        if needle_clean == cand["cleanNotes"]:
            return {"match": cand, "score": 1.0}

        score = fuzzy_score(needle_clean, cand["cleanNotes"])
        if score > best_score:
            best_score = score
            best_match = cand
            if best_score >= 0.95:
                break

        # Check time budget every 20 candidates
        if i % 20 == 19 and time.time() > deadline:
            break

    if best_score >= OVERALL_THRESHOLD:
        return {"match": best_match, "score": best_score}
    return None


def fuzzy_match_workday(all_rows, historicals, log, on_progress=None):
    """
    Fuzzy match unmapped Workday Include records against historicals.
    Returns count of matches applied.
    """
    log("Stage 4a: Fuzzy matching Workday records vs historicals...")

    try:
        # Filter for unmapped Workday Include records
        unmapped = [
            r for r in all_rows
            if r.get("Source") == "Workday"
            and (r.get("Inc/Excl") == "Include" or r.get("Incl/Excl") == "Include")
            and not r.get("13WCF Line Item Mapping")
        ]

        if not unmapped:
            log("  -> No unmapped Workday records to fuzzy match")
            return 0

        log(f"  -> {len(unmapped)} unmapped Workday records to process")

        if not historicals:
            log("  -> No historical records available for fuzzy matching")
            return 0

        log(f"  -> {len(historicals)} historical Workday records available")

        # Index historicals by Account Name, deduplicated by cleanNotes
        hist_by_account = {}
        total_candidates = 0
        for h in historicals:
            acct = str(h.get("Account Name") or "").strip()
            raw_notes = str(h.get("Notes") or "").strip()
            mapping = str(h.get("13WCF Line Item Mapping") or "").strip()
            if not acct or not raw_notes or not mapping:
                continue

            cleaned_notes = clean_notes_underscore(raw_notes)
            if not cleaned_notes or len(cleaned_notes.replace("_", "")) < 3:
                continue

            if acct not in hist_by_account:
                hist_by_account[acct] = {}
            bucket = hist_by_account[acct]
            if cleaned_notes not in bucket:
                bucket[cleaned_notes] = mapping
                total_candidates += 1

        # Convert to candidate arrays, capped per account
        hist_candidates = {}
        for acct, note_map in hist_by_account.items():
            candidates = []
            for clean_notes, mapping in note_map.items():
                candidates.append({"cleanNotes": clean_notes, "mapping": mapping})
                if len(candidates) >= MAX_CANDIDATES_PER_ACCOUNT:
                    break
            hist_candidates[acct] = candidates

        log(f"  -> Indexed {len(hist_candidates)} bank accounts, {total_candidates} unique candidates")

        match_count = 0

        for i, row in enumerate(unmapped):
            acct = str(row.get("Account Name") or row.get("Bank Account") or "").strip()
            raw_notes = str(row.get("Notes") or row.get("Addenda") or "").strip()

            if not raw_notes or not acct:
                if on_progress:
                    on_progress(i + 1, len(unmapped))
                continue

            notes_clean = clean_notes_underscore(raw_notes)
            if not notes_clean or len(notes_clean.replace("_", "")) < 3:
                if on_progress:
                    on_progress(i + 1, len(unmapped))
                continue

            candidates = hist_candidates.get(acct)
            if not candidates:
                if on_progress:
                    on_progress(i + 1, len(unmapped))
                continue

            result = find_best_fuzzy_match(notes_clean, candidates)
            if result:
                mapping = result["match"]["mapping"]

                # Fix intercompany direction based on sign
                if "Intercompany" in mapping:
                    rd = row.get("R/D")
                    if rd == "Receipt":
                        mapping = "Intercompany Inflow"
                    elif rd == "Disbursement":
                        mapping = "Intercompany Outflow"

                row["13WCF Line Item Mapping"] = mapping
                row["Matched_Substring"] = f"Fuzzy({round(result['score'] * 100)}%)"
                match_count += 1

            if on_progress:
                on_progress(i + 1, len(unmapped))

        log(f"  -> Fuzzy matched: {match_count} of {len(unmapped)} unmapped Workday records")
        log("Stage 4a complete")
        return match_count

    except Exception as e:
        log(f"  Warning: Fuzzy matching failed: {e}")
        log("  -> Continuing pipeline without fuzzy matches")
        return 0
