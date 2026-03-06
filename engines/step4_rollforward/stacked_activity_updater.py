"""
Stacked Activity Updater Module
--------------------------------
Reads the Alteryx_Output tab from an Activity Aggregator file,
filters for non-excluded rows, takes columns A-X, and appends them
into the Stacked Activity tab of the Activity Rollforward file.

Uses streaming XML approach for memory efficiency with large files.
"""

import os
import re
import zipfile
import numpy as np
import pandas as pd
from lxml import etree
from datetime import datetime, date

NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"s": NAMESPACE}

# Style mapping: aggregator col index -> (SA col letter, style_id, is_date)
COL_STYLE_MAP = {
    0: ("B", "68", False),    # Account Name
    1: ("C", "318", True),    # Transaction Date
    2: ("D", "68", False),    # Asset Code
    3: ("E", None, False),    # Notes
    4: ("F", "25", False),    # Net Activity - USD
    5: ("G", "319", False),   # Activity Week
    6: ("H", "320", True),    # Week Ending
    7: ("I", "319", False),   # Actuals Week
    8: ("J", "68", False),    # Consolidated Entity
    9: ("K", "68", False),    # Entity/Enterprise
    10: ("L", "68", False),   # 13WCF Ref #
    11: ("M", "68", False),   # Incl/Excl
    12: ("N", "68", False),   # Provider Name
    13: ("O", "68", False),   # Sub Account Name
    14: ("P", "68", False),   # From Address
    15: ("Q", "68", False),   # To Address
    16: ("R", "68", False),   # Tags
    17: ("S", "68", False),   # House/Custodial
    18: ("T", "68", False),   # Vendor
    19: ("U", "68", False),   # Account Type
    20: ("V", "68", False),   # Source
    21: ("W", "68", False),   # Type
    22: ("X", "68", False),   # Sub Type
    23: ("Y", "68", False),   # 13WCF Line Item Mapping
}


def get_sheet_xml_path(zip_path, sheet_name):
    """Find the XML path for a given sheet name"""
    with zipfile.ZipFile(zip_path, "r") as z:
        wb_xml = etree.parse(z.open("xl/workbook.xml"))
        for s in wb_xml.findall(".//s:sheet", NS):
            if s.get("name") == sheet_name:
                r_id = s.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                break
        else:
            raise ValueError(f"Sheet '{sheet_name}' not found")
        rels_xml = etree.parse(z.open("xl/_rels/workbook.xml.rels"))
        for rel in rels_xml.getroot():
            if rel.get("Id") == r_id:
                return "xl/" + rel.get("Target")
    raise ValueError(f"Could not resolve path for '{sheet_name}'")


def find_last_row_streaming(zip_path, sheet_xml_path):
    """Find the last row in column B using streaming"""
    last_b_row = 1
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(sheet_xml_path) as f:
            for event, elem in etree.iterparse(f, events=("end",), tag=f"{{{NAMESPACE}}}c"):
                ref = elem.get("r", "")
                if ref.startswith("B") and not ref[1:2].isalpha():
                    row_num = int(re.sub(r"[A-Z]+", "", ref))
                    if row_num > last_b_row:
                        last_b_row = row_num
                elem.clear()
    return last_b_row


def get_shared_strings_with_index(zip_path):
    """Extract shared strings with their indices"""
    with zipfile.ZipFile(zip_path, "r") as z:
        if "xl/sharedStrings.xml" not in z.namelist():
            return {}, 0
        idx = 0
        mapping = {}
        with z.open("xl/sharedStrings.xml") as f:
            for event, elem in etree.iterparse(f, events=("end",), tag=f"{{{NAMESPACE}}}si"):
                text = "".join(elem.itertext())
                mapping[text] = idx
                idx += 1
                elem.clear()
    return mapping, idx


def build_new_shared_strings(zip_path, new_strings_set, existing_mapping, next_idx):
    """Build updated shared strings XML"""
    new_mapping = dict(existing_mapping)
    added = []
    for s in sorted(new_strings_set):
        if s not in new_mapping:
            new_mapping[s] = next_idx
            added.append(s)
            next_idx += 1

    if not added:
        return None, new_mapping

    with zipfile.ZipFile(zip_path, "r") as z:
        if "xl/sharedStrings.xml" in z.namelist():
            raw = z.read("xl/sharedStrings.xml")
        else:
            raw = None

    if raw:
        close_tag = b"</sst>"
        pos = raw.rfind(close_tag)
        new_si_parts = []
        for s in added:
            escaped = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            preserve = ' xml:space="preserve"' if (s and (s[0] == " " or s[-1] == " ")) else ""
            new_si_parts.append(f'<si><t{preserve}>{escaped}</t></si>')
        insert_bytes = "".join(new_si_parts).encode("utf-8")
        new_raw = raw[:pos] + insert_bytes + raw[pos:]
        total = len(new_mapping)
        new_raw = re.sub(rb'count="\d+"', f'count="{total}"'.encode(), new_raw, count=1)
        new_raw = re.sub(rb'uniqueCount="\d+"', f'uniqueCount="{total}"'.encode(), new_raw, count=1)
        return new_raw, new_mapping
    else:
        parts = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>']
        total = len(new_mapping)
        parts.append(f'<sst xmlns="{NAMESPACE}" count="{total}" uniqueCount="{total}">')
        for s, _ in sorted(new_mapping.items(), key=lambda x: x[1]):
            escaped = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f"<si><t>{escaped}</t></si>")
        parts.append("</sst>")
        return "".join(parts).encode("utf-8"), new_mapping


def value_to_cell_xml(val, col_letter, row_num, string_mapping, style_id=None, is_date=False):
    """Convert a value to Excel cell XML"""
    ref = f"{col_letter}{row_num}"

    # Handle None / NaN
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass

    style_attr = f' s="{style_id}"' if style_id else ""

    # Date values
    if is_date and isinstance(val, (datetime, date, pd.Timestamp)):
        if isinstance(val, pd.Timestamp):
            val = val.to_pydatetime()
        if isinstance(val, datetime):
            delta = val - datetime(1899, 12, 30)
            serial = delta.days + delta.seconds / 86400
        else:
            delta = val - date(1899, 12, 30)
            serial = delta.days
        return f'<c r="{ref}"{style_attr}><v>{serial}</v></c>'

    # String values
    if isinstance(val, str):
        if val in string_mapping:
            return f'<c r="{ref}"{style_attr} t="s"><v>{string_mapping[val]}</v></c>'
        else:
            escaped = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{escaped}</t></is></c>'

    # Numeric values
    if isinstance(val, (int, float, np.integer, np.floating)):
        return f'<c r="{ref}"{style_attr}><v>{val}</v></c>'

    # Fallback
    escaped = str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{escaped}</t></is></c>'


def build_rows_xml(df, start_row, string_mapping, log=None):
    """Build XML for rows to append"""
    parts = []
    total = len(df)
    for i in range(total):
        row_num = start_row + i
        cells = []
        for j in range(24):
            val = df.iat[i, j]
            col_letter, style_id, is_date = COL_STYLE_MAP[j]
            cell = value_to_cell_xml(val, col_letter, row_num, string_mapping, style_id, is_date)
            if cell:
                cells.append(cell)
        if cells:
            parts.append(f'<row r="{row_num}">{"".join(cells)}</row>')
        if log and (i + 1) % 2000 == 0:
            log.append(f"    Built {i+1:,}/{total:,} rows...")
    if log:
        log.append(f"    Built {total:,}/{total:,} rows")
    return "".join(parts).encode("utf-8")


def update_dimension(xml_bytes):
    """Update the sheet dimension after adding rows"""
    pattern = rb'<dimension ref="[^"]*"'
    match = re.search(pattern, xml_bytes)
    if match:
        row_nums = re.findall(rb'<row r="(\d+)"', xml_bytes[-50000:])
        if row_nums:
            max_row = max(int(r) for r in row_nums)
            new_dim = f'<dimension ref="A1:AD{max_row}"'.encode()
            xml_bytes = xml_bytes[:match.start()] + new_dim + xml_bytes[match.end():]
    return xml_bytes


def stream_append_rows(zip_in_path, sheet_xml_path, new_rows_xml_bytes, output_path):
    """Stream append rows to sheet XML"""
    with zipfile.ZipFile(zip_in_path, "r") as zin:
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.namelist():
                if item == sheet_xml_path:
                    raw = zin.read(item)
                    close_tag = b"</sheetData>"
                    pos = raw.rfind(close_tag)
                    if pos == -1:
                        raise ValueError("Could not find </sheetData>")
                    modified = raw[:pos] + new_rows_xml_bytes + raw[pos:]
                    modified = update_dimension(modified)
                    zout.writestr(item, modified)
                    del raw, modified
                else:
                    zout.writestr(item, zin.read(item))


def append_activity_data(aggregator_path, rollforward_path, output_path, log=None):
    """
    Main function to append activity data from aggregator to rollforward file

    Args:
        aggregator_path: Path to Activity Aggregator Excel file
        rollforward_path: Path to Activity Rollforward Excel file
        output_path: Path for output file
        log: Optional list to append log messages to

    Returns:
        dict with success status and statistics
    """
    if log is None:
        log = []

    try:
        log.append("Reading Activity Aggregator file...")
        df_agg = pd.read_excel(aggregator_path, sheet_name="Alteryx_Output", engine="openpyxl")

        filter_col = "Manual User Check"
        df_filtered = df_agg[df_agg[filter_col] != "Exclude - Pass"].copy()

        log.append(f"  Total rows: {len(df_agg):,}")
        log.append(f"  Filtered rows: {len(df_filtered):,}")
        log.append(f"  Mapped: {(df_agg[filter_col] == 'Mapped').sum():,}")
        log.append(f"  Not Mapped - Check: {(df_agg[filter_col] == 'Not Mapped - Check').sum():,}")

        del df_agg

        df_to_append = df_filtered.iloc[:, :24].reset_index(drop=True)
        del df_filtered

        if len(df_to_append) == 0:
            log.append("⚠ No rows to append (all excluded)")
            return {'success': True, 'rows_appended': 0}

        log.append("Finding insertion point in Stacked Activity...")
        sheet_path = get_sheet_xml_path(rollforward_path, "Stacked Activity")
        last_row = find_last_row_streaming(rollforward_path, sheet_path)
        start_row = last_row + 1
        log.append(f"  Last row in column B: {last_row:,}")
        log.append(f"  Will insert at row: {start_row:,}")

        log.append("Preparing shared strings...")
        existing_map, next_idx = get_shared_strings_with_index(rollforward_path)
        log.append(f"  Existing strings: {len(existing_map):,}")

        all_new_strings = set()
        for col in df_to_append.columns:
            for val in df_to_append[col]:
                if isinstance(val, str):
                    all_new_strings.add(val)

        new_ss_xml, full_mapping = build_new_shared_strings(rollforward_path, all_new_strings, existing_map, next_idx)
        log.append(f"  Total strings after update: {len(full_mapping):,}")
        del existing_map, all_new_strings

        log.append(f"Building row XML for {len(df_to_append):,} rows...")
        rows_xml = build_rows_xml(df_to_append, start_row, full_mapping, log)
        rows_count = len(df_to_append)
        del df_to_append, full_mapping
        log.append(f"  Row XML size: {len(rows_xml) / 1024 / 1024:.1f} MB")

        log.append("Writing output file with appended rows...")
        stream_append_rows(rollforward_path, sheet_path, rows_xml, output_path)
        del rows_xml

        if new_ss_xml is not None:
            log.append("Updating shared strings...")
            temp_path = output_path + ".tmp"
            with zipfile.ZipFile(output_path, "r") as zin:
                with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as zout:
                    for item in zin.namelist():
                        if item == "xl/sharedStrings.xml":
                            zout.writestr(item, new_ss_xml)
                        else:
                            zout.writestr(item, zin.read(item))
            os.replace(temp_path, output_path)
            del new_ss_xml

        log.append(f"✓ Successfully appended {rows_count:,} rows to Stacked Activity tab")

        return {'success': True, 'rows_appended': rows_count}

    except Exception as e:
        log.append(f"✗ ERROR in Stacked Activity update: {str(e)}")
        import traceback
        log.append(traceback.format_exc())
        return {'success': False, 'error': str(e)}
