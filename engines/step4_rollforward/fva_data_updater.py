"""
FvA Data Tab Updater Module
----------------------------
Updates FvA Data Tabs (1-Week, 4-Week, 13-Week) in the Activity Rollforward file
by reading the "13WCF - Consol" sheet from 13WCF input files.

Uses streaming XML approach for memory efficiency with large files.
"""

import os
import re
import zipfile
import traceback
import numpy as np
import pandas as pd
from lxml import etree
from datetime import datetime, date

NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

TAB_MAP = {
    "1week":  "1-Week FvA Data Tab",
    "4week":  "4-Week FvA Data Tab",
    "13week": "13-Week FvA Data Tab",
}

SOURCE_SHEET = "13WCF - Consol"


def fva_get_sheet_xml_paths(zip_path, sheet_names):
    """Return {sheet_name: xml_path} for requested sheets."""
    result = {}
    with zipfile.ZipFile(zip_path, "r") as z:
        wb_xml = etree.parse(z.open("xl/workbook.xml"))
        rels_xml = etree.parse(z.open("xl/_rels/workbook.xml.rels"))
        rel_map = {r.get("Id"): r.get("Target") for r in rels_xml.getroot()}
        for s in wb_xml.findall(f".//{{{NAMESPACE}}}sheet"):
            name = s.get("name")
            if name in sheet_names:
                r_id = s.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                target = rel_map[r_id]
                # Handle both absolute (/xl/worksheets/...) and relative (worksheets/...) paths
                if target.startswith("/"):
                    result[name] = target.lstrip("/")
                elif target.startswith("xl/"):
                    result[name] = target
                else:
                    result[name] = "xl/" + target
    return result


def fva_capture_sheet_styles(zip_path, sheet_xml_path):
    """Parse sheet XML and return style_map, row_meta, prefix, suffix."""
    style_map = {}
    row_meta = {}

    with zipfile.ZipFile(zip_path, "r") as z:
        raw = z.read(sheet_xml_path)

    sd_open = raw.find(b"<sheetData")
    sd_close = raw.find(b"</sheetData>") + len(b"</sheetData>")
    prefix = raw[:sd_open]
    suffix = raw[sd_close:]

    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(sheet_xml_path) as f:
            for event, elem in etree.iterparse(f, events=("end",), tag=f"{{{NAMESPACE}}}row"):
                r = elem.get("r")
                row_attrs = {}
                for k, v in elem.attrib.items():
                    if k != "r" and not k.startswith("{"):
                        row_attrs[k] = v
                row_meta[r] = row_attrs
                for cell in elem:
                    ref = cell.get("r", "")
                    s = cell.get("s", "")
                    if s:
                        style_map[ref] = s
                elem.clear()

    # Extract namespace-prefixed row attributes from raw XML
    sd_section = raw[sd_open:sd_close]
    row_ns_attrs = {}
    for match in re.finditer(rb'<row r="(\d+)"([^>]*?)>', sd_section):
        row_num = match.group(1).decode()
        attr_str = match.group(2).decode()
        ns_parts = re.findall(r'(\w+:\w+="[^"]*")', attr_str)
        if ns_parts:
            row_ns_attrs[row_num] = " ".join(ns_parts)

    for r, ns_str in row_ns_attrs.items():
        if r not in row_meta:
            row_meta[r] = {}
        row_meta[r]["_ns_attrs"] = ns_str

    del raw
    return style_map, row_meta, prefix, suffix


def fva_get_shared_strings(zip_path):
    """Load shared strings table."""
    with zipfile.ZipFile(zip_path, "r") as z:
        if "xl/sharedStrings.xml" not in z.namelist():
            return [], {}
        strings = []
        with z.open("xl/sharedStrings.xml") as f:
            for _, elem in etree.iterparse(f, events=("end",), tag=f"{{{NAMESPACE}}}si"):
                strings.append("".join(elem.itertext()))
                elem.clear()
    return strings, {s: i for i, s in enumerate(strings)}


def fva_add_to_shared_strings(strings_list, string_index, new_strings):
    """Add new strings to the shared strings table."""
    for s in new_strings:
        if s not in string_index:
            string_index[s] = len(strings_list)
            strings_list.append(s)


def fva_rebuild_shared_strings_xml(zip_path, strings_list):
    """Rebuild xl/sharedStrings.xml from the full list."""
    with zipfile.ZipFile(zip_path, "r") as z:
        if "xl/sharedStrings.xml" in z.namelist():
            raw = z.read("xl/sharedStrings.xml")
        else:
            raw = None

    if raw:
        close_tag = b"</sst>"
        pos_close = raw.rfind(close_tag)
        pos_open = raw.find(b">", raw.find(b"<sst")) + 1

        si_parts = []
        for s in strings_list:
            escaped = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            preserve = ' xml:space="preserve"' if (s and (s[0] == " " or s[-1] == " ")) else ""
            si_parts.append(f'<si><t{preserve}>{escaped}</t></si>')

        total = len(strings_list)
        header = raw[:pos_open].decode("utf-8")
        header = re.sub(r'count="\d+"', f'count="{total}"', header)
        header = re.sub(r'uniqueCount="\d+"', f'uniqueCount="{total}"', header)

        return (header + "".join(si_parts) + "</sst>").encode("utf-8")
    else:
        parts = [f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                 f'<sst xmlns="{NAMESPACE}" count="{len(strings_list)}" uniqueCount="{len(strings_list)}">']
        for s in strings_list:
            escaped = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f"<si><t>{escaped}</t></si>")
        parts.append("</sst>")
        return "".join(parts).encode("utf-8")


def fva_col_letter(col_num):
    """1-indexed column number to Excel column letter."""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def fva_cell_xml(ref, val, style_id, string_index):
    """Build XML string for a single cell."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass

    s_attr = f' s="{style_id}"' if style_id else ""

    if isinstance(val, (datetime, date, pd.Timestamp)):
        if isinstance(val, pd.Timestamp):
            val = val.to_pydatetime()
        if isinstance(val, datetime):
            delta = val - datetime(1899, 12, 30)
            serial = delta.days + delta.seconds / 86400
        else:
            delta = val - date(1899, 12, 30)
            serial = delta.days
        return f'<c r="{ref}"{s_attr}><v>{serial}</v></c>'

    if isinstance(val, str):
        if val in string_index:
            return f'<c r="{ref}"{s_attr} t="s"><v>{string_index[val]}</v></c>'
        escaped = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f'<c r="{ref}"{s_attr} t="str"><v>{escaped}</v></c>'

    if isinstance(val, (int, float, np.integer, np.floating)):
        return f'<c r="{ref}"{s_attr}><v>{val}</v></c>'

    escaped = str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<c r="{ref}"{s_attr} t="str"><v>{escaped}</v></c>'


def fva_build_sheet_data(df, style_map, row_meta, string_index):
    """Build <sheetData>...</sheetData> XML bytes from DataFrame + style map."""
    parts = ["<sheetData>"]
    n_rows, n_cols = df.shape

    for i in range(n_rows):
        excel_row = i + 1
        row_str = str(excel_row)

        r_attrs = row_meta.get(row_str, {})
        regular_attrs = "".join(f' {k}="{v}"' for k, v in r_attrs.items() if k != "_ns_attrs")
        ns_attrs = r_attrs.get("_ns_attrs", "")
        attr_str = regular_attrs + (" " + ns_attrs if ns_attrs else "")

        cells = []
        for j in range(n_cols):
            cl = fva_col_letter(j + 1)
            ref = f"{cl}{excel_row}"
            val = df.iat[i, j]
            sid = style_map.get(ref, "")
            c = fva_cell_xml(ref, val, sid, string_index)
            if c:
                cells.append(c)
            elif sid:
                cells.append(f'<c r="{ref}" s="{sid}"/>')

        if cells:
            parts.append(f'<row r="{row_str}"{attr_str}>{"".join(cells)}</row>')

    parts.append("</sheetData>")
    return "".join(parts).encode("utf-8")


def update_fva_tabs(rollforward_path, input_files, output_path, log=None):
    """
    Update FvA Data Tabs in the Activity Rollforward file.

    Args:
        rollforward_path: Path to Activity Rollforward Excel file
        input_files: dict like {"1week": "path.xlsx", "4week": "path.xlsx", "13week": "path.xlsx"}
        output_path: Path for output file
        log: Optional list to append log messages to

    Returns:
        dict with success status and statistics
    """
    if log is None:
        log = []

    if not input_files:
        log.append("No FvA input files specified. Skipping FvA update.")
        return {'success': True, 'tabs_updated': 0}

    try:
        tabs_to_update = {}
        for key, filepath in input_files.items():
            tab_name = TAB_MAP[key]
            log.append(f"Reading '{SOURCE_SHEET}' from {key}: {os.path.basename(filepath)}")
            df = pd.read_excel(filepath, sheet_name=SOURCE_SHEET, header=None, engine="openpyxl")
            log.append(f"  Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
            tabs_to_update[tab_name] = df

        sheet_paths = fva_get_sheet_xml_paths(rollforward_path, set(tabs_to_update.keys()))
        log.append(f"Sheet XML paths resolved")

        log.append("Capturing existing styles...")
        sheet_info = {}
        for tab_name in tabs_to_update:
            xml_path = sheet_paths[tab_name]
            style_map, row_meta, prefix, suffix = fva_capture_sheet_styles(rollforward_path, xml_path)
            sheet_info[tab_name] = (xml_path, style_map, row_meta, prefix, suffix)
            log.append(f"  {tab_name}: {len(style_map):,} styled cells, {len(row_meta):,} rows")

        log.append("Loading shared strings...")
        ss_list, ss_index = fva_get_shared_strings(rollforward_path)
        log.append(f"  Existing: {len(ss_list):,} strings")

        for tab_name, df in tabs_to_update.items():
            new_strings = set()
            for col in df.columns:
                for val in df[col]:
                    if isinstance(val, str):
                        new_strings.add(val)
            fva_add_to_shared_strings(ss_list, ss_index, new_strings)
        log.append(f"  After adding new: {len(ss_list):,} strings")

        log.append("Building replacement sheet XML...")
        replacements = {}
        for tab_name, df in tabs_to_update.items():
            xml_path, style_map, row_meta, prefix, suffix = sheet_info[tab_name]
            sheet_data = fva_build_sheet_data(df, style_map, row_meta, ss_index)
            full_xml = prefix + sheet_data + suffix
            replacements[xml_path] = full_xml
            log.append(f"  {tab_name}: {len(full_xml) / 1024:.0f} KB")

        new_ss_xml = fva_rebuild_shared_strings_xml(rollforward_path, ss_list)

        # When input and output are the same file, use a temp file to avoid corruption
        same_file = os.path.abspath(rollforward_path) == os.path.abspath(output_path)
        write_target = output_path + ".fva_tmp" if same_file else output_path

        log.append(f"Writing output: {os.path.basename(output_path)}")
        with zipfile.ZipFile(rollforward_path, "r") as zin:
            has_ss = "xl/sharedStrings.xml" in zin.namelist()
            with zipfile.ZipFile(write_target, "w", zipfile.ZIP_DEFLATED) as zout:
                for item in zin.namelist():
                    if item in replacements:
                        log.append(f"  Replacing: {item}")
                        zout.writestr(item, replacements[item])
                    elif item == "xl/sharedStrings.xml":
                        zout.writestr(item, new_ss_xml)
                    elif item == "[Content_Types].xml" and not has_ss:
                        # Register sharedStrings part in Content_Types if it's new
                        ct_raw = zin.read(item)
                        ct_str = ct_raw.decode("utf-8")
                        if "sharedStrings.xml" not in ct_str:
                            insert = '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
                            ct_str = ct_str.replace("</Types>", insert + "</Types>")
                        zout.writestr(item, ct_str.encode("utf-8"))
                    elif item == "xl/_rels/workbook.xml.rels" and not has_ss:
                        # Add sharedStrings relationship if it's new
                        rels_raw = zin.read(item)
                        rels_str = rels_raw.decode("utf-8")
                        if "sharedStrings" not in rels_str:
                            existing_ids = [int(m) for m in re.findall(r'Id="rId(\d+)"', rels_str)]
                            new_id = max(existing_ids) + 1 if existing_ids else 1
                            rel_entry = f'<Relationship Id="rId{new_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
                            rels_str = rels_str.replace("</Relationships>", rel_entry + "</Relationships>")
                        zout.writestr(item, rels_str.encode("utf-8"))
                    else:
                        zout.writestr(item, zin.read(item))
                # If sharedStrings.xml didn't exist in original, add it now
                if not has_ss:
                    log.append("  Adding new xl/sharedStrings.xml (not in original)")
                    zout.writestr("xl/sharedStrings.xml", new_ss_xml)

        # If we used a temp file, replace the original
        if same_file:
            os.replace(write_target, output_path)

        log.append(f"✓ FvA Data Tabs updated successfully")
        for key in input_files:
            log.append(f"  Updated: {TAB_MAP[key]}")

        return {'success': True, 'tabs_updated': len(tabs_to_update)}

    except Exception as e:
        log.append(f"✗ ERROR in FvA update: {str(e)}")
        log.append(traceback.format_exc())
        return {'success': False, 'error': str(e)}
