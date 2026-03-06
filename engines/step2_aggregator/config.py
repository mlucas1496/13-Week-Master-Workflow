"""
Column definitions and constants for Activity Aggregator.
"""

# Stablecoin set (used for counter-asset detection and 1:1 USD equivalence)
STABLECOINS = {"USDC", "USDT", "USDSKY", "USD"}

# 63 columns for Alteryx_Output (exact order from reference file)
ALTERYX_COLS = [
    "Account Name", "Transaction Date", "Asset Code", "Notes", "Net Activity - USD",
    "Activity Week", "Week Ending", "Actuals Week", "Consolidated Entity", "Entity/Enterprise",
    "13WCF Ref #", "Incl/Excl", "Provider Name", "Sub Account Name", "From Address",
    "To Address", "Tags", "House/Custodial", "Vendor", "Account Type",
    "Source", "Type", "Sub Type", "13WCF Line Item Mapping", "Notes_Clean",
    "UID", "Matched_Substring", "R/D", "Manual User Check", "Counter_Asset_Exists",
    "Counter Asset Base Code",
    "F25", "Right_F25", "Repayment Asset", "user app", "Loan ID",
    "Est. Total BPS", "Origination: Tx ID",
    "Final Interest Mode (Calc) (from Approved Loans)",
    "Final Principal Mode (Calc) (from Approved Loans)",
    "F26", "F27", "F28", "F29",
    "Right_F26", "Right_F27", "Right_F28", "Right_F29",
    "Interest Type", "Loan Type", "Submission Date", "Trigger Date_Format",
    "Asset Count", "Implied Price", "Calculated Interest Income",
    "Interest Income Rate (BPS) Calculated*", "Actual Interest Income",
    "Principal Outstanding",
    "Final Principal Date (Calc) (from Approved Loans)",
    "Final Interest Date (Calc) (from Approved Loans)",
    "Final Principal TX ID (Calc) (from Approved Loans)",
    "Final Interest TX ID (Calc) (from Approved Loans) 2",
    "Origination: Tx Date",
]

# BTH Trade Log -- 52 columns matching reference Row 9 headers exactly
BTH_COLS = [
    "Entity Name", "Provider Name", "Account Number", "Account Name", "Sub Account Name",
    "Transaction ID", "Blockchain Transaction ID", "From Address", "To Address", "Order ID",
    "Transaction Date", "Type", "Sub Type", "Cr/Dr", "Base Asset Code",
    "Base Asset Name", "Base Asset Amount", "Price", "Counter Asset Code", "Counter Asset Name",
    "Counter Asset Amount", "Fee Asset Code", "Fee Asset Name", "Fee Asset Amount",
    "Rebate Asset Code", "Rebate Asset Name", "Rebate Amount", "Reference Currency",
    "Base Asset Value", "Counter Asset Value", "Fee Asset Value", "Rebate Asset Value",
    "Fiat Cash Impact", "Source", "Process", "Counterparty", "Tags", "Notes",
    "Enterprise", "House/Custodial", "Vendor", "Account Type", "Ref ID", "Ref",
    "13WCFIncl/Excl", "Consolidated Entity", "Activity Week", "Actuals Week",
    "Week Ending", "Counter_Asset_Exists", "Counter Asset Base Code", "New Wallet Check",
]

# BTH Row 8 -- partial header (cols 0-37 same as BTH_COLS, plus special overrides)
BTH_ROW8_SPECIAL = {43: "Ref", 44: "Include Excl", 49: "Counter Asset"}

# BTH data key -> output column key mapping for "Source" field
BTH_KEY_MAP = {"Source": "Source_CSV"}

# WD Bank Statement Lines -- 26 columns at offset 1 (col A is blank)
WD_COLS = [
    "Bank Statement Line", "Bank Account", "Bank Statement", "Statement Line Date",
    "Type Code", "Statement Line Amount", "Debit/Credit", "Currency",
    "Reference Number", "Addenda", "Reconciliation Status", "Auto Reconciled by Rule",
    "Last Updated Date Time",
    "Net Activity - Native", "FX", "Net Activity - USD",
    "Activity Week", "Week Ending", "Actuals Week",
    "Consolidated Entity", "Entity", "13WCF Ref #",
    "UID", "Inc/Excl", "Rec/Disb", "Bank Acct Flag",
]

# Columns from the All Transactions CSV that stage2b actually needs
BTH_NEEDED_COLS = {
    "Entity Name", "Provider Name", "Account Number", "Account Name", "Sub Account Name",
    "Transaction ID", "Blockchain Transaction ID", "From Address", "To Address", "Order ID",
    "Transaction Date", "Type", "Sub Type", "Cr/Dr",
    "Base Asset Code", "Base Asset Name", "Base Asset Amount", "Price",
    "Counter Asset Code", "Counter Asset Name", "Counter Asset Amount",
    "Fee Asset Code", "Fee Asset Name", "Fee Asset Amount",
    "Rebate Asset Code", "Rebate Asset Name", "Rebate Amount",
    "Reference Currency", "Base Asset Value", "Counter Asset Value", "Fee Asset Value", "Rebate Asset Value",
    "Fiat Cash Impact", "Source", "Process", "Counterparty", "Tags", "Notes",
}

# Sheets to read from previous week's file
PREV_WEEK_SHEETS = [
    "Alteryx_Output",
    "Wallet Mapping",
    "Lukka Ref Mapping",
    "Legal Entity to Enterprise",
    "WD Bank acct -> Ledger acct Map",
    "Calendar",
    "Calendar Mapping",
    "FX",
    "Mapping - Workday Accounts",
]

# Copy-forward sheet names for output
COPY_SHEET_NAMES = [
    "Update Instructions", "Alteryx Column Agg Mapping",
    "Alteryx Output ->", "Alteryx Inputs ->",
    "Lukka Mapping ->", "WD Mapping ->",
    "Wallet Mapping", "Lukka Ref Mapping", "Legal Entity to Enterprise",
    "WD Bank acct -> Ledger acct Map", "Calendar", "Calendar Mapping",
    "FX", "Mapping - Workday Accounts",
]

# Currencies for Bank of Canada FX
CURRENCIES = ["EUR", "GBP", "SGD", "HKD", "CAD"]
BOC_SERIES = {
    "EUR": "FXEURCAD",
    "GBP": "FXGBPCAD",
    "SGD": "FXSGDCAD",
    "HKD": "FXHKDCAD",
    "CAD": "FXUSDCAD",
}
