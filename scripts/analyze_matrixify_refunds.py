"""
Analyze Matrixify Export to map refund data structure.
"""
import pandas as pd
import sys

XLSX = "/workspaces/ML-Audit/imports/Export_2026-02-10_135944.xlsx"

print("=" * 80)
print("STEP 0: Load the Orders sheet")
print("=" * 80)

df = pd.read_excel(XLSX, sheet_name="Orders", engine="openpyxl")
print(f"Shape: {df.shape}")
print(f"Columns ({len(df.columns)}):")
for i, c in enumerate(df.columns):
    print(f"  [{i:3d}] {c}")

# ── Step 1: Find the row-type column ──────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 1: Identify the row-type column")
print("=" * 80)

# Look for columns containing "type" (case-insensitive)
type_cols = [c for c in df.columns if "type" in c.lower()]
print(f"Columns with 'type' in name: {type_cols}")

# The most likely candidate is "Line: Type" or similar
for tc in type_cols:
    print(f"\n  Unique values in '{tc}':")
    vc = df[tc].value_counts(dropna=False)
    for val, cnt in vc.items():
        print(f"    {repr(val):40s} -> {cnt:>6d} rows")

# Determine the row type column
row_type_col = None
for candidate in ["Line: Type", "Row Type", "Type"]:
    if candidate in df.columns:
        row_type_col = candidate
        break
if row_type_col is None and type_cols:
    row_type_col = type_cols[0]

print(f"\nUsing row type column: '{row_type_col}'")

# ── Step 2: REFUND rows ──────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 2: REFUND rows — non-null columns and sample data")
print("=" * 80)

refund_mask = df[row_type_col].astype(str).str.lower().str.strip() == "refund"
refund_df = df[refund_mask]
print(f"Total REFUND rows: {len(refund_df)}")

if len(refund_df) > 0:
    non_null_cols = []
    for c in df.columns:
        non_null_count = refund_df[c].notna().sum()
        non_empty = refund_df[c].apply(lambda x: x != "" and x != " " if isinstance(x, str) else pd.notna(x)).sum()
        if non_empty > 0:
            non_null_cols.append((c, non_empty, len(refund_df)))
    
    print(f"\nColumns with non-null/non-empty values in REFUND rows ({len(non_null_cols)}):")
    for col, cnt, total in non_null_cols:
        print(f"  {col:55s}  {cnt:>5d}/{total}")
    
    print(f"\nSample 5 REFUND rows (non-null columns only):")
    sample_cols = [c for c, cnt, _ in non_null_cols]
    sample = refund_df[sample_cols].head(5)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 250)
    pd.set_option('display.max_colwidth', 60)
    print(sample.to_string())
else:
    print("No rows with exact 'refund' type. Checking for partial matches...")
    for tc in type_cols:
        matches = df[df[tc].astype(str).str.lower().str.contains("refund", na=False)]
        if len(matches) > 0:
            print(f"  Found {len(matches)} rows in '{tc}' containing 'refund'")
            print(f"  Values: {matches[tc].unique()}")

# ── Step 3: REFUND LINE rows ────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 3: REFUND LINE rows — non-null columns and sample data")
print("=" * 80)

refund_line_mask = df[row_type_col].astype(str).str.lower().str.strip() == "refund line"
refund_line_df = df[refund_line_mask]
print(f"Total REFUND LINE rows: {len(refund_line_df)}")

if len(refund_line_df) > 0:
    non_null_cols_rl = []
    for c in df.columns:
        non_empty = refund_line_df[c].apply(lambda x: x != "" and x != " " if isinstance(x, str) else pd.notna(x)).sum()
        if non_empty > 0:
            non_null_cols_rl.append((c, non_empty, len(refund_line_df)))
    
    print(f"\nColumns with non-null values in REFUND LINE rows ({len(non_null_cols_rl)}):")
    for col, cnt, total in non_null_cols_rl:
        print(f"  {col:55s}  {cnt:>5d}/{total}")
    
    print(f"\nSample 5 REFUND LINE rows (non-null columns only):")
    sample_cols_rl = [c for c, cnt, _ in non_null_cols_rl]
    sample_rl = refund_line_df[sample_cols_rl].head(5)
    print(sample_rl.to_string())
else:
    print("No 'Refund Line' rows found. Checking variations...")
    all_types = df[row_type_col].unique()
    refund_types = [t for t in all_types if isinstance(t, str) and "refund" in t.lower()]
    print(f"  All row types containing 'refund': {refund_types}")

# ── Step 4: Order with both line items and refunds ──────────────────────────
print("\n" + "=" * 80)
print("STEP 4: Relationship — order with both line items and refunds")
print("=" * 80)

# Find order ID column
id_cols = [c for c in df.columns if "id" in c.lower() or "name" in c.lower() or "number" in c.lower()]
print(f"Potential order ID columns: {id_cols[:15]}")

# Use "Name" or "ID" column
order_id_col = None
for candidate in ["Name", "ID", "Order Name", "Order ID"]:
    if candidate in df.columns:
        order_id_col = candidate
        break
if order_id_col is None:
    order_id_col = id_cols[0] if id_cols else df.columns[0]

print(f"Using order ID column: '{order_id_col}'")

# Find orders that have refund-type rows
all_types = df[row_type_col].astype(str).str.lower().str.strip().unique()
print(f"\nAll row types: {sorted(all_types)}")

refund_related_types = [t for t in all_types if "refund" in str(t).lower()]
line_types = [t for t in all_types if "line" in str(t).lower() and "refund" not in str(t).lower()]
print(f"Refund-related types: {refund_related_types}")
print(f"Line item types: {line_types}")

# Find an order that has both a line item row and a refund row
if refund_related_types:
    refund_orders = df[df[row_type_col].astype(str).str.lower().str.strip().isin(refund_related_types)][order_id_col].unique()
    print(f"\nOrders with refund rows: {len(refund_orders)} orders")
    
    if line_types:
        line_orders = df[df[row_type_col].astype(str).str.lower().str.strip().isin(line_types)][order_id_col].unique()
    else:
        line_orders = refund_orders
    
    both = set(refund_orders) & set(line_orders)
    if not both:
        both = set(refund_orders)
    
    if both:
        sample_order = sorted([x for x in both if pd.notna(x)])[0]
        print(f"\nSample order with both types: {sample_order}")
        order_rows = df[df[order_id_col] == sample_order]
        print(f"Total rows for this order: {len(order_rows)}")
        
        non_null_order_cols = [c for c in df.columns if order_rows[c].notna().any()]
        print(f"\nAll rows for order {sample_order}:")
        print(order_rows[non_null_order_cols].to_string())

# ── Step 5: Zip vendor orders with refunds ───────────────────────────────────
print("\n" + "=" * 80)
print("STEP 5: Zip vendor — orders with refunds")
print("=" * 80)

vendor_cols = [c for c in df.columns if "vendor" in c.lower()]
print(f"Vendor columns: {vendor_cols}")

zip_mask = pd.Series(False, index=df.index)
for vc in vendor_cols:
    zip_mask = zip_mask | (df[vc].astype(str).str.lower().str.strip() == "zip")

zip_df = df[zip_mask]
print(f"Rows with Zip vendor: {len(zip_df)}")

if len(zip_df) > 0:
    zip_order_ids = zip_df[order_id_col].unique()
    zip_refund_orders = []
    for oid in zip_order_ids:
        order_rows = df[df[order_id_col] == oid]
        types_in_order = order_rows[row_type_col].astype(str).str.lower().str.strip().unique()
        if any("refund" in t for t in types_in_order):
            zip_refund_orders.append(oid)
    
    print(f"Zip orders with refunds: {len(zip_refund_orders)}")
    
    for oid in zip_refund_orders[:3]:
        print(f"\n{'─' * 70}")
        print(f"ORDER: {oid}")
        order_rows = df[df[order_id_col] == oid]
        non_null_cols_o = [c for c in df.columns if order_rows[c].notna().any()]
        print(order_rows[non_null_cols_o].to_string())
        
        amount_cols = [c for c in non_null_cols_o if any(kw in c.lower() for kw in ["amount", "total", "price", "refund", "money"])]
        if amount_cols:
            print(f"\n  Amount/money columns for this order:")
            print(order_rows[[row_type_col] + amount_cols].to_string())
else:
    print("No Zip vendor rows found. Showing vendor distribution:")
    for vc in vendor_cols:
        print(f"\n  Top vendors in '{vc}':")
        print(df[vc].value_counts().head(20).to_string())

# ── Step 6: Columns containing "refund" or "return" ─────────────────────────
print("\n" + "=" * 80)
print("STEP 6: ALL columns containing 'refund' or 'return' (case-insensitive)")
print("=" * 80)

refund_return_cols = [c for c in df.columns if "refund" in c.lower() or "return" in c.lower()]
print(f"Found {len(refund_return_cols)} columns:")
for c in refund_return_cols:
    non_null = df[c].notna().sum()
    print(f"  {c:60s}  non-null: {non_null:>6d}/{len(df)}")
    sample_vals = df[c].dropna().unique()[:5]
    print(f"    Sample values: {list(sample_vals)}")

# ── Step 7: Transaction rows — refund vs sale ───────────────────────────────
print("\n" + "=" * 80)
print("STEP 7: Transaction rows — refund transaction vs sale transaction")
print("=" * 80)

txn_mask = df[row_type_col].astype(str).str.lower().str.strip() == "transaction"
txn_df = df[txn_mask]
print(f"Total Transaction rows: {len(txn_df)}")

if len(txn_df) > 0:
    non_null_txn_cols = []
    for c in df.columns:
        non_empty = txn_df[c].apply(lambda x: x != "" and x != " " if isinstance(x, str) else pd.notna(x)).sum()
        if non_empty > 0:
            non_null_txn_cols.append((c, non_empty))
    
    print(f"\nColumns with data in Transaction rows ({len(non_null_txn_cols)}):")
    for col, cnt in non_null_txn_cols:
        print(f"  {col:55s}  {cnt:>5d}/{len(txn_df)}")
    
    txn_type_cols = [c for c in df.columns if any(kw in c.lower() for kw in ["kind", "gateway", "status", "transaction"])]
    print(f"\nTransaction-related columns: {txn_type_cols}")
    
    for tc in txn_type_cols:
        if txn_df[tc].notna().sum() > 0:
            print(f"\n  Unique values in '{tc}' for transactions:")
            print(f"  {txn_df[tc].value_counts().to_string()}")
    
    kind_col = None
    for candidate in txn_type_cols:
        vals = txn_df[candidate].astype(str).str.lower().unique()
        if "refund" in vals or "sale" in vals:
            kind_col = candidate
            break
    
    if kind_col is None:
        kind_col = txn_type_cols[0] if txn_type_cols else None
    
    if kind_col:
        print(f"\nUsing '{kind_col}' to differentiate transaction types")
        
        refund_txn = txn_df[txn_df[kind_col].astype(str).str.lower().str.contains("refund", na=False)]
        if len(refund_txn) > 0:
            print(f"\n--- REFUND Transaction (sample, {len(refund_txn)} total) ---")
            sample_cols_rt = [c for c, _ in non_null_txn_cols]
            print(refund_txn[sample_cols_rt].head(3).to_string())
        
        sale_txn = txn_df[txn_df[kind_col].astype(str).str.lower().str.contains("sale", na=False)]
        if len(sale_txn) > 0:
            print(f"\n--- SALE Transaction (sample, {len(sale_txn)} total) ---")
            sample_cols_st = [c for c, _ in non_null_txn_cols]
            print(sale_txn[sample_cols_st].head(3).to_string())
        
        print(f"\nAll transaction kinds/types:")
        print(txn_df[kind_col].value_counts().to_string())
    else:
        print("\nShowing sample transaction rows:")
        sample_cols_t = [c for c, _ in non_null_txn_cols]
        print(txn_df[sample_cols_t].head(5).to_string())
else:
    print("No 'Transaction' rows found.")
    all_types_lower = [str(t).lower().strip() for t in df[row_type_col].unique()]
    txn_like = [t for t in all_types_lower if "trans" in t or "payment" in t]
    print(f"Transaction-like types: {txn_like}")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
