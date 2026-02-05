#!/usr/bin/env python3
"""
Data Import Script

Imports data files from the /imports folder into the database.

Supported file types:
- Caprice pricing logs (*.xlsx with "Prices Today" sheet)
- Matrixify exports (coming soon)

Usage:
    python scripts/import_data.py
    python scripts/import_data.py --file imports/capricelog-13012026.xlsx
"""
import os
import sys
import re
import argparse
from pathlib import Path
from datetime import datetime, date, timezone
from decimal import Decimal, InvalidOperation

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import text

from app.models.base import SessionLocal, engine, Base
from app.models.competitive_pricing import CompetitivePricing


def parse_decimal(value) -> Decimal | None:
    """Safely parse decimal value"""
    if pd.isna(value) or value is None or value == '':
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def extract_date_from_filename(filename: str) -> date | None:
    """
    Extract date from Caprice filename

    Examples:
        capricelog-13012026.xlsx -> 2026-01-13
        capricelog-07012026.xlsx -> 2026-01-07
        caprice_2026-01-13.xlsx -> 2026-01-13
    """
    # Pattern: DDMMYYYY
    match = re.search(r'(\d{2})(\d{2})(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        try:
            return date(int(year), int(month), int(day))
        except ValueError:
            pass

    # Pattern: YYYY-MM-DD
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            return date(int(year), int(month), int(day))
        except ValueError:
            pass

    return None


def _detect_sheet_and_format(file_path: str):
    """
    Detect the correct sheet name and column format for a Caprice file.

    Returns (sheet_name, format_id) where format_id is:
      4 = "Prices Today" (2026+ files)
      3 = "log" (Jul-Dec 2025 files)
      2 = "Sheet1" with 'Variant ID' column (Feb-Jun 2025)
      1 = "Sheet1" with 'variantId' column (Jan-Feb 2025)
    """
    xl = pd.ExcelFile(file_path)
    sheets = xl.sheet_names

    if 'Prices Today' in sheets:
        return 'Prices Today', 4
    if 'log' in sheets:
        return 'log', 3
    if 'Sheet1' in sheets:
        df_peek = pd.read_excel(file_path, sheet_name='Sheet1', nrows=1)
        if 'Variant ID' in df_peek.columns:
            return 'Sheet1', 2
        return 'Sheet1', 1

    # Fallback: try first sheet
    return sheets[0], 0


def _get_column_map(format_id: int, df_columns) -> dict:
    """
    Return the appropriate column mapping for the detected format.
    Only includes columns that actually exist in the dataframe.
    """
    # Modern format (Prices Today / log)
    modern_map = {
        'Variant ID': 'variant_id',
        'Match': 'match_rule',
        'Set Price': 'set_price',
        'Ceiling Price': 'ceiling_price',
        'Vendor': 'vendor',
        'Variant SKU': 'variant_sku',
        'Title': 'title',
        'RRP': 'rrp',
        '% Off': 'discount_off_rrp_pct',
        'Current Cass Price': 'current_price',
        'Cass Minimum': 'minimum_price',
        'Lowest Price': 'lowest_competitor_price',
        'LowestPrice-MinPrice': 'price_vs_minimum',
        '$ Below Minimum': 'price_vs_minimum',
        'NETT': 'nett_cost',
        '% Profit Margin': 'profit_margin_pct',
        'Profit': 'profit_amount',
        'Profit ($)': 'profit_amount',
        # Competitor prices
        '8appliances': 'price_8appliances',
        'appliancesonline': 'price_appliancesonline',
        'austpek': 'price_austpek',
        'binglee': 'price_binglee',
        'blueleafbath': 'price_blueleafbath',
        'brandsdirectonline': 'price_brandsdirect',
        'buildmat': 'price_buildmat',
        'cookandbathe': 'price_cookandbathe',
        'designerbathware': 'price_designerbathware',
        'harveynorman': 'price_harveynorman',
        'idealbathroomcentre': 'price_idealbathroom',
        'justbathroomware': 'price_justbathroomware',
        'thebluespace': 'price_thebluespace',
        'wellsons': 'price_wellsons',
        'winnings': 'price_winnings',
        'agcequipment': 'price_agcequipment',
        'berloniappliances': 'price_berloniapp',
        'eands': 'price_eands',
        'plumbingsales': 'price_plumbingsales',
        'powerland': 'price_powerland',
        'saappliancewarehouse': 'price_saappliances',
        'samedayhotwaterservice': 'price_sameday',
        'shireskylights': 'price_shire',
        'voguespas': 'price_vogue',
    }

    # Legacy format (Sheet1 with different column names)
    legacy_map = {
        'Variant ID': 'variant_id',
        'variantId': 'variant_id',
        'vendor': 'vendor',
        'sku': 'variant_sku',
        'minimum': 'minimum_price',
        'match': 'match_rule',
        'lowest': 'lowest_competitor_price',
        # Competitor prices (same names across all formats)
        '8appliances': 'price_8appliances',
        'appliancesonline': 'price_appliancesonline',
        'binglee': 'price_binglee',
        'blueleafbath': 'price_blueleafbath',
        'brandsdirectonline': 'price_brandsdirect',
        'buildmat': 'price_buildmat',
        'cookandbathe': 'price_cookandbathe',
        'eands': 'price_eands',
        'harveynorman': 'price_harveynorman',
        'idealbathroomcentre': 'price_idealbathroom',
        'saappliancewarehouse': 'price_saappliances',
        'thebluespace': 'price_thebluespace',
        'wellsons': 'price_wellsons',
        'winnings': 'price_winnings',
        'shireskylights': 'price_shire',
    }

    base_map = modern_map if format_id >= 3 else legacy_map

    # Filter to only include columns present in the dataframe
    col_set = set(df_columns)
    return {k: v for k, v in base_map.items() if k in col_set}


def import_caprice_file(file_path: str, db) -> dict:
    """
    Import a Caprice pricing log Excel file

    Args:
        file_path: Path to the Excel file
        db: Database session

    Returns:
        Dict with import results
    """
    filename = os.path.basename(file_path)

    # Extract date from filename
    pricing_date = extract_date_from_filename(filename)
    if not pricing_date:
        print(f"  WARNING: Could not extract date from filename: {filename}")
        print(f"  Using today's date instead")
        pricing_date = date.today()

    print(f"\n{'='*60}")
    print(f"IMPORTING: {filename}")
    print(f"Pricing Date: {pricing_date}")
    print(f"{'='*60}")

    # Check if we already have data for this date
    existing_count = db.query(CompetitivePricing).filter(
        CompetitivePricing.pricing_date == pricing_date
    ).count()

    if existing_count > 0:
        print(f"  Found {existing_count:,} existing records for {pricing_date}")
        print(f"  Will update existing records")

    # Auto-detect sheet name and format
    try:
        sheet_name, format_id = _detect_sheet_and_format(file_path)
    except Exception as e:
        return {"success": False, "error": f"Failed to detect format: {e}"}

    # Read the Excel file with detected sheet
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        return {"success": False, "error": f"Failed to read file: {e}"}

    print(f"  Rows in file: {len(df):,} (format {format_id}, sheet '{sheet_name}')")

    # Get column mapping for this format
    column_map = _get_column_map(format_id, df.columns)

    # Determine variant ID column name
    variant_id_col = 'variantId' if 'variantId' in df.columns else 'Variant ID'

    imported = 0
    updated = 0
    skipped = 0
    errors = 0

    source_filename = filename
    import_time = datetime.now(timezone.utc)

    # Track variant IDs we've seen in this file to handle duplicates
    seen_variants = set()

    for idx, row in df.iterrows():
        try:
            # Skip rows without variant ID
            variant_id = row.get(variant_id_col)
            if pd.isna(variant_id) or not variant_id:
                skipped += 1
                continue

            variant_id = int(variant_id)

            # Skip duplicate variant IDs within same file
            if variant_id in seen_variants:
                skipped += 1
                continue
            seen_variants.add(variant_id)

            # Check if exists for this date
            existing = db.query(CompetitivePricing).filter(
                CompetitivePricing.variant_id == variant_id,
                CompetitivePricing.pricing_date == pricing_date
            ).first()

            if existing:
                pricing = existing
                updated += 1
            else:
                pricing = CompetitivePricing(
                    variant_id=variant_id,
                    pricing_date=pricing_date
                )
                db.add(pricing)
                imported += 1

            # Map columns
            for excel_col, model_field in column_map.items():
                if excel_col in row.index:
                    value = row[excel_col]

                    # Handle different field types
                    if model_field == 'variant_id':
                        continue  # Already set
                    elif model_field in ['match_rule', 'vendor', 'variant_sku', 'title']:
                        # String fields
                        setattr(pricing, model_field, str(value) if pd.notna(value) else None)
                    else:
                        # Decimal fields
                        setattr(pricing, model_field, parse_decimal(value))

            # Set metadata
            pricing.source_file = source_filename
            pricing.import_date = import_time

            # Calculate flags
            pricing.calculate_flags()

            # Commit every 1000 rows
            if (imported + updated) % 1000 == 0:
                db.commit()
                print(f"  Progress: {imported + updated:,} processed...")

        except Exception as e:
            errors += 1
            db.rollback()  # Rollback failed transaction
            if errors <= 3:
                print(f"  Error row {idx}: {str(e)[:100]}")

    # Final commit
    db.commit()

    # Summary
    print(f"\n  RESULTS for {pricing_date}:")
    print(f"    New records:     {imported:,}")
    print(f"    Updated records: {updated:,}")
    print(f"    Skipped:         {skipped:,}")
    print(f"    Errors:          {errors:,}")

    return {
        "success": True,
        "pricing_date": pricing_date,
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
        "errors": errors
    }


def detect_file_type(file_path: str) -> str:
    """Detect the type of import file"""
    filename = os.path.basename(file_path).lower()

    if 'caprice' in filename:
        return 'caprice'
    elif 'matrixify' in filename:
        return 'matrixify'

    # Try to detect by content
    try:
        xl = pd.ExcelFile(file_path)
        if 'Prices Today' in xl.sheet_names:
            return 'caprice'
        elif 'Orders' in xl.sheet_names:
            return 'matrixify'
    except:
        pass

    return 'unknown'


def process_imports_folder(imports_path: str = 'imports'):
    """Process all files in the imports folder"""
    db = SessionLocal()

    try:
        # Create tables if needed
        Base.metadata.create_all(bind=engine)

        # Find all Excel files
        import_files = sorted(list(Path(imports_path).glob('*.xlsx')))

        if not import_files:
            print(f"No .xlsx files found in {imports_path}/")
            return

        print(f"\nFound {len(import_files)} file(s) to process")
        print(f"Files: {[f.name for f in import_files]}")

        results = []
        for file_path in import_files:
            file_type = detect_file_type(str(file_path))

            if file_type == 'caprice':
                result = import_caprice_file(str(file_path), db)
                results.append(result)
            elif file_type == 'matrixify':
                print(f"\nMatrixify import not yet implemented: {file_path.name}")
            else:
                print(f"\nUnknown file type: {file_path.name}")

        # Final summary
        print(f"\n{'='*60}")
        print(f"IMPORT COMPLETE")
        print(f"{'='*60}")

        total_pricing = db.query(CompetitivePricing).count()
        print(f"Total records in competitive_pricing: {total_pricing:,}")

        # Show date range
        from sqlalchemy import func
        date_stats = db.query(
            func.min(CompetitivePricing.pricing_date),
            func.max(CompetitivePricing.pricing_date),
            func.count(func.distinct(CompetitivePricing.pricing_date))
        ).first()

        if date_stats[0]:
            print(f"Date range: {date_stats[0]} to {date_stats[1]} ({date_stats[2]} days)")

        # Show alerts for latest date
        latest_date = date_stats[1]
        if latest_date:
            losing_money = db.query(CompetitivePricing).filter(
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.is_losing_money == True
            ).count()
            no_cost = db.query(CompetitivePricing).filter(
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.has_no_cost == True
            ).count()
            above_rrp = db.query(CompetitivePricing).filter(
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.is_above_rrp == True
            ).count()

            print(f"\n  ALERTS (latest: {latest_date}):")
            print(f"    Products losing money: {losing_money:,}")
            print(f"    Products without cost: {no_cost:,}")
            print(f"    Products above RRP:    {above_rrp:,}")

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description='Import data files into the database')
    parser.add_argument('--file', '-f', help='Specific file to import')
    parser.add_argument('--folder', default='imports', help='Folder to scan for imports')

    args = parser.parse_args()

    if args.file:
        db = SessionLocal()
        try:
            Base.metadata.create_all(bind=engine)
            file_type = detect_file_type(args.file)
            if file_type == 'caprice':
                import_caprice_file(args.file, db)
            else:
                print(f"Unknown file type: {args.file}")
        finally:
            db.close()
    else:
        process_imports_folder(args.folder)


if __name__ == '__main__':
    main()
