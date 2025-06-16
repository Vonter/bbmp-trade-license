#!/usr/bin/env python3
import gzip
import os
import polars as pl
import logging

from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
from utils import *

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def process_html_file(file_path):
    """Process a single HTML file and extract all required fields."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract basic fields
        base_data = {
            'Application Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblApplicationNumber'),
            'Date': extract_field_by_id(soup, 'ContentPlaceHolder1_lblSubmitDate'),
            'Applicant Name': extract_field_by_id(soup, 'ContentPlaceHolder1_lblApplicantName'),
            'Trade Name': extract_field_by_id(soup, 'ContentPlaceHolder1_lblTradeName'),

            'Trade Type': extract_field_by_id(soup, 'ContentPlaceHolder1_lblTradeType'),

            'Paid Amount': extract_field_by_id(soup, 'ContentPlaceHolder1_lblLicensetotalfee'),
            'Penalty Amount': extract_field_by_id(soup, 'ContentPlaceHolder1_lblPenalty'),
            'Total Paid Amount': extract_field_by_id(soup, 'ContentPlaceHolder1_lblLicenseFee'),

            'Telephone Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblTelephoneNo'),
            'Mobile Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblMobileNo'),
            'E-Mail ID': extract_field_by_id(soup, 'ContentPlaceHolder1_lblEmailID'),

            'Constituency': extract_field_by_id(soup, 'ContentPlaceHolder1_lblMLConstituency'),
            'Ward': extract_field_by_id(soup, 'ContentPlaceHolder1_lblWard'),
            'Address Door Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblDoorNo'),
            'Address Street': extract_field_by_id(soup, 'ContentPlaceHolder1_lblStreet'),
            'Address Area': extract_field_by_id(soup, 'ContentPlaceHolder1_lblArea'),
            'Address PIN': extract_field_by_id(soup, 'ContentPlaceHolder1_lblPIN'),
            
            'Zonal Classification': extract_field_by_id(soup, 'ContentPlaceHolder1_lblZonalCalssification'),
            'Property ID': extract_field_by_id(soup, 'ContentPlaceHolder1_lblPropertyID'),
            'Property Survey Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblSurveyNo'),
            'BESCOM RR Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblBESCOMNo'),
            'VAT Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblVATNo'),
            'TIN Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblTINNo'),
            'Old Application Number': extract_field_by_id(soup, 'ContentPlaceHolder1_lblOldApplicationNumber'),
        }
        
        # Extract trade information from table
        trade_info = extract_trade_information_table(soup)
        
        records = []
        if trade_info:
            for trade in trade_info:
                record = base_data.copy()
                record.update(trade)
                records.append(record)
        else:
            records.append(base_data)
        
        return records
    
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {str(e)}")
        return []

def main():
    logging.info("Starting application data extraction...")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Find all HTML files recursively in raw/applications
    applications_dir = Path('raw/applications')
    if not applications_dir.exists():
        logging.error(f"Directory {applications_dir} does not exist")
        return
    html_files = list(applications_dir.rglob('*.html'))
    logging.info(f"Found {len(html_files)} HTML files to process")
    if not html_files:
        logging.warning("No HTML files found in raw/applications directory")
        return
    
    # Load existing applications from Parquet and filter out those that are already in the Parquet
    existing_apps = get_all_existing_applications()
    if existing_apps:
        html_files = [file for file in html_files if str(file.stem).isdigit() and int(file.stem) not in existing_apps.keys()]
        logging.info(f"Processing {len(html_files)} new HTML files")
    else:
        logging.info(f"No existing applications found, processing all {len(html_files)} HTML files")

    # Process all files that are not already in the Parquet
    all_records = []
    for file_path in tqdm(html_files, desc="Processing HTML files"):
        records = process_html_file(file_path)
        if records[0]['Applicant Name'] == '':
            continue
        all_records.extend(records)

    # Create DataFrame if there are new records, otherwise use existing DataFrame
    if all_records:
        df = pl.DataFrame(all_records)
        df = df.with_columns(pl.col('Date').str.strptime(pl.Datetime, format='%d-%m-%Y', strict=False))
    else:
        logging.info("No new records to process, skipping Parquet creation.")
        return

    # Append existing DataFrame to new DataFrame
    if existing_apps:
        existing_df = pl.read_parquet('data/combined.parquet')
        df = pl.concat([existing_df, df])
        
    # Add derived columns
    df = df.with_columns(pl.col('Application Number').str.slice(0, 1).alias('Application Type'))
    df = df.with_columns(pl.col('Application Type').map_elements(lambda x: 'Renewal' if x == 'R' else 'New', return_dtype=pl.Utf8).alias('Application Type'))
    df = df.with_columns(pl.col('Application Number').str.slice(9).alias('Application ID').cast(pl.Int64))

    # Cast columns
    df = df.with_columns(pl.col('Application Type').cast(pl.Categorical))
    df = df.with_columns(pl.col('Trade Type').cast(pl.Categorical))
    df = df.with_columns(pl.col('Major Trade').cast(pl.Categorical))
    df = df.with_columns(pl.col('Minor Trade').cast(pl.Categorical))
    df = df.with_columns(pl.col('Sub Trade').cast(pl.Categorical))
    df = df.with_columns(pl.col('Paid Amount').cast(pl.Float64))
    df = df.with_columns(pl.col('Penalty Amount').cast(pl.Float64))
    df = df.with_columns(pl.col('Total Paid Amount').cast(pl.Float64))
    df = df.with_columns(pl.col('Constituency').cast(pl.Categorical))
    df = df.with_columns(pl.col('Ward').cast(pl.Categorical))

    # Sort columns
    df = df.select(['Application Number', 'Application Type', 'Date', 'Application ID', 'Applicant Name', 'Trade Name', 'Trade Type', 'Major Trade', 'Minor Trade', 'Sub Trade', 'Paid Amount', 'Penalty Amount', 'Total Paid Amount', 'Telephone Number', 'Mobile Number', 'E-Mail ID', 'Constituency', 'Ward', 'Address Door Number', 'Address Street', 'Address Area', 'Address PIN', 'Zonal Classification', 'Property ID', 'Property Survey Number', 'BESCOM RR Number', 'VAT Number', 'TIN Number', 'Old Application Number'])
        
    # Remove duplicates based on Application Number and trade information
    df = df.unique(subset=['Application Number', 'Major Trade', 'Minor Trade', 'Sub Trade'])

    # Sort by Date descending
    df = df.sort('Application ID', descending=True)

    # Drop null rows
    df = df.drop_nulls()

    # Save as Parquet
    df.write_parquet('data/combined.parquet')

    # Remove unneeded fields for final dataset
    df = df.drop(['Application Number', 'Applicant Name', 'Telephone Number', 'Mobile Number', 'E-Mail ID', 'Property ID', 'Property Survey Number', 'BESCOM RR Number', 'VAT Number', 'TIN Number', 'Old Application Number'])
    
    # Save final Parquet and compressed CSV
    df.write_parquet('data/trade-license.parquet')
    with gzip.open('data/trade-license.csv.gz', 'wb') as f:
        df.write_csv(f)

    logging.info(f"Successfully saved {len(df)} records")

if __name__ == '__main__':
    main()