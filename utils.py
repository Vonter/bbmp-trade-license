#!/usr/bin/env python3
import datetime
import polars as pl

from pathlib import Path

def get_all_existing_applications():
    """Get all existing application numbers/dates from data/combined.parquet."""
    existing_apps = {}
    
    # Load from Parquet
    try:
        df = pl.read_parquet('data/combined.parquet')
        existing_apps = df.select(['Application Number', 'Date']).to_dicts()
        existing_apps = {int(app['Application Number'][9:]): (int("20" + app['Application Number'][1:3]), int(app['Application Number'][7:9]), int(app['Application Number'][5:7])) for app in existing_apps if app['Date'] is not None}
    except:
        return {}

    return existing_apps

def parse_license_number(license_num):
    """Parse license number and return components."""
    if not license_num or len(license_num) < 14:
        raise ValueError(f"Invalid license number format: {license_num}")
    
    return {
        'type': 'renewal' if license_num[0] == 'R' else 'new',
        'application_year': '20' + license_num[1:3],
        'month': license_num[7:9],
        'day': license_num[5:7],
        'application_number': license_num[9:]
    }

def generate_license_number(year, month, day, app_num, license_type='R'):
    """Generate license number in correct format."""
    return f"{license_type}{str(year)[-2:]}{str(year+1)[-2:]}{str(day).zfill(2)}{str(month).zfill(2)}{app_num}"

def load_save_failed_apps(failed_apps=None, save=False):
    """Load or save failed application numbers."""
    failed_file = Path('.failed.txt')
    
    if save and failed_apps is not None:
        try:
            failed_file.write_text('\n'.join(map(str, sorted(failed_apps))) + '\n')
            print(f"Saved {len(failed_apps)} failed application numbers")
        except Exception as e:
            print(f"Error saving failed app numbers: {e}")
        return
    
    # Load mode
    if not failed_file.exists():
        return set()
    
    try:
        failed_apps = {int(line.strip()) for line in failed_file.read_text().strip().split('\n') if line.strip().isdigit()}
        print(f"Loaded {len(failed_apps)} previously failed application numbers")
        return failed_apps
    except Exception as e:
        print(f"Error loading failed app numbers: {e}")
        return set()

def get_next_date(year, month, day, delta=1):
    """Get next date using datetime module."""
    date = datetime.date(year, month, day) + datetime.timedelta(days=delta)
    return date.year, date.month, date.day

def is_future_date(year, month, day):
    """Check if date is in the future."""
    today = datetime.date.today()
    return datetime.date(year, month, day) > today

def extract_field_by_id(soup, field_id, default=""):
    """Extract text content from an element by its ID."""
    element = soup.find(id=field_id)
    if element:
        return element.get_text(strip=True)
    return default

def extract_trade_information_table(soup):
    """Extract trade information from the gvTradeInformation table."""
    trade_info = []
    table = soup.find(id="ContentPlaceHolder1_gvTradeInformation")
    
    if table:
        # Find all data rows (skip header row)
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 3:  # We need at least 3 columns for Major, Minor, Sub Trade
                major_trade = cols[0].get_text(strip=True)
                minor_trade = cols[1].get_text(strip=True)
                sub_trade = cols[2].get_text(strip=True)
                
                trade_info.append({
                    'Major Trade': major_trade,
                    'Minor Trade': minor_trade,
                    'Sub Trade': sub_trade
                })
    
    return trade_info