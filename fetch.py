#!/usr/bin/env python3
import requests
import time

from pathlib import Path
from utils import *

def fetch_application(license_num):
    """Fetch single application and return response."""
    return requests.post(
        'https://trade.bbmpgov.in/Forms/frmApplicationStatusPublic.aspx',
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://trade.bbmpgov.in',
            'Connection': 'keep-alive',
            'Referer': 'https://trade.bbmpgov.in/Forms/frmApplicationStatusPublic.aspx',
            'Upgrade-Insecure-Requests': '1'
        },
        data={
            'ToolkitScriptManager1_HiddenField': '',
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': '/wEPDwUKMTcwNDA4ODI5Mw9kFgJmD2QWAgIDD2QWAgIDD2QWAgIBD2QWAgIBD2QWBgIrD2QWAmYPZBYCAgEPPCsAEQIBEBYAFgAWAAwUKwAAZAI5DzwrABECARAWABYAFgAMFCsAAGQCRQ8UKwAFZCgpWFN5c3RlbS5HdWlkLCBtc2NvcmxpYiwgVmVyc2lvbj00LjAuMC4wLCBDdWx0dXJlPW5ldXRyYWwsIFB1YmxpY0tleVRva2VuPWI3N2E1YzU2MTkzNGUwODkkNWRjNjIyYzYtOGZlMy00ODVjLWIwOGEtZTJiZjM5ZTI1ODBlAgEUKwABPCsABAEAZmRkGAMFJGN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkbXVsdGl2aWV3MQ8PZGZkBSNjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJGd2UmVtYXJrcw9nZAUsY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRndlRyYWRlSW5mb3JtYXRpb24PZ2SOUkV05Wpxb3B/yepQzOGGcaZwxf7eEurfGitU2+qr6g==',
            '__VIEWSTATEGENERATOR': 'EA52BDA5',
            '__EVENTVALIDATION': '/wEdAAPgoOWtpqRBwHWZ4MJ1gHo/Ai4zgla7TU2Rym4tehP4uo5MW6XMaFjzT9nUdsIknotNgqy55/CIIamJztmsYz0N2sCjSa/oNelPW+ydyIU8WQ==',
            'ctl00$ContentPlaceHolder1$txtApplicationNumber': license_num,
            'ctl00$ContentPlaceHolder1$btnSearch': 'Get Status'
        }
    )

def save_application(response_text, year, month, day, app_num, license_type):
    """Save application data to file."""
    output_dir = Path('raw/applications') / str(year) / str(month).zfill(2) / \
                 str(day).zfill(2) / ('renewal' if license_type == 'R' else 'new')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{app_num}.html"
    output_file.write_text(response_text, encoding='utf-8')

def fetch_missing_applications():
    """Fetch missing applications by filling gaps in application numbers."""
    existing_apps = get_all_existing_applications()
    failed_apps = load_save_failed_apps()
    
    if not existing_apps:
        print("No existing applications found")
        return

    # Start from the first known valid application number
    app_num = min(existing_apps.keys())
    year, month, day = existing_apps.get(app_num)
    last_success_date = (year, month, day)
    consecutive_failures = consecutive_invalid = 0

    max_date_attempts = 2
    max_consecutive_invalid_attempts = 10

    while app_num > 0:
        # End if date is in the future and too many invalid app numbers
        if is_future_date(year, month, day) and consecutive_invalid >= max_consecutive_invalid_attempts:
            print(f"Date is in the future, exiting. Last successful date: {year}/{month}/{day}")
            break

        # Save failed app numbers after too many invalid app numbers
        if consecutive_invalid > max_consecutive_invalid_attempts:
            load_save_failed_apps(failed_apps, save=True)
            consecutive_failures = consecutive_invalid = 0
            app_num += 1
            continue

        # Skip existing applications
        if app_num in existing_apps:
            year, month, day = existing_apps.get(app_num)
            last_success_date = (year, month, day)
            consecutive_failures = 0
            app_num += 1
            continue

        # Skip previously failed applications
        if app_num in existing_apps or app_num in failed_apps:
            consecutive_failures = 0
            app_num += 1
            continue

        # Mark as failed after max attempts
        if consecutive_failures == max_date_attempts:
            print(f"Failed fetching {app_num}. Last attempted date: {year}/{month}/{day}")
            failed_apps.add(app_num)
            consecutive_failures = 0
            consecutive_invalid += 1
            app_num += 1
            year, month, day = last_success_date
            continue

        # Try both renewal and new application types
        success = False
        for license_type in ['R', 'N']:
            license_num = generate_license_number(year, month, day, app_num, license_type)

            try:
                response = fetch_application(license_num)
                
                if 'ContentPlaceHolder1_lblApplicationNumber' not in response.text or response.status_code != 200:
                    continue

                save_application(response.text, year, month, day, app_num, license_type)
                print(f"Successfully fetched {license_num}")
             
                existing_apps[app_num] = (year, month, day)
                last_success_date = (year, month, day)
                success = True
                time.sleep(1)
                break

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {license_num}: {e}")
                time.sleep(1)
                continue
        
        if success:
            consecutive_failures = 0
            app_num += 1
        else:
            consecutive_failures += 1
            year, month, day = get_next_date(year, month, day)

if __name__ == "__main__":
    Path('raw').mkdir(exist_ok=True)
    fetch_missing_applications()
