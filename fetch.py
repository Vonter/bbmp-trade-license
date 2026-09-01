#!/usr/bin/env python3
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils import (
    generate_license_number,
    get_all_existing_applications,
    get_next_date,
    is_future_date,
    load_save_failed_apps,
)


APPLICATION_STATUS_URL = (
    'https://trade.bbmpgov.in/Forms/frmApplicationStatusPublic.aspx'
)
APPLICATION_NUMBER_FIELD = 'ctl00$ContentPlaceHolder1$txtApplicationNumber'
SEARCH_BUTTON_FIELD = 'ctl00$ContentPlaceHolder1$btnSearch'
RESULT_APPLICATION_NUMBER_ID = 'ContentPlaceHolder1_lblApplicationNumber'


class PortalResponseError(RuntimeError):
    """Raised when the BBMP portal does not return a usable search form."""


class BBMPClient:
    """Fetch applications using fresh ASP.NET form state for every search."""

    def __init__(self, session=None, timeout=30, max_retries=3, retry_delay=1):
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/144.0.0.0 Safari/537.36'
            ),
            'Accept': (
                'text/html,application/xhtml+xml,application/xml;q=0.9,'
                '*/*;q=0.8'
            ),
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def _fetch_once(self, license_num):
        # ASP.NET ties VIEWSTATE/EVENTVALIDATION to the current page/session.
        # Values copied during an earlier run start returning HTTP 500 after
        # the portal application or its session is restarted.
        form_response = self.session.get(
            APPLICATION_STATUS_URL,
            timeout=self.timeout,
        )
        form_response.raise_for_status()

        soup = BeautifulSoup(form_response.text, 'html.parser')
        form = soup.find('form', id='form1')
        if form is None:
            raise PortalResponseError('BBMP response did not contain the search form')

        form_data = {
            field['name']: field.get('value', '')
            for field in form.select('input[type="hidden"][name]')
        }
        missing_fields = {
            '__VIEWSTATE',
            '__VIEWSTATEGENERATOR',
            '__EVENTVALIDATION',
        } - form_data.keys()
        if missing_fields:
            missing = ', '.join(sorted(missing_fields))
            raise PortalResponseError(f'BBMP search form is missing: {missing}')

        form_data.update({
            APPLICATION_NUMBER_FIELD: license_num,
            SEARCH_BUTTON_FIELD: 'Get Status',
        })
        action_url = urljoin(form_response.url, form.get('action', ''))
        response = self.session.post(
            action_url,
            data=form_data,
            headers={'Referer': form_response.url},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response

    def fetch_application(self, license_num):
        """Fetch one application, retrying the complete form handshake."""
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._fetch_once(license_num)
            except (requests.exceptions.RequestException, PortalResponseError):
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_delay * attempt)


def fetch_application(license_num, client=None):
    """Fetch a single application and return its response."""
    return (client or BBMPClient()).fetch_application(license_num)


def response_application_number(response):
    """Return the application number shown in a successful result."""
    soup = BeautifulSoup(response.text, 'html.parser')
    element = soup.find(id=RESULT_APPLICATION_NUMBER_ID)
    return element.get_text(strip=True) if element else None


def save_application(response_text, year, month, day, app_num, license_type):
    """Save application data to file."""
    output_dir = (
        Path('raw/applications')
        / str(year)
        / str(month).zfill(2)
        / str(day).zfill(2)
        / ('renewal' if license_type == 'R' else 'new')
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f'{app_num}.html'
    output_file.write_text(response_text, encoding='utf-8')


def fetch_missing_applications(client=None):
    """Fetch missing applications by filling gaps in application numbers."""
    existing_apps = get_all_existing_applications()
    failed_apps = load_save_failed_apps()
    client = client or BBMPClient()

    if not existing_apps:
        print('No existing applications found')
        return

    # Start from the first known valid application number.
    app_num = min(existing_apps)
    year, month, day = existing_apps[app_num]
    last_success_date = (year, month, day)
    consecutive_failures = 0
    consecutive_invalid = 0
    failures_since_save = 0

    max_date_attempts = 2
    max_consecutive_invalid_attempts = 10

    while app_num > 0:
        # Once the scan reaches tomorrow, a run of invalid IDs means it has
        # caught up with the portal.
        if (
            is_future_date(year, month, day)
            and consecutive_invalid >= max_consecutive_invalid_attempts
        ):
            print(
                'Date is in the future, exiting. '
                f'Last successful date: {last_success_date[0]}/'
                f'{last_success_date[1]}/{last_success_date[2]}'
            )
            break

        # Persist in batches, but do not advance app_num here. The previous
        # implementation did so and silently skipped one untested ID after
        # every batch of failures.
        if failures_since_save >= max_consecutive_invalid_attempts:
            load_save_failed_apps(failed_apps, save=True)
            failures_since_save = 0

        if app_num in existing_apps:
            year, month, day = existing_apps[app_num]
            last_success_date = (year, month, day)
            consecutive_failures = 0
            consecutive_invalid = 0
            app_num += 1
            continue

        if app_num in failed_apps:
            consecutive_failures = 0
            app_num += 1
            continue

        if consecutive_failures == max_date_attempts:
            print(
                f'Failed fetching {app_num}. '
                f'Last attempted date: {year}/{month}/{day}'
            )
            failed_apps.add(app_num)
            failures_since_save += 1
            consecutive_failures = 0
            consecutive_invalid += 1
            app_num += 1
            year, month, day = last_success_date
            continue

        success = False
        for license_type in ('R', 'N'):
            license_num = generate_license_number(
                year,
                month,
                day,
                app_num,
                license_type,
            )

            try:
                response = fetch_application(license_num, client=client)
                result_license_num = response_application_number(response)

                if result_license_num is None:
                    continue
                if result_license_num != license_num:
                    raise PortalResponseError(
                        f'BBMP returned {result_license_num} for {license_num}'
                    )

                save_application(
                    response.text,
                    year,
                    month,
                    day,
                    app_num,
                    license_type,
                )
                print(f'Successfully fetched {license_num}')

                existing_apps[app_num] = (year, month, day)
                last_success_date = (year, month, day)
                success = True
                time.sleep(1)
                break

            except (requests.exceptions.RequestException, PortalResponseError) as exc:
                # A portal/transport failure says nothing about whether the
                # license exists. Abort instead of poisoning .failed.txt.
                print(f'Portal error fetching {license_num}: {exc}')
                if failures_since_save:
                    load_save_failed_apps(failed_apps, save=True)
                return

        if success:
            consecutive_failures = 0
            consecutive_invalid = 0
            app_num += 1
        else:
            consecutive_failures += 1
            year, month, day = get_next_date(year, month, day)

    if failures_since_save:
        load_save_failed_apps(failed_apps, save=True)


if __name__ == '__main__':
    Path('raw').mkdir(exist_ok=True)
    fetch_missing_applications()
