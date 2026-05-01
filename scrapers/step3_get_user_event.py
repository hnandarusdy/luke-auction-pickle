#!/usr/bin/env python3
"""
Step 3: Get User Events - Extract EventIDs from API after opening auction watch URLs.
"""

import time
import re
import json
import csv
import sys
import os
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.browser import PicklesBrowser
from utils.whatsapp_notifier import with_error_notification
from utils.logger import get_logger
from db.connection import MySecondDB


class Step2WatchEventDB:
    """Database operations for pickles_live_step2_watch_event table"""

    def __init__(self):
        self.db = MySecondDB()
        self.logger = get_logger("step2_watch_event_db", log_to_file=True)

    def insert_watch_events(self, watch_events_data):
        try:
            if not watch_events_data:
                return 0
            df = pd.DataFrame(watch_events_data)
            expected_columns = ['row_order', 'auction_registration', 'auction_watch_url',
                              'event_id', 'event_name', 'event_status']
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_columns]
            self.db.write_to_sql(df, 'pickles_live_step2_watch_event', how='append')
            inserted_count = len(watch_events_data)
            print(f"✅ Inserted {inserted_count} watch events into database")
            self.logger.info(f"Inserted {inserted_count} watch events into pickles_live_step2_watch_event")
            return inserted_count
        except Exception as e:
            self.logger.error(f"Error inserting watch events: {str(e)}")
            print(f"❌ Database insert failed: {str(e)}")
            raise


class EventIDExtractor:
    """Extract EventIDs from Velocicast API after opening auction watch URLs."""

    def __init__(self):
        self.logger = get_logger("event_id_extractor", log_to_file=True)
        self.browser = None
        self.db_handler = Step2WatchEventDB()
        self.USERNAME = "hnandarusdy2@gmail.com"
        self.PASSWORD = "123qwe!@#QWE"

    def read_step2_csv(self) -> List[str]:
        """Read pickles_auction_step2.csv and extract auction_registration list."""
        try:
            print("📄 Reading pickles_auction_step2.csv...")
            df = pd.read_csv("pickles_auction_step2.csv")
            print(f"   📊 Total records found: {len(df)}")

            valid_urls = df[
                df['auction_registration'].notna() &
                (df['auction_registration'].str.strip() != '')
            ]
            print(f"   🔗 Valid registration URLs: {len(valid_urls)}")
            auction_registration_urls = valid_urls['auction_registration'].tolist()
            print(f"   ✅ Registration URLs to process: {len(auction_registration_urls)}")

            for i, url in enumerate(auction_registration_urls, 1):
                print(f"   {i}. {url[:80]}...")

            return auction_registration_urls
        except Exception as e:
            print(f"❌ Error reading CSV: {str(e)}")
            self.logger.error(f"Error reading step2 CSV: {str(e)}")
            return []

    def login_to_pickles(self) -> bool:
        """Login to Pickles website."""
        try:
            print("🔐 Logging into Pickles...")
            self.browser = PicklesBrowser(headless=False, wait_timeout=15)
            self.browser.setup()

            if not self.browser.login(self.USERNAME, self.PASSWORD):
                print("❌ Login failed!")
                return False

            print("✅ Successfully logged in!")
            return True
        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            self.logger.error(f"Login error: {str(e)}")
            return False

    def open_registration_and_watch(self, registration_url: str, index: int) -> bool:
        """Open auction registration URL and click 'Just watch' button."""
        try:
            print(f"   🌐 Opening registration page ({self._get_ordinal(index)})...")
            print(f"   🔗 URL: {registration_url}")

            self.browser.page.goto(registration_url, wait_until="domcontentloaded")
            print(f"   ⏳ Waiting for page to load...")
            time.sleep(3)

            print(f"   🔍 Looking for 'Just watch' button...")

            just_watch_selectors = [
                "//a[contains(text(), 'Just watch')]",
                "//a[contains(@class, 'btn') and contains(text(), 'Just watch')]",
                "//a[@rel='nofollow' and contains(text(), 'Just watch')]",
                "//a[contains(@href, 'registrationType=LIVE_VIEW')]"
            ]

            button_found = False
            for selector in just_watch_selectors:
                try:
                    btn = self.browser.page.locator(f"xpath={selector}")
                    if btn.count() > 0:
                        btn.first.click()
                        print(f"   ✅ Clicked 'Just watch' button")
                        button_found = True
                        break
                except Exception:
                    continue

            if not button_found:
                print(f"   ⚠️ 'Just watch' button not found, trying fallback...")
                try:
                    reg_link = self.browser.page.locator("a[href*='registrationType=LIVE_VIEW']")
                    if reg_link.count() > 0:
                        reg_link.first.click()
                        print(f"   ✅ Clicked registration link as fallback")
                        button_found = True
                except Exception:
                    pass

            if button_found:
                print(f"   ⏳ Waiting 3 seconds after clicking...")
                time.sleep(3)

                print(f"   📍 Current URL: {self.browser.page.url}")

                # Check if redirected to Registration Form page
                if "Registration Form" in (self.browser.page.title() or ""):
                    print(f"   🔄 Redirected to Registration Form page")
                    try:
                        confirm_btn = self.browser.page.locator("input[value='Confirm'], input[value='CONFIRM']")
                        if confirm_btn.count() > 0:
                            confirm_btn.first.click()
                            print(f"   ✅ Clicked 'CONFIRM' button")
                            time.sleep(3)
                    except Exception as e:
                        print(f"   ⚠️ CONFIRM button not found: {str(e)}")

                print(f"   ✅ Successfully processed {self._get_ordinal(index)} registration")
                return True
            else:
                print(f"   ❌ Could not find or click 'Just watch' button")
                return False

        except Exception as e:
            print(f"   💥 Error processing registration: {str(e)}")
            self.logger.error(f"Error processing {registration_url}: {str(e)}")
            return False

    def open_watch_urls(self, registration_urls: List[str]) -> bool:
        """Open auction watch URLs with delays."""
        try:
            print("📄 Reading auction_watch_url from CSV...")
            df = pd.read_csv("pickles_auction_step2.csv")

            valid_watch_urls = df[
                df['auction_watch_url'].notna() &
                (df['auction_watch_url'].str.strip() != '')
            ]
            watch_urls = valid_watch_urls['auction_watch_url'].tolist()
            print(f"   🔗 Found {len(watch_urls)} watch URLs to process")

            if not watch_urls:
                print("   ⚠️ No watch URLs found, skipping")
                return True

            for i, watch_url in enumerate(watch_urls, 1):
                print(f"\n📺 Opening watch URL {i}/{len(watch_urls)}: {watch_url[:80]}...")
                try:
                    self.browser.page.goto(watch_url, wait_until="domcontentloaded")
                    time.sleep(3)
                    print(f"   ✅ Opened watch URL {i}")
                except Exception as e:
                    print(f"   ❌ Error: {str(e)}")

                if i < len(watch_urls):
                    time.sleep(1)

            print(f"\n✅ All {len(watch_urls)} watch URLs processed!")
            return True
        except Exception as e:
            print(f"❌ Error processing watch URLs: {str(e)}")
            return False

    def get_user_events_api(self) -> List[Dict]:
        """Get all user events from the refresh API."""
        try:
            print("🔄 Getting user events from API...")

            api_url = "https://api.pickles-au.velocicast.io/api/events/refresh/user-events"
            print(f"🔗 Getting API: {api_url}")

            # Open API in new tab
            new_page = self.browser.open_new_tab(api_url)
            time.sleep(3)

            page_source = new_page.content()
            print(f"📊 API Response received (length: {len(page_source)} chars)")

            try:
                json_match = re.search(r'\[.*\]', page_source, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group(0))
                    if isinstance(data, list):
                        events = [item for item in data if isinstance(item, dict) and 'EventID' in item]
                        print(f"✅ Found {len(events)} events with EventIDs")
                        for i, event in enumerate(events, 1):
                            print(f"   {i}. EventID: {event.get('EventID')} - {event.get('Name')}")
                        new_page.close()
                        return events
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON: {str(e)}")

            new_page.close()
            return []
        except Exception as e:
            print(f"💥 Error getting user events: {str(e)}")
            self.logger.error(f"Error getting user events: {str(e)}")
            return []

    def update_step2_csv_with_events(self, events: List[Dict]) -> List[Dict]:
        """Update pickles_auction_step2.csv with event IDs."""
        try:
            print("📝 Updating pickles_auction_step2.csv with event IDs...")
            df = pd.read_csv("pickles_auction_step2.csv")

            if 'event_id' not in df.columns:
                df['event_id'] = None
            if 'event_name' not in df.columns:
                df['event_name'] = None

            watch_events_data = []
            for i, event in enumerate(events):
                row_index = i
                row_order = i + 1
                event_id = str(event.get('EventID', ''))
                event_name = event.get('Name', '')
                event_status = event.get('Status', '')

                print(f"   📌 Mapping Event {row_order}: EventID {event_id} → Row {row_order}")

                if row_index < len(df):
                    df.loc[row_index, 'event_id'] = event_id
                    df.loc[row_index, 'event_name'] = event_name
                    watch_events_data.append({
                        'row_order': row_order,
                        'auction_registration': df.loc[row_index, 'auction_registration'],
                        'auction_watch_url': df.loc[row_index, 'auction_watch_url'],
                        'event_id': event_id,
                        'event_name': event_name,
                        'event_status': event_status
                    })
                else:
                    watch_events_data.append({
                        'row_order': row_order,
                        'auction_registration': None,
                        'auction_watch_url': None,
                        'event_id': event_id,
                        'event_name': event_name,
                        'event_status': event_status
                    })

            df.to_csv("pickles_auction_step2.csv", index=False)
            print(f"✅ Updated pickles_auction_step2.csv with {len(events)} event IDs")
            return watch_events_data
        except Exception as e:
            print(f"❌ Error updating step2 CSV: {str(e)}")
            self.logger.error(f"Error updating step2 CSV: {str(e)}")
            return []

    def fetch_and_save_items_json(self, event_id: str) -> bool:
        """Fetch items data from API and save as JSON file."""
        try:
            print(f"📦 Fetching items data for EventID: {event_id}")
            from datetime import datetime

            json_folder = "json_data"
            os.makedirs(json_folder, exist_ok=True)

            items_api_url = f"https://api.pickles-au.velocicast.io/api/events/{event_id}/items?user=true"
            print(f"🔗 Getting items API: {items_api_url}")

            new_page = self.browser.open_new_tab(items_api_url)
            time.sleep(3)

            page_source = new_page.content()
            new_page.close()

            json_match = re.search(r'\{.*\}|\[.*\]', page_source, re.DOTALL)
            if json_match:
                json_data = json.loads(json_match.group(0))
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                json_filename = os.path.join(json_folder, f"{event_id}_{timestamp}.json")
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                print(f"✅ Saved items JSON: {json_filename}")
                return True
            else:
                print(f"❌ No JSON found in items response")
                return False

        except Exception as e:
            print(f"💥 Error fetching items data: {str(e)}")
            self.logger.error(f"Error fetching items for EventID {event_id}: {str(e)}")
            return False

    def _get_ordinal(self, number: int) -> str:
        if 10 <= number % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th')
        return f"{number}{suffix}"

    def run(self):
        """Main execution method."""
        print("🚀 Starting EventID Extraction...")
        try:
            print("\n1️⃣ Reading pickles_auction_step2.csv...")
            registration_urls = self.read_step2_csv()
            if not registration_urls:
                print("❌ No registration URLs found. Exiting.")
                return

            print("\n2️⃣ Logging into Pickles...")
            if not self.login_to_pickles():
                return

            print(f"\n3️⃣ Processing {len(registration_urls)} registration URLs...")
            for i, registration_url in enumerate(registration_urls, 1):
                print(f"\n📄 Processing registration {i}/{len(registration_urls)}:")
                self.open_registration_and_watch(registration_url, i)
                if i < len(registration_urls):
                    time.sleep(1)

            print(f"\n✅ All {len(registration_urls)} registrations processed!")

            print(f"\n🔗 Processing auction watch URLs...")
            self.open_watch_urls(registration_urls)

            print(f"\n4️⃣ Getting user events from API...")
            events = self.get_user_events_api()
            if not events:
                print("❌ No events found. Exiting.")
                return

            print(f"\n📝 Updating CSV with event IDs...")
            watch_events_data = self.update_step2_csv_with_events(events)

            print(f"\n💾 Inserting watch events into database...")
            try:
                self.db_handler.insert_watch_events(watch_events_data)
            except Exception as e:
                print(f"❌ Database insert failed: {str(e)}")

            print(f"\n5️⃣ Fetching items for {len(events)} events...")
            successful_saves = 0
            for i, event in enumerate(events, 1):
                event_id = str(event.get('EventID', ''))
                print(f"\n📦 Event {i}/{len(events)}: EventID {event_id}")
                if event_id and self.fetch_and_save_items_json(event_id):
                    successful_saves += 1
                if i < len(events):
                    time.sleep(1)

            print(f"\n🎉 Completed! JSON files saved: {successful_saves}/{len(events)}")

        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            self.logger.error(f"Unexpected error in run(): {str(e)}")
        finally:
            if self.browser:
                self.browser.close()


@with_error_notification()
def main():
    extractor = EventIDExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
