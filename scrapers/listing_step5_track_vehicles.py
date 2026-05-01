#!/usr/bin/env python3
"""
Step 5: Track Vehicles by Stock Number

Queries stock numbers from DB where max scrape date > current date,
opens each vehicle's details page, and captures bidding API responses.
Uses Playwright for browser automation.
"""

import os
import sys
import json
import time
import re
import requests
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.browser import PicklesBrowser
from db.connection import MySecondDB
from utils.logger import get_logger
from utils.whatsapp_notifier import with_error_notification


class VehicleTracker:
    """Track vehicles by stock number and capture bidding API responses"""

    def __init__(self):
        self.db = MySecondDB()
        self.logger = get_logger("vehicle_tracker", log_to_file=True)
        self.browser = None
        self.output_folder = str(PROJECT_ROOT / "vehicles_tracking")
        os.makedirs(self.output_folder, exist_ok=True)

    def setup_driver(self):
        """Setup Playwright browser"""
        try:
            print("🔧 Setting up Playwright browser...")
            self.browser = PicklesBrowser(enable_network_logging=True)
            self.browser.setup()
            print("✅ Browser setup successful")
            self.logger.info("Playwright browser setup completed")
            return True
        except Exception as e:
            self.logger.error(f"Error setting up browser: {str(e)}")
            print(f"❌ Error setting up browser: {str(e)}")
            return False

    def get_stock_numbers_to_track(self):
        """Query database for stock numbers that need tracking"""
        try:
            query = """
            with stg1 as (
                select stockNumber,
                       CURDATE() as cur_date,
                       MAX(DATE_ADD(saleEnd, INTERVAL 7 DAY)) as max_scrape_date
                from pickles_sale_info
                group by stockNumber
            )
            select *, max_scrape_date > cur_date as is_trackable
            from stg1
            where max_scrape_date > CURDATE()
            """
            print("📊 Querying database for trackable stock numbers...")
            df = self.db.read_sql(query)

            if df.empty:
                print("⚠️ No stock numbers found for tracking")
                return []

            print(f"✅ Found {len(df)} stock numbers to track")
            stock_list = df.to_dict('records')
            for stock in stock_list:
                print(f"   📋 Stock: {stock['stockNumber']} | Max scrape date: {stock['max_scrape_date']}")
            return stock_list
        except Exception as e:
            self.logger.error(f"Error querying stock numbers: {str(e)}")
            print(f"❌ Error querying database: {str(e)}")
            return []

    def build_vehicle_url(self, stock_number):
        return f"https://www.pickles.com.au/used/details/cars/{stock_number}"

    def navigate_to_vehicle_page(self, stock_number):
        """Navigate to vehicle details page"""
        try:
            url = self.build_vehicle_url(stock_number)
            print(f"   🌐 Navigating to: {url}")
            self.browser.page.goto(url, wait_until="domcontentloaded")
            self.browser.page.wait_for_selector("body")
            print(f"   ✅ Page loaded")
            return True
        except Exception as e:
            print(f"   ❌ Navigation error: {str(e)}")
            self.logger.error(f"Navigation error for stock {stock_number}: {str(e)}")
            return False

    def capture_api_response_in_new_tab(self, stock_number, api_url):
        """Open API URL in a new tab and capture the response"""
        try:
            print(f"   🔗 Opening API URL in new tab: {api_url}")

            new_page = self.browser.open_new_tab(api_url)
            time.sleep(2)

            page_source = new_page.content()

            # Extract JSON from page
            json_content = None
            json_match = re.search(r'<pre[^>]*>(.*?)</pre>', page_source, re.DOTALL)
            if json_match:
                json_content = json_match.group(1).strip()
            else:
                # Try to find raw JSON
                json_match = re.search(r'\{.*\}|\[.*\]', page_source, re.DOTALL)
                if json_match:
                    json_content = json_match.group(0)

            new_page.close()

            if json_content and json_content != '<html><head></head><body></body></html>':
                self._save_api_response(stock_number, api_url, json_content)
                print(f"   ✅ API response captured and saved")
                return True
            else:
                print(f"   ⚠️ No valid content found in API response")
                return False

        except Exception as e:
            print(f"   ❌ Error capturing API response: {str(e)}")
            self.logger.error(f"Error capturing API for {api_url}: {str(e)}")
            return False

    def _save_api_response(self, stock_number, api_url, response_content):
        """Save API response to file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stock_{stock_number}_api_response_{timestamp}.json"
            file_path = os.path.join(self.output_folder, filename)

            try:
                json_data = json.loads(response_content)
                formatted = json.dumps(json_data, indent=2, ensure_ascii=False)
            except json.JSONDecodeError:
                formatted = response_content

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(formatted)

            print(f"   💾 Saved to: {filename}")
            self.logger.info(f"API response saved: {file_path}")
        except Exception as e:
            print(f"   ❌ Error saving response: {str(e)}")

    def track_vehicle(self, stock_info):
        """Track a single vehicle"""
        try:
            stock_number = stock_info['stockNumber']
            print(f"\n🚗 Processing Stock Number: {stock_number}")

            if not self.navigate_to_vehicle_page(stock_number):
                return False

            api_url = f"https://www.pickles.com.au/api-website/buyer/ms-bidding-controller/v2/api/bidding/{stock_number}/lot-details/"
            return self.capture_api_response_in_new_tab(stock_number, api_url)
        except Exception as e:
            print(f"   ❌ Error tracking stock {stock_info.get('stockNumber')}: {str(e)}")
            return False

    def run_tracking(self):
        """Main tracking process"""
        try:
            print("🚀 Starting Vehicle Tracking by Stock Number...")
            print("=" * 60)

            if not self.setup_driver():
                return False

            stock_list = self.get_stock_numbers_to_track()
            if not stock_list:
                return False

            print(f"\n🎯 Tracking {len(stock_list)} vehicles...")

            successful = 0
            failed = 0

            for i, stock_info in enumerate(stock_list, 1):
                print(f"\n{'=' * 20} VEHICLE {i}/{len(stock_list)} {'=' * 20}")
                if self.track_vehicle(stock_info):
                    successful += 1
                else:
                    failed += 1

            print(f"\n{'=' * 60}")
            print(f"📊 TRACKING SUMMARY")
            print(f"✅ Tracked: {successful} | ❌ Failed: {failed} | Total: {len(stock_list)}")
            print(f"📁 Files saved to: {self.output_folder}/")

            return successful > 0
        except Exception as e:
            self.logger.error(f"Error in tracking: {str(e)}")
            return False
        finally:
            if self.browser:
                print("\n🔒 Closing browser...")
                self.browser.close()


@with_error_notification()
def main():
    banner = """
╔══════════════════════════════════════════════════════════════╗
║              VEHICLE TRACKER BY STOCK NUMBER                ║
║        Track vehicles using bidding API monitoring          ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    tracker = VehicleTracker()
    success = tracker.run_tracking()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
