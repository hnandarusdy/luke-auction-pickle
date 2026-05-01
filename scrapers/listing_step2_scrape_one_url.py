#!/usr/bin/env python3
"""
Online Scraper Step 2: Direct auction listing page POST request scanner
Scrapes a single URL provided as command-line parameter.
Uses Playwright for browser automation and network interception.

Usage: python listing_step2_scrape_one_url.py <url>
"""

import time
import json
import os
import sys
import logging
import re
import requests
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.browser import PicklesBrowser
from utils.logger import get_logger
from scrapers.listing_step4_sale_info_to_db import main as sale_info_to_db_main
from utils.whatsapp_notifier import with_error_notification


class OnlineScraperStep2:
    """Direct POST request scanner for specific auction listing page"""

    def __init__(self, target_url=None):
        self.logger = get_logger("online_scraper_step2", log_to_file=True)
        self.browser = None
        self.target_url = target_url
        self.captured_post_requests = []

        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "..", "json_data_online")
        self.output_dir = os.path.abspath(self.output_dir)
        self.current_page = 1
        self.limit = 1000

        self.custom_logger = self._setup_custom_logging(target_url)

        os.makedirs(self.output_dir, exist_ok=True)

    def _setup_custom_logging(self, target_url):
        """Setup custom logging for this specific execution"""
        try:
            sale_id = "unknown"
            if target_url:
                url_parts = target_url.split('/')
                if len(url_parts) >= 2:
                    sale_id = url_parts[-1]

            script_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(script_dir, "..", "logs_listing_task_scheduler")
            log_dir = os.path.abspath(log_dir)
            os.makedirs(log_dir, exist_ok=True)

            current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"{sale_id}_{current_date}_log.txt"
            log_filepath = os.path.join(log_dir, log_filename)

            custom_logger = logging.getLogger(f"auction_scraper_{sale_id}")
            custom_logger.setLevel(logging.INFO)
            custom_logger.handlers = []

            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(formatter)
            custom_logger.addHandler(file_handler)

            custom_logger.info("=" * 60)
            custom_logger.info("AUCTION SCRAPER EXECUTION STARTED")
            custom_logger.info(f"Sale ID: {sale_id}")
            custom_logger.info(f"Target URL: {target_url}")
            custom_logger.info("=" * 60)

            print(f"📝 Custom log file created: {log_filepath}")
            return custom_logger
        except Exception as e:
            print(f"⚠️ Warning: Could not setup custom logging: {e}")
            dummy = logging.getLogger("dummy")
            dummy.addHandler(logging.NullHandler())
            return dummy

    def setup_driver_with_network_logging(self):
        """Setup Playwright browser with network logging enabled."""
        try:
            self.browser = PicklesBrowser(enable_network_logging=True)
            self.browser.setup()
            self.logger.info("✅ Playwright browser with network logging setup successful")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to setup browser: {e}")
            return False

    def parse_auction_data_from_url(self, url):
        """Parse auction data from a single URL"""
        try:
            if not url:
                return None

            print(f"📋 Parsing URL: {url}")
            url_parts = url.split('/')
            if len(url_parts) >= 6:
                auction_name = url_parts[-2]
                sale_id = url_parts[-1]
                full_name = f"{sale_id}_{auction_name}"
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                full_name = f"auction_{timestamp}"
                sale_id = "unknown"

            auction_data = {
                'index': 1,
                'listing_url': url,
                'auction_name': full_name,
                'sale_id': sale_id,
                'sale_info_url': ''
            }
            print(f"✅ Parsed: {full_name} (Sale ID: {sale_id})")
            return auction_data
        except Exception as e:
            print(f"❌ Error parsing URL: {e}")
            return None

    def step1_navigate_to_listing_page(self, listing_url, page=1):
        """Navigate directly to the auction listing page with pagination"""
        try:
            target_url = f"{listing_url}?page={page}&limit={self.limit}"
            print(f"🌐 Navigating to page {page}: {target_url}")

            # Clear captured requests before navigation
            self.browser.clear_captured_requests()

            # Navigate to blank first to reset
            self.browser.page.goto("about:blank")
            time.sleep(1)

            # Navigate to target
            self.browser.page.goto(target_url, wait_until="domcontentloaded")
            print("⏱️  Waiting for page to load...")
            time.sleep(5)

            current_url = self.browser.page.url
            print(f"✅ Loaded page {page} - URL: {current_url}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error navigating to page: {e}")
            print(f"❌ Error: {e}")
            return False

    def step2_scan_for_post_requests(self):
        """Scan captured network requests for target POST requests"""
        try:
            print("🔍 Scanning for POST requests...")
            target_pattern = "/api-website/buyer/ms-web-asset-search/v2/api/product/public/"

            post_requests = self.browser.get_captured_post_requests(url_pattern=target_pattern)

            # Filter for /search endpoint
            filtered = [r for r in post_requests if '/search' in r['url']]

            print(f"📊 Found {len(filtered)} target POST requests")

            for req in filtered:
                print(f"   🎯 URL: {req['url']}")
                if req.get('post_data'):
                    print(f"   📝 Post Data Length: {len(req['post_data'])} chars")

            if not filtered:
                all_posts = self.browser.get_captured_post_requests()
                print(f"⚠️ No target POSTs found. Total POSTs captured: {len(all_posts)}")
                if all_posts:
                    for r in all_posts[:3]:
                        print(f"   ⏭️ Other POST: {r['url'][:80]}...")

            return filtered
        except Exception as e:
            self.logger.error(f"❌ Error scanning for POST requests: {e}")
            return []

    def step3_call_post_and_save(self, post_requests, page, auction_name):
        """Call POST requests and save JSON responses"""
        try:
            if not post_requests:
                print("⚠️ No POST requests to call")
                return None

            print(f"📞 Calling POST request for Page {page}...")

            session = requests.Session()

            # Copy cookies from browser
            cookies_dict = self.browser.get_cookies_dict()
            print(f"🍪 Copying {len(cookies_dict)} cookies from browser")
            session.cookies.update(cookies_dict)

            post_request = post_requests[0]

            headers = post_request['headers'].copy()
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

            post_data = post_request.get('post_data', '')

            if post_data:
                content_type = headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    try:
                        json_data = json.loads(post_data)

                        if page > 1:
                            skip_value = (page - 1) * self.limit
                            json_data['skip'] = skip_value
                            json_data['top'] = self.limit
                            print(f"   🔄 Modified JSON for page {page}: skip={skip_value}, top={self.limit}")
                        else:
                            if 'top' not in json_data:
                                json_data['top'] = self.limit

                        response = session.post(post_request['url'], json=json_data, headers=headers, timeout=30)
                    except json.JSONDecodeError:
                        response = session.post(post_request['url'], data=post_data, headers=headers, timeout=30)
                else:
                    response = session.post(post_request['url'], data=post_data, headers=headers, timeout=30)
            else:
                response = session.post(post_request['url'], headers=headers, timeout=30)

            print(f"   📊 Response Status: {response.status_code}")
            print(f"   📏 Response Length: {len(response.text)} chars")

            if response.status_code == 200:
                try:
                    response_json = response.json()

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in auction_name[:50])
                    filename = f"{safe_name}_page{page}_{timestamp}.json"
                    file_path = os.path.join(self.output_dir, filename)

                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(response_json, f, indent=2, ensure_ascii=False)

                    print(f"   💾 ✅ Saved: {file_path}")

                    if isinstance(response_json, dict):
                        odata_count = response_json.get('@odata.count', 0)
                        value_items = response_json.get('value', [])
                        items_in_page = len(value_items)
                        print(f"   📊 @odata.count: {odata_count}, Items: {items_in_page}")
                        return {
                            'filename': file_path,
                            'odata_count': odata_count,
                            'items_count': items_in_page,
                            'response_json': response_json
                        }
                    else:
                        return {
                            'filename': file_path,
                            'odata_count': len(response_json),
                            'items_count': len(response_json),
                            'response_json': response_json
                        }
                except json.JSONDecodeError:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in auction_name[:50])
                    filename = f"{safe_name}_page{page}_{timestamp}.txt"
                    file_path = os.path.join(self.output_dir, filename)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    print(f"   ⚠️ Saved text (not JSON): {file_path}")
                    return None
            else:
                print(f"   ❌ Request failed: {response.status_code}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error in POST requests: {e}")
            print(f"❌ Error: {e}")
            return None

    def process_auction(self, auction_data):
        """Process a single auction URL with pagination"""
        try:
            listing_url = auction_data['listing_url']
            auction_name = auction_data['auction_name']
            index = auction_data['index']

            print(f"\n🎯 Processing Auction {index}: {auction_name}")
            print(f"🔗 URL: {listing_url}")

            page = 2  # Start from page 2 (page 1 assumed already collected)
            total_items_collected = 1000

            while True:
                print(f"\n{'=' * 15} PAGE {page} {'=' * 15}")

                if not self.step1_navigate_to_listing_page(listing_url, page):
                    break

                post_requests = self.step2_scan_for_post_requests()
                if not post_requests:
                    print(f"❌ No POST requests found for page {page}")
                    break

                result = self.step3_call_post_and_save(post_requests, page, auction_name)
                if not result:
                    break

                odata_count = result['odata_count']
                items_in_page = result['items_count']
                total_items_collected += items_in_page

                print(f"📊 Page {page}: {items_in_page} items | Total: {total_items_collected}/{odata_count}")

                if total_items_collected < odata_count:
                    page += 1
                    continue
                else:
                    print(f"✅ All pages collected! Total: {total_items_collected}")
                    break

            return {
                'auction_name': auction_name,
                'total_pages': page,
                'total_items': total_items_collected,
                'success': True
            }
        except Exception as e:
            print(f"❌ Error processing auction: {e}")
            self.logger.error(f"Error processing auction: {e}")
            return {'auction_name': auction_data.get('auction_name', ''), 'success': False, 'error': str(e)}

    def run_single_url_scraper(self):
        """Run the scraper for a single URL"""
        try:
            print("🚀 Starting Single URL Scraper (Playwright)...")
            print(f"🎯 Target URL: {self.target_url}")
            print("=" * 60)

            self.custom_logger.info("Starting Single URL Scraper execution")

            if not self.setup_driver_with_network_logging():
                self.custom_logger.error("Failed to setup browser")
                return

            auction_data = self.parse_auction_data_from_url(self.target_url)
            if not auction_data:
                self.custom_logger.error("Failed to parse auction data from URL")
                return

            self.custom_logger.info(f"Processing: {auction_data['auction_name']}")
            result = self.process_auction(auction_data)

            print("\n" + "=" * 60)
            if result.get('success'):
                print(f"🎉 Complete! Pages: {result['total_pages']}, Items: {result['total_items']}")
                self.custom_logger.info(f"SUCCESS: {result['total_items']} items across {result['total_pages']} pages")
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown')}")
                self.custom_logger.error(f"FAILED: {result.get('error')}")

        except Exception as e:
            self.logger.error(f"❌ Error: {e}")
            self.custom_logger.error(f"Critical error: {e}")
        finally:
            if self.browser:
                print("🔒 Closing browser...")
                self.browser.close()
            self.custom_logger.info("EXECUTION FINISHED")


@with_error_notification()
def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("❌ Usage: python listing_step2_scrape_one_url.py <auction_url>")
        sys.exit(1)

    target_url = sys.argv[1]

    if not target_url.startswith(('http://', 'https://')):
        print("❌ Error: URL must start with http:// or https://")
        sys.exit(1)

    if 'pickles.com.au' not in target_url:
        print("❌ Error: URL must be from pickles.com.au domain")
        sys.exit(1)

    scraper = OnlineScraperStep2(target_url=target_url)
    scraper.run_single_url_scraper()


if __name__ == "__main__":
    main()
    time.sleep(10)
    sale_info_to_db_main()
