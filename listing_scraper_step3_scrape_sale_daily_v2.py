#!/usr/bin/env python3
"""
Step 3: Daily Sale Scraper v2
Scrapes all active sales from pickles_live_schedule where end_sale_date >= current_date
"""

import time
import json
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from pickles_login import PicklesScraper
from logger import get_logger
import requests
from datetime import datetime
from db import MySecondDB

class DailySaleScraperV2:
    """
    Daily scraper for all active auctions using Selenium
    """
    
    def __init__(self):
        """Initialize the scraper."""
        self.logger = get_logger("daily_sale_scraper_v2", log_to_file=True)
        self.db = MySecondDB()
        self.driver = None
        self.wait = None
        self.network_logs = []
        self.output_dir = "json_data_online"
        self.current_page = 1
        self.limit = 120
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 Created directory: {self.output_dir}")
    
    def setup_driver_with_network_logging(self):
        """Setup Chrome driver with network logging enabled."""
        try:
            # Enable logging for network requests
            caps = DesiredCapabilities.CHROME
            caps['goog:loggingPrefs'] = {'performance': 'ALL'}
            
            pickles_scraper = PicklesScraper()
            pickles_scraper.setup_driver()
            self.driver = pickles_scraper.driver
            
            # Enable network logging
            self.driver.execute_cdp_cmd('Network.enable', {})
            
            self.wait = WebDriverWait(self.driver, 30)
            self.logger.info("✅ Chrome driver with network logging setup successful")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False
    
    def read_auction_urls(self):
        """Read auction URLs from database."""
        try:
            print("� Reading auction URLs from database...")
            query = "select * from pickles_live_schedule where end_sale_date >= current_date"
            df = self.db.read_data(query)
            
            if df.empty:
                print("📄 No active auction URLs found in database")
                return []
            
            print(f"📊 Found {len(df)} active auction URLs")
            
            # Extract URLs and create auction names
            auction_data = []
            for index, row in df.iterrows():
                sale_info_url = row['sale_info_url']
                
                # Transform the URL from /auction/saleinfo/ format to /used/search/s/ format
                if '/auction/saleinfo/' in sale_info_url:
                    sale_number = sale_info_url.split('/auction/saleinfo/')[1].rstrip('/')
                    listing_url = f"https://www.pickles.com.au/used/search/s/{sale_number}/"
                    
                    # Extract auction name from URL for file naming
                    # Example: https://www.pickles.com.au/used/search/s/national-online-motor-vehicle-auction/11924
                    url_parts = listing_url.split('/')
                    if len(url_parts) >= 6:
                        auction_name = url_parts[-2]  # Second to last part
                    else:
                        auction_name = f"auction_{index}"
                    
                    auction_data.append({
                        'url': listing_url,
                        'name': auction_name
                    })
                    print(f"✅ Added URL: {listing_url}")
                else:
                    print(f"⚠️ Skipping URL (unexpected format): {sale_info_url}")
            
            print(f"📊 Transformed {len(auction_data)} URLs for scraping")
            return auction_data
            
        except Exception as e:
            self.logger.error(f"Error reading auction URLs from database: {e}")
            print(f"❌ Error reading auction URLs: {e}")
            return []
    
    def pause_for_user(self, message):
        """
        Pause execution and wait for user to press continue
        
        Args:
            message (str): Message to display to user
        """
        print(f"\n⏸️  PAUSE: {message}")
        input("Press Enter to continue...")
        print("▶️  Continuing...\n")
    
    def step1_navigate_to_listing_page(self, listing_url, page=1):
        """Step 1: Navigate directly to the auction listing page with pagination"""
        try:
            # Build URL with pagination parameters
            target_url = f"{listing_url}?page={page}&limit={self.limit}"
            
            print(f"🌐 Step 1: Navigating to auction listing page (Page {page})...")
            print(f"🔗 Target URL: {target_url}")
            
            # Clear performance logs before navigation
            self.driver.get("about:blank")
            time.sleep(1)
            
            # Navigate to the target page with pagination
            self.driver.get(target_url)
            self.logger.info(f"📊 Navigated to: {target_url}")
            
            # Wait for page to load completely
            print("⏱️  Waiting for page to load completely...")
            time.sleep(5)
            
            # Check if page loaded successfully
            current_url = self.driver.current_url
            if "search" in current_url:
                print(f"✅ Successfully loaded auction listing page (Page {page})")
                print(f"📍 Current URL: {current_url}")
                return True
            else:
                print(f"⚠️  Page may have redirected to: {current_url}")
                return True  # Continue anyway as it might still work
            
        except Exception as e:
            self.logger.error(f"❌ Error navigating to page: {e}")
            print(f"❌ Error: {e}")
            return False
    
    def step2_scan_for_post_requests(self):
        """Step 2: Scan for POST requests and print them out (only specific API endpoints)"""
        try:
            print("🔍 Step 2: Scanning for POST requests...")
            print("🎯 Looking specifically for: https://www.pickles.com.au/api-website/buyer/ms-web-asset-search/v2/api/product/public/<sale_id>/search")
            
            # Get performance logs (network requests)
            logs = self.driver.get_log('performance')
            post_requests = []
            all_requests = []
            target_pattern = "/api-website/buyer/ms-web-asset-search/v2/api/product/public/"
            
            print(f"📊 Total network logs captured: {len(logs)}")
            
            for log in logs:
                try:
                    message = json.loads(log['message'])
                    if message['message']['method'] == 'Network.requestWillBeSent':
                        request = message['message']['params']['request']
                        
                        # Track all requests for debugging
                        all_requests.append({
                            'method': request['method'],
                            'url': request['url'],
                            'timestamp': log['timestamp']
                        })
                        
                        # Focus on POST requests with specific URL pattern
                        if request['method'] == 'POST' and target_pattern in request['url'] and '/search' in request['url']:
                            post_info = {
                                'url': request['url'],
                                'method': request['method'],
                                'headers': request.get('headers', {}),
                                'postData': request.get('postData', ''),
                                'timestamp': log['timestamp']
                            }
                            post_requests.append(post_info)
                            
                            print(f"\n🎯 Found TARGET POST request:")
                            print(f"   🔗 URL: {request['url']}")
                            print(f"   🕒 Timestamp: {log['timestamp']}")
                            print(f"   📦 Headers:")
                            for key, value in request.get('headers', {}).items():
                                print(f"      {key}: {value}")
                            
                            if 'postData' in request and request['postData']:
                                print(f"   📝 Post Data: {request['postData']}")
                            else:
                                print(f"   📝 Post Data: (empty)")
                        elif request['method'] == 'POST':
                            print(f"\n⏭️  Skipped POST request (not target pattern): {request['url'][:80]}...")
                            
                except Exception as parse_error:
                    continue
            
            # Show summary of all requests for debugging
            print(f"\n📊 Network Request Summary:")
            print(f"   🌐 Total requests: {len(all_requests)}")
            
            methods_count = {}
            for req in all_requests:
                method = req['method']
                methods_count[method] = methods_count.get(method, 0) + 1
            
            for method, count in methods_count.items():
                print(f"   📋 {method}: {count} requests")
            
            print(f"   🎯 TARGET POST requests: {len(post_requests)}")
            
            if len(post_requests) == 0:
                print(f"\n⚠️  No TARGET POST requests found!")
                print(f"💡 This could mean:")
                print(f"   • The page loads data via GET requests")
                print(f"   • POST requests happen later (after user interaction)")
                print(f"   • The specific API endpoint is not being called")
                
                # Show some sample POST requests for debugging
                sample_posts = [req for req in all_requests if req['method'] == 'POST']
                if sample_posts:
                    print(f"\n🔍 Other POST requests found:")
                    for i, req in enumerate(sample_posts[:3]):
                        print(f"   {i+1}. {req['url'][:80]}...")
            
            self.logger.info(f"✅ Found {len(post_requests)} TARGET POST requests out of {len(all_requests)} total requests")
            
            return post_requests
            
        except Exception as e:
            self.logger.error(f"❌ Error scanning for POST requests: {e}")
            print(f"❌ Error: {e}")
            return []
    
    def step4_find_next_page_button(self):
        """Step 4: Find the next page button"""
        try:
            print("🔍 Step 4: Looking for next page button...")
            print("🎯 Searching for button with ID 'ps-ch-right-btn' or similar pagination buttons...")
            
            # Try multiple selectors for next page button
            selectors = [
                "#ps-ch-right-btn",  # Specific ID mentioned
                "button[id*='right-btn']",  # Buttons with 'right-btn' in ID
                "button[class*='nav-icon']",  # Buttons with nav-icon class
                "button:has(.pds-icon-chevron--right)",  # Buttons containing right chevron icon
                ".pds-icon-chevron--right",  # The icon itself
                "button[aria-label*='next']",  # Buttons with 'next' in aria-label
                "button[aria-label*='Next']",  # Buttons with 'Next' in aria-label
            ]
            
            next_button = None
            
            for selector in selectors:
                try:
                    # Find button using current selector
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if buttons:
                        for button in buttons:
                            # Check if button is visible and enabled
                            if button.is_displayed() and button.is_enabled():
                                button_id = button.get_attribute('id')
                                button_class = button.get_attribute('class')
                                button_text = button.text
                                aria_label = button.get_attribute('aria-label')
                                
                                print(f"   ✅ Found potential next button:")
                                print(f"      🔗 Selector: {selector}")
                                print(f"      🆔 ID: {button_id}")
                                print(f"      📝 Class: {button_class}")
                                print(f"      📄 Text: '{button_text}'")
                                print(f"      🏷️  Aria-label: {aria_label}")
                                
                                # Prefer the specific ID if found
                                if button_id == 'ps-ch-right-btn':
                                    next_button = button
                                    print(f"      🎯 PERFECT MATCH - Using this button!")
                                    break
                                elif not next_button:  # Take first valid button if no perfect match
                                    next_button = button
                                    print(f"      ✅ Will use this button if no better match found")
                        
                        if next_button and next_button.get_attribute('id') == 'ps-ch-right-btn':
                            break  # Found perfect match, stop searching
                            
                except Exception as selector_error:
                    print(f"   ⚠️  Selector '{selector}' failed: {selector_error}")
                    continue
            
            if next_button:
                print(f"\n✅ Selected next page button:")
                print(f"   🆔 ID: {next_button.get_attribute('id')}")
                print(f"   📝 Class: {next_button.get_attribute('class')}")
                print(f"   📄 Text: '{next_button.text}'")
                return next_button
            else:
                print(f"\n❌ No next page button found!")
                print(f"💡 This could mean:")
                print(f"   • This is the last page")
                print(f"   • Button is not loaded yet")
                print(f"   • Button has different selectors than expected")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error finding next page button: {e}")
            print(f"❌ Error: {e}")
            return None
    
    def step5_click_next_button_and_scan(self, next_button, page_number):
        """Step 5: Click next button and scan for new POST requests"""
        try:
            print(f"\n▶️  Step 5: Clicking next page button (Page {page_number})...")
            
            # Clear performance logs before clicking
            self.driver.get_log('performance')
            
            # Click the next button
            next_button.click()
            self.logger.info(f"✅ Clicked next page button for page {page_number}")
            print("✅ Next button clicked successfully")
            
            # Wait for page to load
            print("⏱️  Waiting for new page to load...")
            time.sleep(3)
            
            # Scan for new POST requests
            print(f"🔍 Scanning for POST requests on page {page_number}...")
            post_requests = self.step2_scan_for_post_requests()
            
            return post_requests
            
        except Exception as e:
            self.logger.error(f"❌ Error clicking next button: {e}")
            print(f"❌ Error: {e}")
            return []
    
    def step3_call_post_and_save(self, post_requests, page, auction_name):
        """Step 3: Call POST requests and save JSON responses"""
        try:
            if not post_requests:
                print("⚠️  No POST requests found to call")
                return None
            
            print(f"📞 Step 3: Calling POST request for Page {page}...")
            
            session = requests.Session()
            
            # Copy cookies from browser to requests session
            browser_cookies = self.driver.get_cookies()
            print(f"🍪 Copying {len(browser_cookies)} cookies from browser to session")
            
            for cookie in browser_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Process first POST request
            post_request = post_requests[0]  # Take the first target POST request
            
            try:
                print(f"\n📞 Calling POST request for Page {page}...")
                print(f"   🔗 URL: {post_request['url']}")
                
                # Prepare headers
                headers = post_request['headers'].copy()
                
                # Add some standard headers if missing
                if 'User-Agent' not in headers:
                    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                
                # Prepare data
                post_data = post_request.get('postData', '')
                
                print(f"   📦 Headers: {len(headers)} headers")
                print(f"   📝 Post Data Length: {len(post_data)} chars")
                
                # Make the POST request (URL already has pagination from browser navigation)
                if post_data:
                    content_type = headers.get('content-type', '').lower()
                    if 'application/json' in content_type:
                        try:
                            json_data = json.loads(post_data)
                            print(f"   📤 Sending JSON data")
                            response = session.post(
                                post_request['url'],
                                json=json_data,
                                headers=headers,
                                timeout=30
                            )
                        except json.JSONDecodeError:
                            print(f"   📤 Sending form data")
                            response = session.post(
                                post_request['url'],
                                data=post_data,
                                headers=headers,
                                timeout=30
                            )
                    else:
                        print(f"   📤 Sending form data")
                        response = session.post(
                            post_request['url'],
                            data=post_data,
                            headers=headers,
                            timeout=30
                        )
                else:
                    print(f"   📤 Sending empty POST")
                    response = session.post(
                        post_request['url'],
                        headers=headers,
                        timeout=30
                    )
                
                print(f"   📊 Response Status: {response.status_code}")
                print(f"   📏 Response Length: {len(response.text)} chars")
                
                if response.status_code == 200:
                    try:
                        response_json = response.json()
                        
                        # Save to file in json_data_online directory with auction name
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        safe_auction_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in auction_name[:50])
                        filename = f"{safe_auction_name}_page{page}_{timestamp}.json"
                        file_path = os.path.join(self.output_dir, filename)
                        
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(response_json, f, indent=2, ensure_ascii=False)
                        
                        print(f"   💾 ✅ Saved JSON response to: {file_path}")
                        
                        # Analyze the response
                        if isinstance(response_json, dict):
                            odata_count = response_json.get('@odata.count', 0)
                            value_items = response_json.get('value', [])
                            items_in_page = len(value_items)
                            
                            print(f"   🔍 Response Analysis:")
                            print(f"      📊 @odata.count (total): {odata_count}")
                            print(f"      📦 Items in this page: {items_in_page}")
                            print(f"      🔑 Top level keys: {list(response_json.keys())}")
                            
                            self.logger.info(f"✅ Saved POST response page {page} to {file_path}")
                            
                            # Return analysis data
                            return {
                                'filename': file_path,
                                'odata_count': odata_count,
                                'items_count': items_in_page,
                                'response_json': response_json
                            }
                        else:
                            print(f"   🔍 Response is a list with {len(response_json)} items")
                            return {
                                'filename': file_path,
                                'odata_count': len(response_json),
                                'items_count': len(response_json),
                                'response_json': response_json
                            }
                        
                    except json.JSONDecodeError:
                        # Save as text if not JSON
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        safe_auction_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in auction_name[:50])
                        filename = f"{safe_auction_name}_page{page}_{timestamp}.txt"
                        file_path = os.path.join(self.output_dir, filename)
                        
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        
                        print(f"   💾 ⚠️  Saved text response to: {file_path}")
                        print(f"      (Response was not valid JSON)")
                        return None
                else:
                    print(f"   ❌ Request failed with status: {response.status_code}")
                    print(f"   📄 Response: {response.text[:200]}...")
                    
                    # Save error response too
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_auction_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in auction_name[:50])
                    filename = f"{safe_auction_name}_error_page{page}_{response.status_code}_{timestamp}.txt"
                    file_path = os.path.join(self.output_dir, filename)
                    
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(f"Status: {response.status_code}\n")
                        f.write(f"Headers: {dict(response.headers)}\n\n")
                        f.write(response.text)
                    
                    print(f"   💾 Saved error response to: {file_path}")
                    return None
                    
            except Exception as req_error:
                print(f"   ❌ Error calling POST request for page {page}: {req_error}")
                self.logger.error(f"❌ Error calling POST request page {page}: {req_error}")
                return None
            
        except Exception as e:
            self.logger.error(f"❌ Error in POST requests: {e}")
            print(f"❌ Error: {e}")
            return None
    
    def process_single_auction(self, auction_data):
        """Process a single auction URL with pagination"""
        try:
            listing_url = auction_data['listing_url']
            auction_name = auction_data['auction_name']
            index = auction_data['index']
            
            print(f"\n🎯 Processing Auction {index}: {auction_name}")
            print(f"🔗 URL: {listing_url}")
            
            # Initialize pagination tracking for this auction
            page = 1
            total_items_collected = 0
            
            while True:
                print(f"\n{'='*15} AUCTION {index} - PAGE {page} {'='*15}")
                
                # Step 1: Navigate to listing page with pagination
                if not self.step1_navigate_to_listing_page(listing_url, page):
                    print(f"❌ Failed to navigate to auction {index} page {page}")
                    break
                
                # Step 2: Scan for POST requests
                post_requests = self.step2_scan_for_post_requests()
                
                if not post_requests:
                    print(f"❌ No POST requests found for auction {index} page {page}")
                    break
                
                # Step 3: Call POST and save response
                result = self.step3_call_post_and_save(post_requests, page, auction_name)
                
                if not result:
                    print(f"❌ Failed to get response for auction {index} page {page}")
                    break
                
                # Analyze results
                odata_count = result['odata_count']
                items_in_page = result['items_count']
                total_items_collected += items_in_page
                
                print(f"\n📊 Auction {index} Page {page} Summary:")
                print(f"   📋 Total available: {odata_count}")
                print(f"   📦 Items in this page: {items_in_page}")
                print(f"   📈 Total collected so far: {total_items_collected}")
                print(f"   💾 Saved to: {result['filename']}")
                
                # Check if we need more pages
                items_per_page = self.limit
                
                if total_items_collected < odata_count:
                    print(f"\n🔄 More pages needed for auction {index}!")
                    print(f"   📊 Total available: {odata_count}")
                    print(f"   📈 Items collected so far: {total_items_collected}")
                    print(f"   📋 Remaining items: {odata_count - total_items_collected}")
                    
                    page += 1
                    print(f"   ➡️  Moving to page {page}...")
                    continue
                else:
                    print(f"\n✅ All pages collected for auction {index}!")
                    print(f"   📊 Total items: {odata_count}")
                    print(f"   📈 Items collected: {total_items_collected}")
                    print(f"   📄 Pages processed: {page}")
                    break
            
            return {
                'auction_name': auction_name,
                'total_pages': page,
                'total_items': total_items_collected,
                'success': True
            }
            
        except Exception as e:
            print(f"❌ Error processing auction {index}: {e}")
            self.logger.error(f"Error processing auction {index}: {e}")
            return {
                'auction_name': auction_name,
                'success': False,
                'error': str(e)
            }
    
    def run_scraper(self):
        """Run the complete scraping process for all auction URLs"""
        try:
            print("🚀 Starting Daily Sale Scraper V2...")
            print("📋 This scraper will:")
            print("   1. Read auction URLs from database")
            print("   2. For each URL, navigate with pagination (?page=N&limit=120)")
            print("   3. Scan for target POST requests (/search endpoints)")
            print("   4. Call POST requests and save responses to json_data_online/")
            print("   5. Process pagination for each auction")
            print("=" * 60)
            
            # Read auction URLs from database
            auction_list = self.read_auction_urls()
            if not auction_list:
                print("❌ No auction URLs found. Exiting.")
                return
            
            # Setup driver
            if not self.setup_driver_with_network_logging():
                return
            
            # Process each auction
            results = []
            total_auctions = len(auction_list)
            
            print(f"\n🎯 Processing {total_auctions} auctions...")
            
            for auction_data in auction_list:
                result = self.process_single_auction(auction_data)
                results.append(result)
                
                # Pause between auctions to avoid overwhelming the server
                if auction_data['index'] < total_auctions:
                    print(f"\n⏱️  Waiting 3 seconds before next auction...")
                    time.sleep(3)
            
            # Final summary
            print("\n" + "=" * 60)
            print("🎉 All Auctions Processing Complete!")
            
            successful_auctions = [r for r in results if r['success']]
            failed_auctions = [r for r in results if not r['success']]
            
            print(f"📊 Summary:")
            print(f"   📋 Total auctions processed: {total_auctions}")
            print(f"   ✅ Successful auctions: {len(successful_auctions)}")
            print(f"   ❌ Failed auctions: {len(failed_auctions)}")
            
            total_pages = sum(r.get('total_pages', 0) for r in successful_auctions)
            total_items = sum(r.get('total_items', 0) for r in successful_auctions)
            
            print(f"   📄 Total pages processed: {total_pages}")
            print(f"   📦 Total items collected: {total_items}")
            print(f"   📁 JSON files saved to: {self.output_dir}/")
            
            if failed_auctions:
                print(f"\n❌ Failed auctions:")
                for failed in failed_auctions:
                    print(f"   • {failed['auction_name']}: {failed.get('error', 'Unknown error')}")
            
            print("\n✅ Online Scraper Step 2 complete!")
            
        except Exception as e:
            self.logger.error(f"❌ Error in main process: {e}")
            print(f"❌ Error: {e}")
        finally:
            if self.driver:
                print("\n🔒 Closing browser...")
                self.driver.quit()

def main():
    """Main function to run the scraper"""
    print("🔍 Daily Sale Scraper V2")
    print("Database-driven batch scraper for active auctions")
    print("=" * 60)
    
    scraper = DailySaleScraperV2()
    scraper.run_scraper()

if __name__ == "__main__":
    main()