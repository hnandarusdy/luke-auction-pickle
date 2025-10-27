#!/usr/bin/env python3
"""
Online Scraper Step 2: Direct auction listing page POST request scanner
Scrapes a single URL provided as command-line parameter
Usage: python listing_scraper_step2_scrape_one_url.py <url>
"""

import time
import json
import os
import sys
import logging
import re
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

class OnlineScraperStep2:
    """
    Direct POST request scanner for specific auction listing page
    """
    
    def __init__(self, target_url=None):
        """Initialize the scraper."""
        self.logger = get_logger("online_scraper_step2", log_to_file=True)
        self.driver = None
        self.wait = None
        self.network_logs = []
        self.target_url = target_url
        
        # Use script directory for output directory (fix for Task Scheduler)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "json_data_online")
        self.current_page = 1
        self.limit = 1000
        
        # Setup custom log file for this execution
        self.custom_logger = self.setup_custom_logging(target_url)
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"📁 Created directory: {self.output_dir}")
            self.custom_logger.info(f"Created directory: {self.output_dir}")

    def setup_custom_logging(self, target_url):
        """Setup custom logging for this specific execution"""
        try:
            # Extract sale ID from URL for log filename
            sale_id = "unknown"
            if target_url:
                # Extract sale ID from URL like: .../national-online-motor-vehicle-auction/11924
                url_parts = target_url.split('/')
                if len(url_parts) >= 2:
                    sale_id = url_parts[-1]  # Get the last part (sale ID)
            
            # Create log directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(script_dir, "logs_listing_task_scheduler")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # Create log filename with format: <sale_id>_<date>_log.txt
            current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"{sale_id}_{current_date}_log.txt"
            log_filepath = os.path.join(log_dir, log_filename)
            
            # Create custom logger
            custom_logger = logging.getLogger(f"auction_scraper_{sale_id}")
            custom_logger.setLevel(logging.INFO)
            
            # Remove existing handlers to avoid duplicates
            custom_logger.handlers = []
            
            # Create file handler
            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            
            # Add handler to logger
            custom_logger.addHandler(file_handler)
            
            # Log the startup
            custom_logger.info("="*60)
            custom_logger.info(f"AUCTION SCRAPER EXECUTION STARTED")
            custom_logger.info(f"Sale ID: {sale_id}")
            custom_logger.info(f"Target URL: {target_url}")
            custom_logger.info(f"Log file: {log_filepath}")
            custom_logger.info("="*60)
            
            print(f"📝 Custom log file created: {log_filepath}")
            
            return custom_logger
            
        except Exception as e:
            print(f"⚠️ Warning: Could not setup custom logging: {e}")
            # Return a dummy logger that doesn't do anything
            dummy_logger = logging.getLogger("dummy")
            dummy_logger.addHandler(logging.NullHandler())
            return dummy_logger
    
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
    
    def parse_auction_data_from_url(self, url):
        """Parse auction data from a single URL"""
        try:
            if not url:
                return None
            
            print(f"� Parsing URL: {url}")
            
            # Extract auction name and sale ID from URL for file naming
            # Example: https://www.pickles.com.au/used/search/s/national-online-motor-vehicle-auction/11924
            url_parts = url.split('/')
            if len(url_parts) >= 6:
                auction_name = url_parts[-2]  # Get the auction name part
                sale_id = url_parts[-1]       # Get the sale ID
                full_name = f"{sale_id}_{auction_name}"  # Use sale_id as prefix
            else:
                # Fallback naming
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
            
            print(f"✅ Parsed auction data:")
            print(f"   📋 Name: {full_name}")
            print(f"   🆔 Sale ID: {sale_id}")
            
            return auction_data
            
        except Exception as e:
            print(f"❌ Error parsing URL: {e}")
            self.logger.error(f"Error parsing URL: {e}")
            return None
    
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
                
                # Make the POST request with modified JSON for pagination
                if post_data:
                    content_type = headers.get('content-type', '').lower()
                    if 'application/json' in content_type:
                        try:
                            json_data = json.loads(post_data)
                            
                            # Modify JSON data for pagination (Azure Cognitive Search uses skip/top in body)
                            if page > 1:
                                skip_value = (page - 1) * self.limit
                                json_data['skip'] = skip_value
                                json_data['top'] = self.limit
                                print(f"   🔄 Modified JSON for page {page}: skip={skip_value}, top={self.limit}")
                            else:
                                # Ensure top is set for page 1
                                if 'top' not in json_data:
                                    json_data['top'] = self.limit
                                print(f"   📤 Page 1: top={self.limit}")
                            
                            print(f"   📤 Sending JSON data with pagination")
                            response = session.post(
                                post_request['url'],  # Use original URL
                                json=json_data,
                                headers=headers,
                                timeout=30
                            )
                        except json.JSONDecodeError:
                            print(f"   📤 Sending form data")
                            response = session.post(
                                post_request['url'],  # Use original URL
                                data=post_data,
                                headers=headers,
                                timeout=30
                            )
                    else:
                        print(f"   📤 Sending form data")
                        response = session.post(
                            post_request['url'],  # Use original URL
                            data=post_data,
                            headers=headers,
                            timeout=30
                        )
                else:
                    print(f"   📤 Sending empty POST")
                    response = session.post(
                        post_request['url'],  # Use original URL
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
    
    def process_auction(self, auction_data):
        """Process a single auction URL with pagination"""
        try:
            listing_url = auction_data['listing_url']
            auction_name = auction_data['auction_name']
            index = auction_data['index']
            
            print(f"\n🎯 Processing Auction {index}: {auction_name}")
            print(f"🔗 URL: {listing_url}")
            
            # Initialize pagination tracking for this auction
            page = 2  # Skip page 1, start directly from page 2
            total_items_collected = 1000  # Assume page 1 had 1000 items (standard page size)
            
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
    
    def run_single_url_scraper(self):
        """Run the scraper for a single URL provided via command line"""
        try:
            print("🚀 Starting Single URL Scraper...")
            print(f"🎯 Target URL: {self.target_url}")
            print("📋 This scraper will:")
            print("   1. Process the provided auction URL")
            print("   2. Navigate with pagination (?page=N&limit=1000) - STARTING FROM PAGE 2")
            print("   3. Scan for target POST requests (/search endpoints)")
            print("   4. Call POST requests and save responses to json_data_online/")
            print("   5. Process pagination for the auction (skipping page 1)")
            print("=" * 60)
            
            # Log the start
            self.custom_logger.info("Starting Single URL Scraper execution")
            self.custom_logger.info(f"Target URL: {self.target_url}")
            
            # Setup driver
            self.custom_logger.info("Setting up Chrome driver with network logging...")
            if not self.setup_driver_with_network_logging():
                self.custom_logger.error("Failed to setup Chrome driver")
                return
            self.custom_logger.info("Chrome driver setup successful")
            
            # Process the single auction
            self.custom_logger.info("Parsing auction data from URL...")
            auction_data = self.parse_auction_data_from_url(self.target_url)
            if not auction_data:
                print("❌ Failed to parse auction data from URL")
                self.custom_logger.error("Failed to parse auction data from URL")
                return
            
            self.custom_logger.info(f"Auction data parsed successfully: {auction_data['auction_name']}")
            self.custom_logger.info("Starting auction processing...")
                
            result = self.process_auction(auction_data)
            
            if not result:
                print("❌ Failed to process auction")
                self.custom_logger.error("Failed to process auction")
                return
            
            # Final summary
            print("\n" + "=" * 60)
            print("🎉 Single URL Processing Complete!")
            
            if result.get('success'):
                print(f"📊 Summary:")
                print(f"   ✅ Auction processed successfully")
                print(f"   📋 Auction name: {result['auction_name']}")
                print(f"   📄 Total pages processed: {result['total_pages']}")
                print(f"   📦 Total items collected: {result['total_items']}")
                print(f"   📁 JSON files saved to: {self.output_dir}/")
                
                # Log success details
                self.custom_logger.info("="*40)
                self.custom_logger.info("AUCTION PROCESSING COMPLETED SUCCESSFULLY")
                self.custom_logger.info(f"Auction name: {result['auction_name']}")
                self.custom_logger.info(f"Total pages processed: {result['total_pages']}")
                self.custom_logger.info(f"Total items collected: {result['total_items']}")
                self.custom_logger.info(f"JSON files saved to: {self.output_dir}/")
                self.custom_logger.info("="*40)
            else:
                print(f"❌ Processing failed: {result.get('error', 'Unknown error')}")
                self.custom_logger.error(f"Processing failed: {result.get('error', 'Unknown error')}")
            
            print("\n✅ Single URL Scraper complete!")
            self.custom_logger.info("Single URL Scraper execution completed")
            
        except Exception as e:
            self.logger.error(f"❌ Error in single URL process: {e}")
            self.custom_logger.error(f"Critical error in single URL process: {e}")
            print(f"❌ Error: {e}")
        finally:
            if self.driver:
                print("\n🔒 Closing browser...")
                self.custom_logger.info("Closing browser and cleaning up")
                self.driver.quit()
            
            # Final log entry
            self.custom_logger.info("="*60)
            self.custom_logger.info("AUCTION SCRAPER EXECUTION FINISHED")
            self.custom_logger.info("="*60)

def main():
    """Main function to run the scraper with command-line URL parameter"""
    try:
        print("🔍 Single URL Auction Scraper")
        print("Direct POST request scanner for a single auction listing page")
        print("=" * 60)
        
        # Check command-line arguments
        if len(sys.argv) != 2:
            print("❌ Usage: python listing_scraper_step2_scrape_one_url.py <auction_url>")
            print("\n📋 Example:")
            print("   python listing_scraper_step2_scrape_one_url.py https://www.pickles.com.au/used/search/s/national-online-motor-vehicle-auction/11924")
            print("\n💡 Note: The URL should be a Pickles auction search page URL")
            sys.exit(1)
        
        target_url = sys.argv[1]
        
        # Basic URL validation
        if not target_url.startswith(('http://', 'https://')):
            print("❌ Error: URL must start with http:// or https://")
            sys.exit(1)
        
        if 'pickles.com.au' not in target_url:
            print("❌ Error: URL must be from pickles.com.au domain")
            sys.exit(1)
        
        print(f"✅ Target URL validated: {target_url}")
        
        # Create and run scraper
        scraper = OnlineScraperStep2(target_url=target_url)
        scraper.run_single_url_scraper()
        
        print("\n🎉 Script completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Critical Error: {str(e)}")
        print("\n🔍 Full error details:")
        import traceback
        traceback.print_exc()
        print("\n💡 Common issues:")
        print("   • Check if Chrome browser is installed")
        print("   • Ensure all Python packages are installed: pip install selenium requests")
        print("   • Verify the URL format is correct")
        print("   • Check internet connection")
        
    finally:
        # Always pause to show results
        pass 
        # input("\n🔄 Press Enter to exit...")

if __name__ == "__main__":
    main()
    