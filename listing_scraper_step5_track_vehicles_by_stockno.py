#!/usr/bin/env python3
"""
Step 5: Track Vehicles by Stock Number

This script tracks vehicles that are still available for scraping by:
1. Querying stock numbers from database where max scrape date > current date
2. Opening each vehicle's details page on Pickles website
3. Monitoring network calls for bidding API endpoints
4. Saving API responses to vehicles_tracking folder

Author: GitHub Copilot
Date: October 29, 2025
"""

import os
import json
import time
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    print("⚠️ webdriver-manager not available. Will try system ChromeDriver.")
from db import MySecondDB
from logger import get_logger
from whatsapp_notifier import with_error_notification


class VehicleTracker:
    """
    Track vehicles by stock number and capture bidding API responses
    """
    
    def __init__(self):
        """Initialize the vehicle tracker"""
        self.db = MySecondDB()
        self.logger = get_logger("vehicle_tracker", log_to_file=True)
        self.driver = None
        self.output_folder = "vehicles_tracking"
        self.create_output_folder()
        
    def create_output_folder(self):
        """Create the output folder if it doesn't exist"""
        try:
            if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)
                print(f"📁 Created folder: {self.output_folder}")
                self.logger.info(f"Created output folder: {self.output_folder}")
        except Exception as e:
            self.logger.error(f"Error creating output folder: {str(e)}")
            print(f"❌ Error creating folder: {str(e)}")
    
    def setup_driver(self):
        """Setup Chrome driver with network logging"""
        try:
            print("🔧 Setting up Chrome driver with network logging...")
            
            chrome_options = Options()
            
            # Basic Chrome options (matching PicklesScraper)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            # Additional options to avoid detection
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Enable performance logging to capture network requests (but use proper syntax)
            # For newer Chrome versions, use 'goog:loggingPrefs' instead of 'loggingPrefs'
            try:
                chrome_options.add_experimental_option('perfLoggingPrefs', {
                    'enableNetwork': True,
                    'enablePage': False,
                })
                # Try the old format first
                chrome_options.add_experimental_option('loggingPrefs', {
                    'performance': 'ALL'
                })
            except:
                # If that fails, try the new format
                chrome_options.set_capability('goog:loggingPrefs', {
                    'performance': 'ALL'
                })
            
            # Use the same approach as PicklesScraper with multiple fallbacks
            driver_setup_successful = False
            
            # Method 1: ChromeDriverManager with latest version
            if not driver_setup_successful and WEBDRIVER_MANAGER_AVAILABLE:
                try:
                    self.logger.info("Attempting to use ChromeDriverManager...")
                    print("🔧 Method 1: ChromeDriverManager with latest...")
                    service = Service(ChromeDriverManager(version="latest").install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    self.logger.info("ChromeDriverManager setup successful")
                    print("✅ ChromeDriverManager setup successful")
                    driver_setup_successful = True
                except Exception as chrome_manager_error:
                    self.logger.warning(f"ChromeDriverManager failed: {chrome_manager_error}")
                    print(f"⚠️ ChromeDriverManager failed: {chrome_manager_error}")
            
            # Method 2: Alternative ChromeDriverManager with cache refresh
            if not driver_setup_successful and WEBDRIVER_MANAGER_AVAILABLE:
                try:
                    self.logger.info("Attempting alternative ChromeDriverManager setup...")
                    print("🔧 Method 2: Alternative ChromeDriverManager...")
                    service = Service(ChromeDriverManager(cache_valid_range=1).install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    self.logger.info("Alternative ChromeDriverManager setup successful")
                    print("✅ Alternative ChromeDriverManager setup successful")
                    driver_setup_successful = True
                except Exception as alt_manager_error:
                    self.logger.warning(f"Alternative ChromeDriverManager failed: {alt_manager_error}")
                    print(f"⚠️ Alternative ChromeDriverManager failed: {alt_manager_error}")
            
            # Method 3: Try without performance logging (simplified setup)
            if not driver_setup_successful:
                try:
                    self.logger.info("Attempting simplified Chrome setup without performance logging...")
                    print("🔧 Method 3: Simplified setup without performance logging...")
                    
                    # Create simplified options without performance logging
                    simple_options = Options()
                    simple_options.add_argument("--no-sandbox")
                    simple_options.add_argument("--disable-dev-shm-usage")
                    simple_options.add_argument("--disable-gpu")
                    simple_options.add_argument("--window-size=1920,1080")
                    simple_options.add_argument("--disable-blink-features=AutomationControlled")
                    simple_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                    simple_options.add_experimental_option('useAutomationExtension', False)
                    
                    if WEBDRIVER_MANAGER_AVAILABLE:
                        service = Service(ChromeDriverManager().install())
                        self.driver = webdriver.Chrome(service=service, options=simple_options)
                    else:
                        self.driver = webdriver.Chrome(options=simple_options)
                    
                    self.logger.info("Simplified Chrome setup successful")
                    print("✅ Simplified Chrome setup successful (without performance logging)")
                    print("⚠️ Note: Network request monitoring will be limited")
                    driver_setup_successful = True
                    self.performance_logging_enabled = False
                    
                except Exception as simple_error:
                    self.logger.warning(f"Simplified Chrome setup failed: {simple_error}")
                    print(f"⚠️ Simplified Chrome setup failed: {simple_error}")
            
            # Method 4: System Chrome driver
            if not driver_setup_successful:
                try:
                    self.logger.info("Attempting to use system Chrome driver...")
                    print("🔧 Method 4: System Chrome driver...")
                    
                    # Try with simplified options
                    simple_options = Options()
                    simple_options.add_argument("--no-sandbox")
                    simple_options.add_argument("--disable-dev-shm-usage")
                    simple_options.add_argument("--disable-blink-features=AutomationControlled")
                    
                    self.driver = webdriver.Chrome(options=simple_options)
                    self.logger.info("System Chrome driver setup successful")
                    print("✅ System Chrome driver setup successful")
                    driver_setup_successful = True
                    self.performance_logging_enabled = False
                    
                except Exception as system_driver_error:
                    self.logger.error(f"System Chrome driver failed: {system_driver_error}")
                    print(f"⚠️ System Chrome driver failed: {system_driver_error}")
            
            # Method 5: Local chromedriver.exe in same folder
            if not driver_setup_successful:
                try:
                    self.logger.info("Attempting to use local chromedriver.exe...")
                    print("🔧 Method 5: Local chromedriver.exe...")
                    
                    local_driver_path = os.path.join(os.path.dirname(__file__), "chromedriver.exe")
                    if os.path.exists(local_driver_path):
                        self.logger.info(f"Found local chromedriver.exe at: {local_driver_path}")
                        print(f"📁 Found local chromedriver.exe at: {local_driver_path}")
                        
                        simple_options = Options()
                        simple_options.add_argument("--no-sandbox")
                        simple_options.add_argument("--disable-dev-shm-usage")
                        
                        service = Service(local_driver_path)
                        self.driver = webdriver.Chrome(service=service, options=simple_options)
                        self.logger.info("Local chromedriver.exe setup successful")
                        print("✅ Local chromedriver.exe setup successful")
                        driver_setup_successful = True
                        self.performance_logging_enabled = False
                    else:
                        raise FileNotFoundError(f"chromedriver.exe not found at: {local_driver_path}")
                        
                except Exception as local_driver_error:
                    self.logger.error(f"Local chromedriver.exe failed: {local_driver_error}")
                    print(f"⚠️ Local chromedriver.exe failed: {local_driver_error}")
            
            if not driver_setup_successful:
                # Provide detailed error message with Chrome version info
                try:
                    import subprocess
                    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
                    if os.path.exists(chrome_path):
                        result = subprocess.run([chrome_path, "--version"], capture_output=True, text=True, timeout=10)
                        chrome_version = result.stdout.strip() if result.returncode == 0 else "Unknown"
                    else:
                        chrome_version = "Chrome not found in standard location"
                except:
                    chrome_version = "Could not determine Chrome version"
                
                current_dir = os.path.dirname(__file__)
                error_msg = f"""
Chrome WebDriver setup failed with all methods.

Current Chrome version: {chrome_version}

SOLUTIONS for Chrome 141.0.7390+:
1. Chrome 141+ requires ChromeDriver from Chrome for Testing:
   - Visit: https://googlechromelabs.github.io/chrome-for-testing/
   - Download ChromeDriver for version 141.0.7390
   - Place chromedriver.exe in: {current_dir}

2. Alternative solutions:
   - Downgrade Chrome to version 130 or earlier
   - Install webdriver-manager: pip install --upgrade webdriver-manager
   - Update Selenium: pip install --upgrade selenium

3. Temporary workaround:
   - Use Chrome in headless mode
   - Use Firefox with geckodriver instead
"""
                print(error_msg)
                self.logger.error(error_msg)
                raise Exception("All ChromeDriver setup methods failed")
            
            # Set additional properties to avoid detection (if driver was created)
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                except:
                    pass  # Ignore if this fails
            
            # Initialize performance logging flag
            if not hasattr(self, 'performance_logging_enabled'):
                self.performance_logging_enabled = True
            
            print("✅ Chrome driver setup successful")
            self.logger.info("Chrome driver setup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up Chrome driver: {str(e)}")
            print(f"❌ Error setting up driver: {str(e)}")
            return False
    
    def get_stock_numbers_to_track(self):
        """
        Query database for stock numbers that need tracking
        
        Returns:
            list: List of stock numbers with tracking info
        """
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
            self.logger.info("Executing query to get trackable stock numbers")
            
            df = self.db.read_sql(query)
            
            if df.empty:
                print("⚠️ No stock numbers found for tracking")
                return []
            
            print(f"✅ Found {len(df)} stock numbers to track")
            self.logger.info(f"Found {len(df)} stock numbers for tracking")
            
            # Convert to list of dictionaries for easier processing
            stock_list = df.to_dict('records')
            
            # Log details for each stock number
            for stock in stock_list:
                print(f"   📋 Stock: {stock['stockNumber']} | Max scrape date: {stock['max_scrape_date']}")
            
            return stock_list
            
        except Exception as e:
            self.logger.error(f"Error querying stock numbers: {str(e)}")
            print(f"❌ Error querying database: {str(e)}")
            return []
    
    def build_vehicle_url(self, stock_number):
        """
        Build the vehicle details URL
        
        Args:
            stock_number (str): Stock number
            
        Returns:
            str: Vehicle details URL
        """
        return f"https://www.pickles.com.au/used/details/cars/{stock_number}"
    
    def navigate_to_vehicle_page(self, stock_number):
        """
        Navigate to vehicle details page
        
        Args:
            stock_number (str): Stock number
            
        Returns:
            bool: True if navigation successful
        """
        try:
            url = self.build_vehicle_url(stock_number)
            print(f"   🌐 Navigating to: {url}")
            
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            print(f"   ✅ Page loaded successfully")
            return True
            
        except TimeoutException:
            print(f"   ⏰ Timeout waiting for page to load")
            return False
        except Exception as e:
            print(f"   ❌ Navigation error: {str(e)}")
            self.logger.error(f"Navigation error for stock {stock_number}: {str(e)}")
            return False
    
    def wait_and_capture_network_calls(self, stock_number, timeout=10):
        """
        Wait for network calls and return all GET request URLs
        
        Args:
            stock_number (str): Stock number
            timeout (int): Time to wait for network calls
            
        Returns:
            list: List of GET request URLs
        """
        try:
            print(f"   ⏱️ Waiting {timeout} seconds for network calls...")
            time.sleep(timeout)
            
            # Get performance logs (network requests)
            logs = self.driver.get_log('performance')
            
            get_requests = []
            
            print(f"   📊 Total network logs captured: {len(logs)}")
            
            for log in logs:
                try:
                    message = json.loads(log['message'])
                    
                    # Look for network request messages (sent requests)
                    if message['message']['method'] == 'Network.requestWillBeSent':
                        request = message['message']['params']['request']
                        method = request.get('method', '')
                        url = request.get('url', '')
                        
                        # Only capture GET requests
                        if method == 'GET':
                            get_requests.append(url)
                            
                except Exception as e:
                    # Skip malformed log entries
                    continue
            
            print(f"   🔍 Found {len(get_requests)} GET requests:")
            
            # Display all GET requests
            for i, url in enumerate(get_requests, 1):
                print(f"      {i:2d}. {url}")
                
                # Highlight potential bidding APIs
                if any(keyword in url.lower() for keyword in ['bidding', 'auction', 'bid']):
                    print(f"          🎯 ^^^ POTENTIAL BIDDING API ^^^")
            
            return get_requests
            
        except Exception as e:
            print(f"   ❌ Error capturing network calls: {str(e)}")
            self.logger.error(f"Network capture error for stock {stock_number}: {str(e)}")
            return []
    
    def save_get_request(self, stock_number, api_url):
        """
        Make a GET request and save the response
        
        Args:
            stock_number (str): Stock number
            api_url (str): API URL to call
        """
        try:
            print(f"      🔗 Capturing GET request: {api_url}")
            
            # Get cookies from the browser session
            cookies = self.driver.get_cookies()
            session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            
            # Get user agent from browser
            user_agent = self.driver.execute_script("return navigator.userAgent;")
            
            headers = {
                'User-Agent': user_agent,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': self.build_vehicle_url(stock_number)
            }
            
            response = requests.get(api_url, cookies=session_cookies, headers=headers, timeout=10)
            
            if response.status_code == 200:
                print(f"      ✅ GET request successful")
                self.save_api_response(stock_number, api_url, response.text)
            else:
                print(f"      ❌ GET request failed: {response.status_code}")
                
        except Exception as e:
            print(f"      ❌ GET request error: {str(e)}")
            self.logger.error(f"GET request error for {api_url}: {str(e)}")
    
    def make_direct_api_call(self, stock_number, api_url):
        """
        Make direct API call if network capture fails
        
        Args:
            stock_number (str): Stock number
            api_url (str): API URL to call
            
        Returns:
            bool: True if API call successful and response saved
        """
        try:
            print(f"   🔗 Making direct API call: {api_url}")
            
            # Get cookies from the browser session
            cookies = self.driver.get_cookies()
            session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            
            # Get user agent from browser
            user_agent = self.driver.execute_script("return navigator.userAgent;")
            
            headers = {
                'User-Agent': user_agent,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': self.build_vehicle_url(stock_number)
            }
            
            response = requests.get(api_url, cookies=session_cookies, headers=headers, timeout=10)
            
            if response.status_code == 200:
                print(f"   ✅ Direct API call successful")
                self.save_api_response(stock_number, api_url, response.text)
                return True
            else:
                print(f"   ❌ Direct API call failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Direct API call error: {str(e)}")
            self.logger.error(f"Direct API call error for stock {stock_number}: {str(e)}")
            return False
    
    def save_api_response(self, stock_number, api_url, response_body):
        """
        Save API response to file
        
        Args:
            stock_number (str): Stock number
            api_url (str): API URL
            response_body (str): Response body content
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{stock_number}_lot_details_{timestamp}.json"
            filepath = os.path.join(self.output_folder, filename)
            
            # Parse response if it's JSON
            try:
                response_data = json.loads(response_body)
                # Add metadata
                response_data['_metadata'] = {
                    'stock_number': stock_number,
                    'api_url': api_url,
                    'captured_at': timestamp,
                    'captured_date': datetime.now().isoformat()
                }
            except json.JSONDecodeError:
                # If not JSON, wrap it
                response_data = {
                    'raw_response': response_body,
                    '_metadata': {
                        'stock_number': stock_number,
                        'api_url': api_url,
                        'captured_at': timestamp,
                        'captured_date': datetime.now().isoformat()
                    }
                }
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, indent=2, ensure_ascii=False)
            
            print(f"   💾 Saved response to: {filename}")
            self.logger.info(f"Saved API response for stock {stock_number} to {filepath}")
            
        except Exception as e:
            print(f"   ❌ Error saving response: {str(e)}")
            self.logger.error(f"Error saving response for stock {stock_number}: {str(e)}")
    
    def capture_api_response_in_new_tab(self, stock_number, api_url):
        """
        Open API URL in a new tab and capture the response
        
        Args:
            stock_number (str): Stock number
            api_url (str): API URL to open
            
        Returns:
            bool: True if successful
        """
        try:
            print(f"   🔗 Opening API URL in new tab: {api_url}")
            
            # Open a new tab
            self.driver.execute_script("window.open('');")
            
            # Switch to the new tab
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            # Navigate to the API URL
            self.driver.get(api_url)
            
            # Wait a moment for the page to load
            time.sleep(2)
            
            # Get the page content (JSON response)
            page_source = self.driver.page_source
            
            # Extract JSON from the page (it might be wrapped in <pre> tags)
            if '<pre>' in page_source:
                import re
                json_match = re.search(r'<pre[^>]*>(.*?)</pre>', page_source, re.DOTALL)
                if json_match:
                    json_content = json_match.group(1).strip()
                else:
                    json_content = page_source
            else:
                json_content = page_source
            
            # Save the response
            if json_content and json_content != '<html><head></head><body></body></html>':
                self.save_api_response_to_file(stock_number, api_url, json_content)
                print(f"   ✅ API response captured and saved")
                
                # Close the API tab and switch back to main tab
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                
                return True
            else:
                print(f"   ⚠️ No valid content found in API response")
                
                # Close the API tab and switch back to main tab
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                
                return False
                
        except Exception as e:
            print(f"   ❌ Error capturing API response: {str(e)}")
            self.logger.error(f"Error capturing API response for {api_url}: {str(e)}")
            
            # Try to close tab and switch back
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass
                
            return False
    
    def save_api_response_to_file(self, stock_number, api_url, response_content):
        """
        Save API response to file in vehicles_tracking folder
        
        Args:
            stock_number (str): Stock number
            api_url (str): API URL
            response_content (str): Response content
        """
        try:
            # Create vehicles_tracking directory if it doesn't exist
            tracking_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vehicles_tracking')
            os.makedirs(tracking_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stock_{stock_number}_api_response_{timestamp}.json"
            file_path = os.path.join(tracking_dir, filename)
            
            # Try to parse and pretty-print JSON
            try:
                import json
                json_data = json.loads(response_content)
                formatted_content = json.dumps(json_data, indent=2, ensure_ascii=False)
            except json.JSONDecodeError:
                # If not valid JSON, save as-is
                formatted_content = response_content
            
            # Save to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(formatted_content)
            
            print(f"   💾 Saved to: {file_path}")
            self.logger.info(f"API response saved: {file_path}")
            
        except Exception as e:
            print(f"   ❌ Error saving response: {str(e)}")
            self.logger.error(f"Error saving API response: {str(e)}")

    def track_vehicle(self, stock_info):
        """
        Track a single vehicle
        
        Args:
            stock_info (dict): Stock information from database
            
        Returns:
            bool: True if tracking successful
        """
        try:
            stock_number = stock_info['stockNumber']
            print(f"\n🚗 Processing Stock Number: {stock_number}")
            
            # Navigate to vehicle page
            if not self.navigate_to_vehicle_page(stock_number):
                return False
            
            # Directly open the API URL in a new tab and capture response
            api_url = f"https://www.pickles.com.au/api-website/buyer/ms-bidding-controller/v2/api/bidding/{stock_number}/lot-details/"
            success = self.capture_api_response_in_new_tab(stock_number, api_url)
            
            if success:
                print(f"   ✅ Successfully captured API response for stock {stock_number}")
                return True
            else:
                print(f"   ⚠️ Failed to capture API response for stock {stock_number}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error tracking stock {stock_number}: {str(e)}")
            self.logger.error(f"Error tracking stock {stock_number}: {str(e)}")
            return False
    
    def run_tracking(self):
        """
        Main tracking process
        """
        try:
            print("🚀 Starting Vehicle Tracking by Stock Number...")
            print("=" * 60)
            
            # Setup driver
            if not self.setup_driver():
                return False
            
            # Get stock numbers to track
            stock_list = self.get_stock_numbers_to_track()
            if not stock_list:
                return False
            
            print(f"\n🎯 Starting to track {len(stock_list)} vehicles...")
            print("📝 Note: Processing 1 vehicle at a time with user confirmation")
            
            successful_tracks = 0
            failed_tracks = 0
            
            for i, stock_info in enumerate(stock_list, 1):
                print(f"\n{'='*20} VEHICLE {i}/{len(stock_list)} {'='*20}")
                
                # Track the vehicle
                if self.track_vehicle(stock_info):
                    successful_tracks += 1
                else:
                    failed_tracks += 1
                
            
            # Final summary
            print(f"\n" + "=" * 60)
            print("📊 TRACKING SUMMARY")
            print("=" * 60)
            print(f"✅ Successfully tracked: {successful_tracks}")
            print(f"❌ Failed to track: {failed_tracks}")
            print(f"📊 Total processed: {len(stock_list)}")
            print(f"📁 Files saved to: {self.output_folder}/")
            
            return successful_tracks > 0
            
        except Exception as e:
            self.logger.error(f"Error in tracking process: {str(e)}")
            print(f"❌ Error: {str(e)}")
            return False
        finally:
            if self.driver:
                print("\n🔒 Closing browser...")
                self.driver.quit()
            if self.db:
                self.db.close()


@with_error_notification()
def main():
    """Main function"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║              VEHICLE TRACKER BY STOCK NUMBER                ║
║        Track vehicles using bidding API monitoring          ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    
    tracker = VehicleTracker()
    success = tracker.run_tracking()
    
    if success:
        print("\n✅ Vehicle tracking completed successfully!")
        return 0
    else:
        print("\n❌ Vehicle tracking failed!")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
