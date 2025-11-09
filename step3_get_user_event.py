#!/usr/bin/env python3
"""
Step 3: Get User Events - Extract EventIDs from API after opening auction watch URLs.
"""

import time
import pandas as pd
import csv
import json
import re
from typing import List, Optional, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Import the login functionality
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from whatsapp_notifier import with_error_notification
from pickles_login import PicklesScraper
from logger import get_logger
from db import MySecondDB

from db import MySecondDB

class Step2WatchEventDB:
    """
    Database operations for pickles_live_step2_watch_event table
    """
    
    def __init__(self):
        """Initialize database connection"""
        self.db = MySecondDB()
        self.logger = get_logger("step2_watch_event_db", log_to_file=True)
    
    def insert_watch_events(self, watch_events_data):
        """Insert watch event data into database"""
        try:
            if not watch_events_data:
                return 0
            
            # Convert to DataFrame and insert
            df = pd.DataFrame(watch_events_data)
            
            # Ensure columns match table structure
            expected_columns = ['row_order', 'auction_registration', 'auction_watch_url', 
                              'event_id', 'event_name', 'event_status']
            
            # Add missing columns with None values
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Select only the expected columns
            df = df[expected_columns]
            
            # Insert into database (replace existing data since we want fresh data each run)
            result = self.db.write_to_sql(df, 'pickles_live_step2_watch_event', how='append')
            
            inserted_count = len(watch_events_data)
            print(f"✅ Inserted {inserted_count} watch events into database")
            
            self.logger.info(f"Inserted {inserted_count} watch events into pickles_live_step2_watch_event")
            
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error inserting watch events: {str(e)}")
            print(f"❌ Database insert failed: {str(e)}")
            raise

class EventIDExtractor:
    """
    Extract EventIDs from Velocicast API after opening auction watch URLs.
    """
    
    def __init__(self):
        """Initialize the extractor."""
        self.logger = get_logger("event_id_extractor", log_to_file=True)
        self.scraper = None
        self.db_handler = Step2WatchEventDB()
        
        # Credentials (same as in scrape_pickle_schedule.py)
        self.USERNAME = "hnandarusdy2@gmail.com"
        self.PASSWORD = "123qwe!@#QWE"
    
    def read_step2_csv(self) -> List[str]:
        """
        Read pickles_auction_step2.csv and extract auction_registration list.
        
        Returns:
            List[str]: List of auction registration URLs
        """
        try:
            print("📄 Reading pickles_auction_step2.csv...")
            df = pd.read_csv("pickles_auction_step2.csv")
            
            print(f"   📊 Total records found: {len(df)}")
            # Filter for non-empty auction_registration
            valid_urls = df[
                df['auction_registration'].notna() & 
                (df['auction_registration'].str.strip() != '')
            ]
            
            print(f"   🔗 Valid registration URLs: {len(valid_urls)}")
            # Get the list of auction_registration
            auction_registration_urls = valid_urls['auction_registration'].tolist()

            print(f"   ✅ Registration URLs to process: {len(auction_registration_urls)}")

            # Display the URLs for verification
            for i, url in enumerate(auction_registration_urls, 1):
                print(f"   {i}. {url[:80]}...")

            return auction_registration_urls

        except Exception as e:
            print(f"❌ Error reading CSV: {str(e)}")
            self.logger.error(f"Error reading step2 CSV: {str(e)}")
            return []
    
    def login_to_pickles(self) -> bool:
        """
        Login to Pickles website (following scrape_pickle_schedule.py pattern).
        
        Returns:
            bool: True if login successful
        """
        try:
            print("🔐 Logging into Pickles...")
            self.scraper = PicklesScraper(headless=False, wait_timeout=15)
            
            if not self.scraper.login(self.USERNAME, self.PASSWORD):
                print("❌ Login failed!")
                return False
            
            print("✅ Successfully logged in!")
            return True
            
        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            self.logger.error(f"Login error: {str(e)}")
            return False
    
    def open_registration_and_watch(self, registration_url: str, index: int) -> bool:
        """
        Open auction registration URL and click 'Just watch' button.
        
        Args:
            registration_url (str): The registration URL to open
            index (int): The sequence number (1st, 2nd, etc.)
            
        Returns:
            bool: True if successfully opened and clicked watch
        """
        try:
            print(f"   🌐 Opening registration page ({self.get_ordinal(index)})...")
            print(f"   🔗 URL: {registration_url}")
            
            # Navigate to the registration URL
            self.scraper.driver.get(registration_url)
            
            # Wait for page to load
            print(f"   ⏳ Waiting for page to load...")
            time.sleep(3)
            
            print(f"   🔍 Looking for 'Just watch' button...")
            
            # Try to find and click the "Just watch" button
            wait = WebDriverWait(self.scraper.driver, 10)
            
            # Look for the "Just watch" button with various selectors
            just_watch_selectors = [
                "//a[contains(text(), 'Just watch')]",
                "//a[contains(@class, 'btn') and contains(text(), 'Just watch')]",
                "//a[@rel='nofollow' and contains(text(), 'Just watch')]",
                "//a[contains(@href, 'registrationType=LIVE_VIEW')]"
            ]
            
            button_found = False
            for selector in just_watch_selectors:
                try:
                    just_watch_button = wait.until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    
                    print(f"   🎯 Found 'Just watch' button!")
                    print(f"   🔗 Button href: {just_watch_button.get_attribute('href')}")
                    
                    # Click the button
                    just_watch_button.click()
                    print(f"   ✅ Clicked 'Just watch' button")
                    
                    button_found = True
                    break
                    
                except (TimeoutException, NoSuchElementException):
                    continue
            
            if not button_found:
                print(f"   ⚠️ 'Just watch' button not found, trying alternative approaches...")
                
                # Try to find any registration-related links
                try:
                    registration_links = self.scraper.driver.find_elements(
                        By.XPATH, "//a[contains(@href, 'registrationType=LIVE_VIEW')]" 
                    )
                    
                    if registration_links:
                        registration_links[0].click()
                        print(f"   ✅ Clicked registration link as fallback")
                        button_found = True
                    else:
                        print(f"   ❌ No registration links found")
                        
                except Exception as e2:
                    print(f"   ❌ Fallback approach failed: {str(e2)}")
            
            if button_found:
                # Wait for 2-3 seconds after clicking
                print(f"   ⏳ Waiting 3 seconds after clicking...")
                time.sleep(3)
                
                print(f"   📍 Current URL: {self.scraper.driver.current_url}")
                print(f"   📄 Page title: {self.scraper.driver.title}")
                
                # Check if redirected to Registration Form page
                if "Registration Form - Pickles Auctions Australia" in self.scraper.driver.title:
                    print(f"   🔄 Redirected to Registration Form page")
                    print(f"   🔍 Looking for 'CONFIRM' button...")
                    
                    try:
                        # Look for the CONFIRM button
                        confirm_wait = WebDriverWait(self.scraper.driver, 10)
                        confirm_button = confirm_wait.until(
                            EC.element_to_be_clickable((By.XPATH, "//input[@value='Confirm' or @value='CONFIRM']"))
                        )
                        
                        print(f"   🎯 Found 'CONFIRM' button!")
                        print(f"   🖱️ Button details: {confirm_button.get_attribute('outerHTML')}")
                        
                        # Click the CONFIRM button
                        confirm_button.click()
                        print(f"   ✅ Clicked 'CONFIRM' button")
                        
                        # Wait a bit after clicking confirm
                        print(f"   ⏳ Waiting 3 seconds after confirm...")
                        time.sleep(3)
                        
                        print(f"   📍 Final URL: {self.scraper.driver.current_url}")
                        print(f"   📄 Final title: {self.scraper.driver.title}")
                        
                    except (TimeoutException, NoSuchElementException) as e:
                        print(f"   ⚠️ CONFIRM button not found: {str(e)}")
                        print(f"   📄 Current page source preview: {self.scraper.driver.page_source[:500]}...")
                
                print(f"   ✅ Successfully processed {self.get_ordinal(index)} registration")
                return True
            else:
                print(f"   ❌ Could not find or click 'Just watch' button")
                return False
            
        except Exception as e:
            print(f"   💥 Error processing registration: {str(e)}")
            self.logger.error(f"Error processing {registration_url}: {str(e)}")
            return False
    
    def open_watch_urls(self, registration_urls: List[str]) -> bool:
        """
        Open auction watch URLs with delays.
        
        Args:
            registration_urls (List[str]): List of registration URLs to extract watch URLs
            
        Returns:
            bool: True if successfully processed watch URLs
        """
        try:
            print("📄 Reading auction_watch_url from CSV...")
            df = pd.read_csv("pickles_auction_step2.csv")
            
            # Filter for non-empty auction_watch_url
            valid_watch_urls = df[
                df['auction_watch_url'].notna() & 
                (df['auction_watch_url'].str.strip() != '')
            ]
            
            watch_urls = valid_watch_urls['auction_watch_url'].tolist()
            print(f"   🔗 Found {len(watch_urls)} watch URLs to process")
            
            if not watch_urls:
                print("   ⚠️ No watch URLs found, skipping watch URL loop")
                return True
            
            print(f"\n🔗 Processing {len(watch_urls)} auction watch URLs...")
            
            for i, watch_url in enumerate(watch_urls, 1):
                print(f"\n📺 Opening watch URL {i}/{len(watch_urls)}:")
                print(f"   🌐 URL: {watch_url}")
                
                try:
                    # Navigate to the watch URL
                    self.scraper.driver.get(watch_url)
                    
                    # Wait 3 seconds as requested
                    print(f"   ⏳ Waiting 3 seconds...")
                    time.sleep(3)
                    
                    print(f"   ✅ Successfully opened watch URL {i}")
                    print(f"   📍 Current URL: {self.scraper.driver.current_url}")
                    print(f"   📄 Page title: {self.scraper.driver.title}")
                    
                except Exception as e:
                    print(f"   ❌ Error opening watch URL: {str(e)}")
                    self.logger.error(f"Error opening watch URL {watch_url}: {str(e)}")
                
                # Delay between watch URLs (except for the last one)
                if i < len(watch_urls):
                    print("   ⏳ Brief pause before next watch URL...")
                    time.sleep(1)
            
            print(f"\n✅ All {len(watch_urls)} watch URLs have been processed!")
            return True
            
        except Exception as e:
            print(f"❌ Error processing watch URLs: {str(e)}")
            self.logger.error(f"Error processing watch URLs: {str(e)}")
            return False

    def get_ordinal(self, number: int) -> str:
        """Convert number to ordinal (1st, 2nd, 3rd, etc.)"""
        if 10 <= number % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th')
        return f"{number}{suffix}"
    
    def get_user_events_api(self) -> List[Dict]:
        """
        Get all user events from the refresh API.
        
        Returns:
            List[Dict]: List of event data with EventIDs
        """
        try:
            print("🔄 Getting user events from API...")
            
            # Open new tab for API request
            self.scraper.driver.execute_script("window.open('');")
            tabs = self.scraper.driver.window_handles
            self.scraper.driver.switch_to.window(tabs[-1])
            
            # Navigate to the user events API
            api_url = "https://api.pickles-au.velocicast.io/api/events/refresh/user-events"
            print(f"🔗 Getting API: {api_url}")
            
            self.scraper.driver.get(api_url)
            time.sleep(3)
            
            # Get the page source (API response)
            page_source = self.scraper.driver.page_source
            print(f"📊 API Response received (length: {len(page_source)} chars)")
            
            # Try to parse JSON and extract all events
            try:
                # Extract JSON from the page source
                json_match = re.search(r'\[.*\]', page_source, re.DOTALL)
                if json_match:
                    json_text = json_match.group(0)
                    data = json.loads(json_text)
                    
                    print("🎯 Parsing events from API response...")
                    
                    if isinstance(data, list):
                        events = []
                        for item in data:
                            if isinstance(item, dict) and 'EventID' in item:
                                events.append(item)
                        
                        print(f"✅ Found {len(events)} events with EventIDs")
                        for i, event in enumerate(events, 1):
                            event_id = event.get('EventID', 'Unknown')
                            event_name = event.get('Name', 'Unknown')
                            print(f"   {i}. EventID: {event_id} - {event_name}")
                        
                        return events
                    else:
                        print(f"❌ Unexpected API response format: {type(data)}")
                        return []
                        
                else:
                    print("❌ No JSON array found in API response")
                    print(f"📄 Raw response preview: {page_source[:200]}...")
                    return []
                    
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON: {str(e)}")
                print(f"📄 Raw response preview: {page_source[:200]}...")
                return []
                
        except Exception as e:
            print(f"💥 Error getting user events: {str(e)}")
            self.logger.error(f"Error getting user events: {str(e)}")
            return []
            
        finally:
            # Close the API tab and switch back to the original
            try:
                self.scraper.driver.close()
                if len(self.scraper.driver.window_handles) > 0:
                    self.scraper.driver.switch_to.window(self.scraper.driver.window_handles[0])
            except:
                pass
    
    def update_step2_csv_with_events(self, events: List[Dict]) -> List[Dict]:
        """
        Update pickles_auction_step2.csv with event IDs and prepare database data.
        The first user event goes to row 1, second to row 2, etc.
        
        Args:
            events (List[Dict]): List of events from user-events API
            
        Returns:
            List[Dict]: Database-ready data with row mapping
        """
        try:
            print("📝 Updating pickles_auction_step2.csv with event IDs...")
            
            # Read the existing step2 CSV
            df = pd.read_csv("pickles_auction_step2.csv")
            print(f"   📊 Current CSV has {len(df)} rows")
            
            # Add event_id column if it doesn't exist
            if 'event_id' not in df.columns:
                df['event_id'] = None
            if 'event_name' not in df.columns:
                df['event_name'] = None
            
            # Prepare database data
            watch_events_data = []
            
            # Map events to rows: first event goes to first row, etc.
            for i, event in enumerate(events):
                row_index = i  # 0-based index for DataFrame
                row_order = i + 1  # 1-based order for display and database
                
                event_id = str(event.get('EventID', ''))
                event_name = event.get('Name', '')
                event_status = event.get('Status', '')
                
                print(f"   📌 Mapping Event {row_order}: EventID {event_id} → Row {row_order}")
                
                # Update CSV data if row exists
                if row_index < len(df):
                    df.loc[row_index, 'event_id'] = event_id
                    df.loc[row_index, 'event_name'] = event_name
                    
                    # Prepare database record
                    watch_events_data.append({
                        'row_order': row_order,
                        'auction_registration': df.loc[row_index, 'auction_registration'],
                        'auction_watch_url': df.loc[row_index, 'auction_watch_url'],
                        'event_id': event_id,
                        'event_name': event_name,
                        'event_status': event_status
                    })
                else:
                    print(f"   ⚠️ Event {row_order} has no corresponding row in CSV - creating database record only")
                    # Still add to database even if no CSV row
                    watch_events_data.append({
                        'row_order': row_order,
                        'auction_registration': None,
                        'auction_watch_url': None,
                        'event_id': event_id,
                        'event_name': event_name,
                        'event_status': event_status
                    })
            
            # Save updated CSV
            df.to_csv("pickles_auction_step2.csv", index=False)
            print(f"✅ Updated pickles_auction_step2.csv with {len(events)} event IDs")
            
            return watch_events_data
            
        except Exception as e:
            print(f"❌ Error updating step2 CSV: {str(e)}")
            self.logger.error(f"Error updating step2 CSV: {str(e)}")
            return []
    
    def fetch_and_save_items_json(self, event_id: str) -> bool:
        """
        Fetch items data from API and save as JSON file with timestamp.
        
        Args:
            event_id (str): The event ID to fetch items for
            
        Returns:
            bool: True if successfully saved JSON
        """
        try:
            print(f"📦 Fetching items data for EventID: {event_id}")
            
            # Create json_data folder if it doesn't exist
            import os
            from datetime import datetime
            
            json_folder = "json_data"
            if not os.path.exists(json_folder):
                os.makedirs(json_folder)
                print(f"📁 Created folder: {json_folder}")
            
            # Open new tab for items API
            self.scraper.driver.execute_script("window.open('');")
            tabs = self.scraper.driver.window_handles
            self.scraper.driver.switch_to.window(tabs[-1])
            
            # Build items API URL
            items_api_url = f"https://api.pickles-au.velocicast.io/api/events/{event_id}/items?user=true"
            print(f"🔗 Getting items API: {items_api_url}")
            
            self.scraper.driver.get(items_api_url)
            time.sleep(3)
            
            # Get the JSON response
            page_source = self.scraper.driver.page_source
            print(f"📊 Items API response received (length: {len(page_source)} chars)")
            
            # Extract JSON from page source
            json_match = re.search(r'\{.*\}|\[.*\]', page_source, re.DOTALL)
            if json_match:
                json_text = json_match.group(0)
                
                # Validate JSON
                try:
                    json_data = json.loads(json_text)
                    
                    # Generate timestamp for filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    # Save to file with timestamp format: <event_id>_<timestamp>.json
                    json_filename = os.path.join(json_folder, f"{event_id}_{timestamp}.json")
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)
                    
                    print(f"✅ Saved items JSON: {json_filename}")
                    print(f"📏 JSON data size: {len(json_text)} characters")
                    
                    return True
                    
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON in items response: {str(e)}")
                    print(f"📄 Response preview: {page_source[:200]}...")
                    return False
            else:
                print(f"❌ No JSON found in items response")
                print(f"📄 Response preview: {page_source[:200]}...")
                return False
                
        except Exception as e:
            print(f"💥 Error fetching items data: {str(e)}")
            self.logger.error(f"Error fetching items for EventID {event_id}: {str(e)}")
            return False
            
        finally:
            # Close the items API tab and switch back
            try:
                self.scraper.driver.close()
                if len(self.scraper.driver.window_handles) > 0:
                    self.scraper.driver.switch_to.window(self.scraper.driver.window_handles[0])
            except:
                pass
        """
        Save results to pickles_auction_step3.csv.
        
        Args:
            results (List[Dict]): List of result dictionaries
        """
        try:
            csv_filename = "pickles_auction_step3.csv"
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['auction_watch_url', 'event_id']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data
                for result in results:
                    writer.writerow(result)
            
            print(f"💾 Results saved to: {csv_filename}")
            print(f"📊 Total records saved: {len(results)}")
            
            # Display summary
            successful_extractions = [r for r in results if r['event_id']]
            print(f"✅ Successful EventID extractions: {len(successful_extractions)}")
            print(f"❌ Failed extractions: {len(results) - len(successful_extractions)}")
            print(f"📁 JSON files saved in: json_data/ folder")
            
            # Show the results
            print("\n📋 Results Summary:")
            for i, result in enumerate(results, 1):
                event_id = result['event_id'] if result['event_id'] else 'None'
                json_file = f"json_data/{event_id}.json" if event_id != 'None' else 'No JSON file'
                print(f"   {i}. EventID: {event_id} → {json_file}")
                
        except Exception as e:
            print(f"❌ Error saving CSV: {str(e)}")
            self.logger.error(f"Error saving CSV: {str(e)}")
    
    def run(self):
        """
        Main execution method following the new flow:
        1. Login
        2. Loop through and open all watch URLs with delays
        3. Only after all loops finish, get user events API
        4. For each event ID, fetch items and save JSON
        """
        print("🚀 Starting EventID Extraction with New Flow...")
        
        try:
            # Step 1: Read step2 CSV
            print("\n1️⃣ Reading pickles_auction_step2.csv...")
            registration_urls = self.read_step2_csv()
            
            if not registration_urls:
                print("❌ No registration URLs found. Exiting.")
                return
            
            # Step 2: Login to Pickles
            print("\n2️⃣ Logging into Pickles...")
            if not self.login_to_pickles():
                return
            
            # Step 3: Process ALL registration URLs first (with delays)
            print(f"\n3️⃣ Processing {len(registration_urls)} registration URLs...")
            
            for i, registration_url in enumerate(registration_urls, 1):
                print(f"\n📄 Processing registration {i}/{len(registration_urls)}:")
                
                # Open registration page and click 'Just watch'
                self.open_registration_and_watch(registration_url, i)
                
                # Brief pause between URLs (except for the last one)
                if i < len(registration_urls):
                    print("   ⏳ Brief pause before next URL...")
                    time.sleep(1)
            
            print(f"\n✅ All {len(registration_urls)} registrations have been processed!")
            
            # Step 3.5: Process auction watch URLs
            print(f"\n🔗 Step 3.5: Processing auction watch URLs...")
            if not self.open_watch_urls(registration_urls):
                print("⚠️ Failed to process watch URLs, but continuing...")
            
            # Step 4: Get user events API (only after all URLs are processed)
            print(f"\n4️⃣ Getting user events from API...")
            events = self.get_user_events_api()
            
            if not events:
                print("❌ No events found from user events API. Exiting.")
                return
            
            # Step 4.5: Update step2 CSV with event IDs and prepare database data
            print(f"\n4️⃣.5️⃣ Updating pickles_auction_step2.csv with event IDs...")
            watch_events_data = self.update_step2_csv_with_events(events)
            
            # Step 4.6: Insert into database
            print(f"\n4️⃣.6️⃣ Inserting watch events into database...")
            try:
                inserted_count = self.db_handler.insert_watch_events(watch_events_data)
                print(f"✅ Database operations completed - {inserted_count} records inserted")
            except Exception as e:
                print(f"❌ Database insert failed: {str(e)}")
                self.logger.error(f"Database insert error: {str(e)}")
                # Continue with JSON processing even if database fails
            
            # Step 5: For each event ID, fetch items and save JSON
            print(f"\n5️⃣ Fetching items for {len(events)} events...")
            
            successful_saves = 0
            failed_saves = 0
            
            for i, event in enumerate(events, 1):
                event_id = str(event.get('EventID', ''))
                event_name = event.get('Name', 'Unknown')
                
                print(f"\n📦 Processing Event {i}/{len(events)}:")
                print(f"   🆔 EventID: {event_id}")
                print(f"   📋 Name: {event_name}")
                
                if event_id:
                    if self.fetch_and_save_items_json(event_id):
                        successful_saves += 1
                        print(f"   ✅ Successfully saved items JSON")
                    else:
                        failed_saves += 1
                        print(f"   ❌ Failed to save items JSON")
                else:
                    failed_saves += 1
                    print(f"   ❌ No valid EventID found")
                
                # Brief pause between API calls
                if i < len(events):
                    print("   ⏳ Brief pause before next event...")
                    time.sleep(1)
            
            # Final summary
            print(f"\n🎉 Process completed successfully!")
            print(f"📊 Summary:")
            print(f"   🔗 Registration URLs processed: {len(registration_urls)}")
            print(f"   📋 Events found: {len(events)}")
            print(f"   📝 pickles_auction_step2.csv updated with event IDs")
            print(f"   💾 Watch events saved to pickles_live_step2_watch_event table")
            print(f"   ✅ JSON files saved: {successful_saves}")
            print(f"   ❌ Failed saves: {failed_saves}")
            print(f"   📁 JSON files location: json_data/ folder")
            
        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            self.logger.error(f"Unexpected error in run(): {str(e)}")
            
        finally:
            # Cleanup
            if self.scraper:
                print("\n🧹 Cleaning up...")
                self.scraper.close()
                print("✅ Cleanup complete!")

@with_error_notification()
def main():
    """Main function."""
    extractor = EventIDExtractor()
    extractor.run()

if __name__ == "__main__":
    main()
