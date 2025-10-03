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
from pickles_login import PicklesScraper
from logger import get_logger

class EventIDExtractor:
    """
    Extract EventIDs from Velocicast API after opening auction watch URLs.
    """
    
    def __init__(self):
        """Initialize the extractor."""
        self.logger = get_logger("event_id_extractor", log_to_file=True)
        self.scraper = None
        
        # Credentials (same as in scrape_pickle_schedule.py)
        self.USERNAME = "hnandarusdy2@gmail.com"
        self.PASSWORD = "123qwe!@#QWE"
    
    def read_step2_csv(self) -> List[str]:
        """
        Read pickles_auction_step2.csv and extract auction_watch_url list.
        
        Returns:
            List[str]: List of auction watch URLs
        """
        try:
            print("📄 Reading pickles_auction_step2.csv...")
            df = pd.read_csv("pickles_auction_step2.csv")
            
            print(f"   📊 Total records found: {len(df)}")
            
            # Filter for non-empty auction_watch_url
            valid_urls = df[
                df['auction_watch_url'].notna() & 
                (df['auction_watch_url'].str.strip() != '')
            ]
            
            print(f"   🔗 Valid watch URLs: {len(valid_urls)}")
            
            # Get the list of auction_watch_url
            watch_urls = valid_urls['auction_watch_url'].tolist()
            
            print(f"   ✅ Watch URLs to process: {len(watch_urls)}")
            
            # Display the URLs for verification
            for i, url in enumerate(watch_urls, 1):
                print(f"   {i}. {url[:80]}...")
            
            return watch_urls
            
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
    
    def open_watch_url(self, watch_url: str, index: int) -> bool:
        """
        Open auction watch URL and record the sequence.
        
        Args:
            watch_url (str): The watch URL to open
            index (int): The sequence number (1st, 2nd, etc.)
            
        Returns:
            bool: True if successfully opened
        """
        try:
            print(f"   🌐 Opening watch URL ({self.get_ordinal(index)})...")
            print(f"   🔗 URL: {watch_url}")
            
            # Navigate to the watch URL
            self.scraper.driver.get(watch_url)
            
            # Wait for page to load
            time.sleep(3)
            
            print(f"   ✅ Successfully opened {self.get_ordinal(index)} URL")
            print(f"   📍 Current URL: {self.scraper.driver.current_url}")
            print(f"   📄 Page title: {self.scraper.driver.title}")
            
            return True
            
        except Exception as e:
            print(f"   💥 Error opening URL: {str(e)}")
            self.logger.error(f"Error opening {watch_url}: {str(e)}")
            return False
    
    def get_ordinal(self, number: int) -> str:
        """Convert number to ordinal (1st, 2nd, 3rd, etc.)"""
        if 10 <= number % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th')
        return f"{number}{suffix}"
    
    def extract_event_id_from_api(self) -> Optional[str]:
        """
        Wait 5 seconds, open new tab, get user-events API, and extract EventID.
        
        Returns:
            Optional[str]: Event ID or None
        """
        try:
            print("   ⏰ Waiting 5 seconds...")
            time.sleep(5)
            
            print("   🆕 Opening new tab for API request...")
            
            # Open new tab
            self.scraper.driver.execute_script("window.open('');")
            
            # Switch to new tab
            tabs = self.scraper.driver.window_handles
            self.scraper.driver.switch_to.window(tabs[-1])
            
            # Navigate to the API URL
            api_url = "https://api.pickles-au.velocicast.io/api/events/refresh/user-events"
            print(f"   🔗 Getting API: {api_url}")
            
            self.scraper.driver.get(api_url)
            time.sleep(3)
            
            # Get the page source (API response)
            page_source = self.scraper.driver.page_source
            
            print(f"   📊 API Response received (length: {len(page_source)} chars)")
            
            # Try to parse JSON and extract EventID
            try:
                # Extract JSON from the page source (remove HTML wrapper if present)
                json_match = re.search(r'\[.*\]', page_source)
                if json_match:
                    json_text = json_match.group(0)
                    data = json.loads(json_text)
                    
                    print("   🎯 Parsing EventIDs from API response...")
                    
                    if isinstance(data, list) and len(data) > 0:
                        # Get the first EventID
                        for item in data:
                            if isinstance(item, dict) and 'EventID' in item:
                                event_id = str(item['EventID'])
                                print(f"   ✅ Found EventID: {event_id}")
                                return event_id
                        
                        print("   ❌ No EventID found in API response items")
                        return None
                    else:
                        print(f"   ❌ Unexpected API response format: {type(data)}")
                        return None
                        
                else:
                    print("   ❌ No JSON array found in API response")
                    print(f"   📄 Raw response preview: {page_source[:200]}...")
                    return None
                    
            except json.JSONDecodeError as e:
                print(f"   ❌ Error parsing JSON: {str(e)}")
                print(f"   📄 Raw response preview: {page_source[:200]}...")
                return None
                
        except Exception as e:
            print(f"   💥 Error extracting EventID: {str(e)}")
            self.logger.error(f"Error extracting EventID: {str(e)}")
            return None
            
        finally:
            # Close the API tab and switch back to the original
            try:
                self.scraper.driver.close()
                if len(self.scraper.driver.window_handles) > 0:
                    self.scraper.driver.switch_to.window(self.scraper.driver.window_handles[0])
            except:
                pass
    
    def fetch_and_save_items_json(self, event_id: str) -> bool:
        """
        Fetch items data from API and save as JSON file.
        
        Args:
            event_id (str): The event ID to fetch items for
            
        Returns:
            bool: True if successfully saved JSON
        """
        try:
            print(f"   📦 Fetching items data for EventID: {event_id}")
            
            # Create json_data folder if it doesn't exist
            import os
            json_folder = "json_data"
            if not os.path.exists(json_folder):
                os.makedirs(json_folder)
                print(f"   📁 Created folder: {json_folder}")
            
            # Open new tab for items API
            self.scraper.driver.execute_script("window.open('');")
            tabs = self.scraper.driver.window_handles
            self.scraper.driver.switch_to.window(tabs[-1])
            
            # Build items API URL
            items_api_url = f"https://api.pickles-au.velocicast.io/api/events/{event_id}/items?user=true"
            print(f"   🔗 Getting items API: {items_api_url}")
            
            self.scraper.driver.get(items_api_url)
            time.sleep(3)
            
            # Get the JSON response
            page_source = self.scraper.driver.page_source
            print(f"   📊 Items API response received (length: {len(page_source)} chars)")
            
            # Extract JSON from page source
            json_match = re.search(r'\{.*\}|\[.*\]', page_source, re.DOTALL)
            if json_match:
                json_text = json_match.group(0)
                
                # Validate JSON
                try:
                    json_data = json.loads(json_text)
                    
                    # Save to file
                    json_filename = os.path.join(json_folder, f"{event_id}.json")
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2, ensure_ascii=False)
                    
                    print(f"   ✅ Saved items JSON: {json_filename}")
                    print(f"   📏 JSON data size: {len(json_text)} characters")
                    
                    return True
                    
                except json.JSONDecodeError as e:
                    print(f"   ❌ Invalid JSON in items response: {str(e)}")
                    print(f"   📄 Response preview: {page_source[:200]}...")
                    return False
            else:
                print(f"   ❌ No JSON found in items response")
                print(f"   📄 Response preview: {page_source[:200]}...")
                return False
                
        except Exception as e:
            print(f"   💥 Error fetching items data: {str(e)}")
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
        Main execution method.
        """
        print("🚀 Starting EventID Extraction from User Events API...")
        
        try:
            # Step 1: Read step2 CSV
            print("\n1️⃣ Reading pickles_auction_step2.csv...")
            watch_urls = self.read_step2_csv()
            
            if not watch_urls:
                print("❌ No watch URLs found. Exiting.")
                return
            
            # Step 2: Login to Pickles
            print("\n2️⃣ Logging into Pickles...")
            if not self.login_to_pickles():
                return
            
            # Step 3: Process each watch URL
            print(f"\n3️⃣ Processing {len(watch_urls)} watch URLs...")
            results = []
            
            for i, watch_url in enumerate(watch_urls, 1):
                print(f"\n📄 Processing {i}/{len(watch_urls)}:")
                
                # Open the watch URL
                if self.open_watch_url(watch_url, i):
                    # Extract EventID from API
                    event_id = self.extract_event_id_from_api()
                    
                    # If we got an EventID, fetch and save the items JSON
                    if event_id:
                        self.fetch_and_save_items_json(event_id)
                    else:
                        print("   ⚠️ No EventID found, skipping items fetch")
                else:
                    event_id = None
                
                results.append({
                    'auction_watch_url': watch_url,
                    'event_id': event_id
                })
                
                # Small delay between URLs
                if i < len(watch_urls):
                    print("   ⏳ Brief pause before next URL...")
                    time.sleep(2)
            
            # Step 4: Save results
            print(f"\n4️⃣ Saving results...")
            self.save_results_to_csv(results)
            
            print("\n🎉 EventID extraction completed successfully!")
            
        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            self.logger.error(f"Unexpected error in run(): {str(e)}")
            
        finally:
            # Cleanup
            if self.scraper:
                print("\n🧹 Cleaning up...")
                self.scraper.close()
                print("✅ Cleanup complete!")

def main():
    """Main function."""
    extractor = EventIDExtractor()
    extractor.run()

if __name__ == "__main__":
    main()
