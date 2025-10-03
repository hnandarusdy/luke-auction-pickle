#!/usr/bin/env python3
"""
Scrape auction live - Step 2: Extract "Just Watch" URLs from auction registration pages.
"""

import time
import pandas as pd
import csv
from datetime import datetime, date
from typing import List, Optional
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

class AuctionWatchScraper:
    """
    Scraper to extract "Just Watch" URLs from auction registration pages.
    """
    
    def __init__(self):
        """Initialize the scraper."""
        self.logger = get_logger("auction_watch_scraper", log_to_file=True)
        self.scraper = None
        
        # Credentials
        self.USERNAME = "hnandarusdy2@gmail.com"
        self.PASSWORD = "123qwe!@#QWE"
    
    def parse_auction_date(self, date_str: str) -> Optional[date]:
        """
        Parse auction date from various formats.
        
        Args:
            date_str (str): Date string to parse
            
        Returns:
            Optional[date]: Parsed date or None
        """
        if not date_str or pd.isna(date_str):
            return None
            
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Handle date ranges first - take the start date
            if " - " in date_str:
                start_part = date_str.split(" - ")[0].strip()
                return self.parse_auction_date(start_part)
            
            # Remove timezone abbreviations as they're not needed for date parsing
            # Common Australian timezones: AEST, AEDT, AWST, ACST, ACDT
            timezone_abbreviations = ['AEST', 'AEDT', 'AWST', 'ACST', 'ACDT']
            for tz in timezone_abbreviations:
                if date_str.endswith(f' {tz}'):
                    date_str = date_str[:-len(f' {tz}')].strip()
                    break
            
            # Try different date formats
            formats = [
                "%A %d/%m/%Y %I:%M%p",     # Friday 03/10/2025 10:00am
                "%d/%m/%Y %I:%M%p",        # 03/10/2025 10:00am  
                "%A %d/%m/%Y",             # Friday 03/10/2025
                "%d/%m/%Y",                # 03/10/2025
                "%A %d/%m/%Y %H:%M",       # Friday 03/10/2025 10:00
                "%d/%m/%Y %H:%M",          # 03/10/2025 10:00
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt).date()
                    return parsed_date
                except ValueError:
                    continue
            
            # Try with regex extraction if format parsing fails
            import re
            
            # Extract date pattern: dd/mm/yyyy
            date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match = re.search(date_pattern, date_str)
            
            if match:
                day, month, year = match.groups()
                try:
                    parsed_date = date(int(year), int(month), int(day))
                    return parsed_date
                except ValueError:
                    pass
                    
            self.logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {str(e)}")
            return None
    
    def read_and_filter_csv(self) -> List[str]:
        """
        Read CSV and filter for today/past dates with auction_registration URLs.
        
        Returns:
            List[str]: List of auction_registration URLs
        """
        try:
            print("📄 Reading pickles_auctions_detailed.csv...")
            df = pd.read_csv("pickles_auctions_detailed.csv")
            
            print(f"   📊 Total auctions found: {len(df)}")
            
            # Filter for today or past dates
            today = date.today()
            print(f"   📅 Filtering for current/past dates (today: {today})")
            
            # Parse sale dates
            df['parsed_date'] = df['sale_date'].apply(self.parse_auction_date)
            
            # Filter for current and past dates
            date_filtered = df[
                (df['parsed_date'].notna()) & 
                (df['parsed_date'] <= today)
            ]
            
            print(f"   📅 Auctions with current/past dates: {len(date_filtered)}")
            
            # Filter for non-empty auction_registration URLs
            registration_filtered = date_filtered[
                df['auction_registration'].notna() & 
                (df['auction_registration'].str.strip() != '')
            ]
            
            print(f"   🎫 Auctions with registration URLs: {len(registration_filtered)}")
            
            # Get the list of auction_registration URLs
            registration_urls = registration_filtered['auction_registration'].tolist()
            
            print(f"   ✅ Final filtered URLs: {len(registration_urls)}")
            
            # Display the URLs for verification
            for i, url in enumerate(registration_urls, 1):
                print(f"   {i}. {url}")
            
            return registration_urls
            
        except Exception as e:
            print(f"❌ Error reading/filtering CSV: {str(e)}")
            self.logger.error(f"Error reading CSV: {str(e)}")
            return []
    
    def login_to_pickles(self) -> bool:
        """
        Login to Pickles website.
        
        Returns:
            bool: True if login successful
        """
        try:
            print("🔐 Logging into Pickles...")
            self.scraper = PicklesScraper()
            
            if not self.scraper.login(self.USERNAME, self.PASSWORD):
                print("❌ Login failed!")
                return False
            
            print("✅ Successfully logged in!")
            return True
            
        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            self.logger.error(f"Login error: {str(e)}")
            return False
    
    def convert_registration_to_watch_url(self, registration_url: str) -> Optional[str]:
        """
        Convert registration URL to watch URL using the discovered pattern.
        
        Args:
            registration_url (str): The registration URL
            
        Returns:
            Optional[str]: The watch URL or None
        """
        try:
            # Extract saleId from registration URL
            import re
            
            # Pattern to extract saleId parameter
            sale_id_match = re.search(r'saleId=(\d+)', registration_url)
            
            if not sale_id_match:
                print(f"   ❌ Could not extract saleId from: {registration_url}")
                return None
            
            sale_id = sale_id_match.group(1)
            
            # Build watch URL using the discovered pattern
            watch_url = (
                f"https://www.pickles.com.au/group/pickles/bidding/pickles-live/launch?"
                f"p_p_id=PicklesLiveRedirectPortlet_WAR_PWRWeb&"
                f"p_p_lifecycle=1&"
                f"p_p_state=normal&"
                f"p_p_mode=view&"
                f"p_p_col_id=main-content&"
                f"p_p_col_pos=1&"
                f"p_p_col_count=2&"
                f"_PicklesLiveRedirectPortlet_WAR_PWRWeb_action=liveRedirect&"
                f"_PicklesLiveRedirectPortlet_WAR_PWRWeb_sale={sale_id}"
            )
            
            print(f"   ✅ Converted saleId {sale_id} to watch URL")
            return watch_url
            
        except Exception as e:
            print(f"   💥 Error converting URL: {str(e)}")
            self.logger.error(f"Error converting {registration_url}: {str(e)}")
            return None
    
    def save_results_to_csv(self, results: List[dict]) -> None:
        """
        Save results to CSV file.
        
        Args:
            results (List[dict]): List of result dictionaries
        """
        try:
            csv_filename = "pickles_auction_step2.csv"
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['auction_registration', 'auction_watch_url']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data
                for result in results:
                    writer.writerow(result)
            
            print(f"💾 Results saved to: {csv_filename}")
            print(f"📊 Total records saved: {len(results)}")
            
            # Display summary
            successful_extractions = [r for r in results if r['auction_watch_url']]
            print(f"✅ Successful extractions: {len(successful_extractions)}")
            print(f"❌ Failed extractions: {len(results) - len(successful_extractions)}")
            
        except Exception as e:
            print(f"❌ Error saving CSV: {str(e)}")
            self.logger.error(f"Error saving CSV: {str(e)}")
    
    def run(self):
        """
        Main execution method.
        """
        print("🚀 Starting Auction Watch URL Generation...")
        
        try:
            # Step 1: Chill
            print("\n1️⃣ Chilling...")
            time.sleep(1)
            
            # Step 2: Read and filter CSV
            print("\n2️⃣ Reading and filtering CSV...")
            registration_urls = self.read_and_filter_csv()
            
            if not registration_urls:
                print("❌ No registration URLs found. Exiting.")
                return
            
            # Step 3: Convert URLs using pattern (no need to login/scrape!)
            print(f"\n3️⃣ Converting {len(registration_urls)} registration URLs to watch URLs...")
            results = []
            
            for i, reg_url in enumerate(registration_urls, 1):
                print(f"\n� Processing {i}/{len(registration_urls)}:")
                print(f"   📝 Registration URL: {reg_url[:80]}...")
                
                watch_url = self.convert_registration_to_watch_url(reg_url)
                
                results.append({
                    'auction_registration': reg_url,
                    'auction_watch_url': watch_url
                })
            
            # Step 4: Save results
            print(f"\n4️⃣ Saving results...")
            self.save_results_to_csv(results)
            
            print("\n🎉 Process completed successfully!")
            print("💡 Note: Used URL pattern conversion - much faster than web scraping!")
            
        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            self.logger.error(f"Unexpected error in run(): {str(e)}")
            
        finally:
            print("✅ No cleanup needed - no browser was used!")
            print("🚀 Pattern-based conversion completed!")

def main():
    """Main function."""
    scraper = AuctionWatchScraper()
    scraper.run()

if __name__ == "__main__":
    main()
