#!/usr/bin/env python3
"""
Scrape Online Auction Stock Numbers - Step 3: Extract stock numbers from online auction listings.
Reads pickles_auctions_detailed_online.csv and scrapes stock numbers from each auction page.
Includes database integration to store results in pickles_online_inventory table.
"""

import pandas as pd
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from pickles_login import PicklesScraper
from logger import get_logger
from db import MySecondDB
from datetime import datetime

class OnlineStockScraper:
    """
    Scraper to extract stock numbers from online auction listings.
    """
    
    def __init__(self):
        """Initialize the scraper."""
        self.logger = get_logger("online_stock_scraper", log_to_file=True)
        self.input_file = "pickles_auctions_detailed_online.csv"
        self.output_file = "pickles_online_stock_numbers.csv"
        self.driver = None
        self.wait = None
        self.all_stock_data = []
        
        # Database integration
        self.db = MySecondDB()
        self.table_name = "pickles_online_inventory"
    
    def setup_driver(self):
        """Setup Chrome driver."""
        try:
            pickles_scraper = PicklesScraper()
            pickles_scraper.setup_driver()
            self.driver = pickles_scraper.driver
            self.wait = WebDriverWait(self.driver, 20)
            self.logger.info("✅ Chrome driver setup successful")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False
    
    def extract_sale_no_from_url(self, url):
        """
        Extract sale number from URL
        
        Args:
            url (str): The URL (either sale_info_url or listing_url)
            
        Returns:
            str: Sale number or None
        """
        try:
            # Extract sale number from URL query parameters or path
            if "sale_no=" in url:
                # From sale_info_url: ?sale_no=11834
                sale_no = url.split("sale_no=")[1].split("&")[0]
            else:
                # From listing URL: /national-commercial-and-ex-mining-vehicle-auction/11834
                parts = url.split('/')
                sale_no = parts[-1].split('?')[0]
            
            if sale_no.isdigit():
                return sale_no
            return None
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting sale_no from URL {url}: {e}")
            return None
    
    def parse_sale_date(self, sale_date_str):
        """
        Parse sale date string to DATE format for database
        
        Args:
            sale_date_str (str): Sale date from CSV
            
        Returns:
            str: Formatted date string (YYYY-MM-DD) or None
        """
        try:
            if not sale_date_str or pd.isna(sale_date_str):
                return None
            
            # Extract first date from format: "Friday 17/10/2025 10:00am - Friday 17/10/2025 12:00pm AEDT"
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', sale_date_str)
            if date_match:
                date_str = date_match.group(1)
                # Convert from DD/MM/YYYY to YYYY-MM-DD
                day, month, year = date_str.split('/')
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            return None
        except Exception as e:
            self.logger.warning(f"⚠️ Error parsing sale date {sale_date_str}: {e}")
            return None
    
    def get_sale_date_from_csv(self, sale_no):
        """
        Get sale date from the original CSV file
        
        Args:
            sale_no (str): Sale number
            
        Returns:
            str: Formatted sale date (YYYY-MM-DD) or None
        """
        try:
            df = pd.read_csv(self.input_file)
            
            # Find the row with matching sale_no in the sale_info_url
            for index, row in df.iterrows():
                if f"sale_no={sale_no}" in str(row['sale_info_url']):
                    return self.parse_sale_date(row['sale_date'])
            
            return None
        except Exception as e:
            self.logger.warning(f"⚠️ Error getting sale_date for sale_no {sale_no}: {e}")
            return None
    
    def insert_inventory_record(self, url, sale_no, sale_date, stock_no):
        """
        Insert a single inventory record into database
        
        Args:
            url (str): Listing URL
            sale_no (str): Sale number
            sale_date (str): Sale date (YYYY-MM-DD format)
            stock_no (str): Stock number
        """
        try:
            # Escape single quotes in the data
            url_escaped = url.replace("'", "\\'")
            sale_date_value = f"'{sale_date}'" if sale_date else "NULL"
            
            insert_query = f"""
            INSERT INTO {self.table_name} (url, sale_no, sale_date, stock_no)
            VALUES ('{url_escaped}', '{sale_no}', {sale_date_value}, '{stock_no}')
            """
            
            self.db.execute_query(insert_query)
            self.logger.info(f"✅ DB Insert: Sale {sale_no}, Stock {stock_no}")
            
        except Exception as e:
            self.logger.error(f"❌ Error inserting record: {e}")
            # Don't raise here to continue processing other records
    
    def find_and_click_view_listing(self, sale_info_url):
        """
        Open sale info URL and click 'VIEW LISTING' button.
        
        Args:
            sale_info_url (str): The sale info URL
            
        Returns:
            str: The redirected listing URL or None if failed
        """
        try:
            self.logger.info(f"🌐 Opening sale info page: {sale_info_url}")
            self.driver.get(sale_info_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Find the "VIEW LISTING" button
            try:
                # Try multiple selectors for the VIEW LISTING button
                view_listing_selectors = [
                    "//a[contains(text(), 'View listing')]",
                    "//a[contains(text(), 'VIEW LISTING')]",
                    "//a[contains(@class, 'btn') and contains(text(), 'View')]",
                    "//a[@class='btn btn-outline-primary btn-sm mr-2']"
                ]
                
                view_listing_btn = None
                for selector in view_listing_selectors:
                    try:
                        view_listing_btn = self.wait.until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        self.logger.info(f"✅ Found VIEW LISTING button with selector: {selector}")
                        break
                    except TimeoutException:
                        continue
                
                if not view_listing_btn:
                    self.logger.warning(f"❌ Could not find VIEW LISTING button on: {sale_info_url}")
                    return None
                
                # Get the href before clicking
                listing_url = view_listing_btn.get_attribute('href')
                self.logger.info(f"🔗 Found listing URL: {listing_url}")
                
                # Click the button
                try:
                    view_listing_btn.click()
                    self.logger.info("✅ Clicked VIEW LISTING button")
                except ElementClickInterceptedException:
                    # Try JavaScript click if regular click fails
                    self.driver.execute_script("arguments[0].click();", view_listing_btn)
                    self.logger.info("✅ Clicked VIEW LISTING button using JavaScript")
                
                # Wait for redirect
                time.sleep(5)
                
                # Get current URL after redirect
                current_url = self.driver.current_url
                self.logger.info(f"📍 Redirected to: {current_url}")
                
                return current_url
                
            except TimeoutException:
                self.logger.warning(f"❌ Timeout waiting for VIEW LISTING button on: {sale_info_url}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error processing sale info page {sale_info_url}: {e}")
            return None
    
    def scrape_stock_numbers(self, listing_url, auction_title, sale_info_url):
        """
        Scrape stock numbers from the listing page and store in database.
        
        Args:
            listing_url (str): The auction listing URL
            auction_title (str): The auction title for reference
            sale_info_url (str): Original sale info URL for extracting sale_no
            
        Returns:
            list: List of stock numbers found
        """
        try:
            # Extract sale number and get sale date for database
            sale_no = self.extract_sale_no_from_url(sale_info_url)
            sale_date = self.get_sale_date_from_csv(sale_no) if sale_no else None
            
            # Add pagination parameters
            if '?' in listing_url:
                paginated_url = f"{listing_url}&page=1&limit=120"
            else:
                paginated_url = f"{listing_url}?page=1&limit=120"
            
            self.logger.info(f"🔍 Opening paginated listing: {paginated_url}")
            self.driver.get(paginated_url)
            
            # Wait for page to load
            time.sleep(5)
            
            # Find the content wrapper
            try:
                content_wrapper = self.wait.until(
                    EC.presence_of_element_located((
                        By.CLASS_NAME, "content-wrapper_contentgridwrapper__3RCQZ"
                    ))
                )
                self.logger.info("✅ Found content wrapper")
            except TimeoutException:
                self.logger.warning(f"❌ Could not find content wrapper on: {paginated_url}")
                return []
            
            # Find all item columns
            item_columns = self.driver.find_elements(
                By.CSS_SELECTOR, 
                ".content-wrapper_contentgridwrapper__3RCQZ .column.is-4-desktop.is-6-tablet.is-12-mobile"
            )
            
            self.logger.info(f"📦 Found {len(item_columns)} item columns")
            
            stock_numbers = []
            db_inserted_count = 0
            
            for i, column in enumerate(item_columns):
                try:
                    # Look for stock number pattern in the column text
                    column_text = column.text
                    
                    # Find stock numbers using regex pattern
                    stock_matches = re.findall(r'Stock\s+(\d+)', column_text, re.IGNORECASE)
                    
                    for stock_match in stock_matches:
                        stock_numbers.append(stock_match)
                        self.logger.info(f"📋 Found stock number: {stock_match}")
                        
                        # Insert into database
                        if sale_no:
                            try:
                                self.insert_inventory_record(
                                    url=listing_url,
                                    sale_no=sale_no,
                                    sale_date=sale_date,
                                    stock_no=stock_match
                                )
                                db_inserted_count += 1
                            except Exception as db_error:
                                self.logger.error(f"❌ DB insert failed for stock {stock_match}: {db_error}")
                    
                    # Also try to find stock numbers in any format
                    if not stock_matches:
                        # Look for alternative patterns
                        alt_matches = re.findall(r'Stock[:.]?\s*(\d+)', column_text, re.IGNORECASE)
                        for alt_match in alt_matches:
                            stock_numbers.append(alt_match)
                            self.logger.info(f"📋 Found stock number (alt): {alt_match}")
                            
                            # Insert into database
                            if sale_no:
                                try:
                                    self.insert_inventory_record(
                                        url=listing_url,
                                        sale_no=sale_no,
                                        sale_date=sale_date,
                                        stock_no=alt_match
                                    )
                                    db_inserted_count += 1
                                except Exception as db_error:
                                    self.logger.error(f"❌ DB insert failed for stock {alt_match}: {db_error}")
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Error processing column {i}: {e}")
                    continue
            
            self.logger.info(f"✅ Total stock numbers found: {len(stock_numbers)}")
            self.logger.info(f"💾 Database records inserted: {db_inserted_count}")
            
            return stock_numbers
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping stock numbers from {listing_url}: {e}")
            return []
    
    def process_online_auctions(self):
        """
        Process all online auctions and extract stock numbers.
        """
        try:
            # Setup driver
            if not self.setup_driver():
                return
            
            # Read CSV file
            self.logger.info(f"📖 Reading {self.input_file}...")
            df = pd.read_csv(self.input_file)
            self.logger.info(f"📊 Found {len(df)} online auctions to process")
            
            processed_count = 0
            
            for index, row in df.iterrows():
                try:
                    sale_info_url = row['sale_info_url']
                    auction_title = row['title']
                    
                    self.logger.info(f"\n🎯 Processing auction {index + 1}/{len(df)}: {auction_title}")
                    
                    # Step 1: Open sale info page and click VIEW LISTING
                    listing_url = self.find_and_click_view_listing(sale_info_url)
                    
                    if not listing_url:
                        self.logger.warning(f"❌ Failed to get listing URL for: {auction_title}")
                        continue
                    
                    # Step 2: Scrape stock numbers from listing page and store in database
                    stock_numbers = self.scrape_stock_numbers(listing_url, auction_title, sale_info_url)
                    
                    # Store results
                    for stock_number in stock_numbers:
                        self.all_stock_data.append({
                            'auction_title': auction_title,
                            'sale_info_url': sale_info_url,
                            'listing_url': listing_url,
                            'stock_number': stock_number
                        })
                    
                    processed_count += 1
                    self.logger.info(f"✅ Processed {auction_title}: {len(stock_numbers)} stock numbers")
                    
                    # Add delay between auctions
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing auction {index}: {e}")
                    continue
            
            # Save results
            if self.all_stock_data:
                self.save_results()
            else:
                self.logger.warning("❌ No stock numbers were extracted")
            
            self.logger.info(f"🎉 Processing complete! Processed {processed_count}/{len(df)} auctions")
            
        except FileNotFoundError:
            self.logger.error(f"❌ Input file {self.input_file} not found!")
        except Exception as e:
            self.logger.error(f"❌ Error in main processing: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("🔒 Browser closed")
    
    def save_results(self):
        """Save the extracted stock numbers to CSV."""
        try:
            results_df = pd.DataFrame(self.all_stock_data)
            results_df.to_csv(self.output_file, index=False)
            
            self.logger.info(f"💾 Saved {len(self.all_stock_data)} stock numbers to {self.output_file}")
            
            # Display summary
            print(f"\n📊 Stock Numbers Extraction Summary:")
            print(f"   📦 Total stock numbers found: {len(self.all_stock_data)}")
            print(f"   🎯 Unique auctions processed: {len(results_df['auction_title'].unique())}")
            print(f"   💾 CSV results saved to: {self.output_file}")
            print(f"   🗄️ Database: Stock numbers also saved to {self.table_name} table")
            
            # Show sample results
            if len(self.all_stock_data) > 0:
                print(f"\n🔍 Sample Stock Numbers:")
                for i, item in enumerate(self.all_stock_data[:5]):
                    print(f"   {i+1}. Stock {item['stock_number']} - {item['auction_title'][:50]}...")
                
                if len(self.all_stock_data) > 5:
                    print(f"   ... and {len(self.all_stock_data) - 5} more stock numbers")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {e}")

def main():
    """Main function to run the online stock scraper."""
    print("🔢 Starting Online Stock Number Scraper...")
    print("=" * 70)
    
    scraper = OnlineStockScraper()
    scraper.process_online_auctions()
    
    print("=" * 70)
    print("✅ Stock Number Extraction Complete!")

if __name__ == "__main__":
    main()