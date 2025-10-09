#!/usr/bin/env python3
"""
Step 1: Multi-Category Pickles Auction Scraper

This script scrapes multiple auction categories from the Pickles website
and generates CSV files including the required pickles_auctions_detailed.csv
for step2 processing.

Author: GitHub Copilot
Date: October 6, 2025
"""

import sys
import csv
import time
import yaml
import os
import pandas as pd
from datetime import datetime
from pickles_login import PicklesScraper
from logger import get_logger
from db import MySecondDB


class PicklesLiveScheduleDB:
    """
    Database operations for pickles_live_schedule table
    """
    
    def __init__(self):
        """Initialize database connection"""
        self.db = MySecondDB()
        self.logger = get_logger("live_schedule_db", log_to_file=True)
    
    def auction_exists(self, sale_info_url):
        """Check if an auction with this sale_info_url already exists"""
        try:
            if not sale_info_url:
                return False
                
            query = "SELECT COUNT(*) as count FROM pickles_live_schedule WHERE sale_info_url = %s"
            df = self.db.read_sql(query, params=[sale_info_url])
            count = df['count'].iloc[0] if not df.empty else 0
            return count > 0
        except Exception as e:
            self.logger.warning(f"Error checking auction existence: {str(e)}")
            return False
    
    def insert_auctions(self, auctions_data):
        """Insert auction data into database"""
        try:
            if not auctions_data:
                return 0
            
            # Filter out auctions that already exist
            new_auctions = []
            skipped_count = 0
            
            for auction in auctions_data:
                if self.auction_exists(auction.get('sale_info_url')):
                    skipped_count += 1
                    self.logger.info(f"Skipping duplicate auction: {auction.get('title', 'Unknown')[:50]}")
                else:
                    new_auctions.append(auction)
            
            if not new_auctions:
                print(f"⚠️ All {len(auctions_data)} auctions already exist in database - skipping insert")
                return 0
            
            # Convert to DataFrame and insert
            df = pd.DataFrame(new_auctions)
            
            # Ensure columns match table structure
            expected_columns = ['category', 'title', 'location', 'status', 'sale_info_url', 
                              'auction_registration', 'sale_title', 'sale_date', 'sale_occurs']
            
            # Add missing columns with None values
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Select only the expected columns
            df = df[expected_columns]
            
            # Insert into database
            result = self.db.write_to_sql(df, 'pickles_live_schedule', how='append')
            
            inserted_count = len(new_auctions)
            print(f"✅ Inserted {inserted_count} new auctions into database")
            print(f"⏭️ Skipped {skipped_count} duplicate auctions")
            
            self.logger.info(f"Inserted {inserted_count} auctions, skipped {skipped_count} duplicates")
            
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error inserting auctions: {str(e)}")
            print(f"❌ Database insert failed: {str(e)}")
            raise


def load_config(config_path="config.yaml"):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_path}' not found!")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"❌ Error parsing configuration file: {e}")
        sys.exit(1)


def scrape_all_categories(scraper, config, logger):
    """Scrape all enabled auction categories."""
    all_auctions = []
    enabled_categories = [cat for cat in config['auction_categories'] if cat['enabled']]
    
    print(f"📊 Processing {len(enabled_categories)} categories...")
    
    for idx, category in enumerate(enabled_categories, 1):
        category_name = category['name']
        category_url = category['url']
        
        print(f"\\n📂 Category {idx}/{len(enabled_categories)}: {category_name}")
        print(f"🔗 URL: {category_url}")
        
        # Navigate to auction page
        if not scraper.navigate_to_auction_page(category_url):
            print(f"❌ Failed to navigate to {category_name}")
            continue
        
        print(f"✅ Navigated to {category_name}")
        
        # Extract auction details
        auctions = scraper.extract_auction_details()
        
        if not auctions:
            print(f"⚠️ No auctions found in {category_name}")
            continue
        
        print(f"🎯 Found {len(auctions)} auctions")
        
        # Process each auction for detailed info
        for i, auction in enumerate(auctions, 1):
            auction['category'] = category_name
            
            if auction['sale_info_url']:
                print(f"   📄 Processing {i}/{len(auctions)}: {auction['title'][:50]}...")
                
                if scraper.navigate_to_sale_info(auction['sale_info_url']):
                    sale_details = scraper.extract_sale_info_details()
                    auction.update(sale_details)
                    
                    if sale_details['auction_registration']:
                        print(f"      ✅ Registration URL found")
                else:
                    # Set defaults for failed pages
                    auction.update({
                        'auction_registration': None,
                        'sale_title': None,
                        'sale_date': None,
                        'sale_occurs': None
                    })
                
                time.sleep(config['scraper']['delay_between_requests'])
            else:
                # Set defaults for auctions without sale info
                auction.update({
                    'auction_registration': None,
                    'sale_title': None,
                    'sale_date': None,
                    'sale_occurs': None
                })
        
        all_auctions.extend(auctions)
        
        # Delay between categories
        if idx < len(enabled_categories):
            delay = config['scraper']['delay_between_categories']
            print(f"⏳ Waiting {delay}s before next category...")
            time.sleep(delay)
    
    return all_auctions


def export_to_csv(auctions, filename):
    """Export auction data to CSV file."""
    try:
        fieldnames = ['category', 'title', 'location', 'status', 'sale_info_url', 
                     'auction_registration', 'sale_title', 'sale_date', 'sale_occurs']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for auction in auctions:
                writer.writerow(auction)
        
        print(f"📄 Exported {len(auctions)} auctions to: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {str(e)}")
        return False


def main():
    """Main function."""
    # Load configuration
    config = load_config()
    
    # Initialize logger
    logger = get_logger("pickles_scraper", log_to_file=True)
    logger.info("=== Multi-Category Pickles Scraper Started ===")
    
    # Initialize database handler
    db_handler = PicklesLiveScheduleDB()
    
    # Display banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                PICKLES MULTI-CATEGORY SCRAPER               ║
║        Generates pickles_auctions_detailed.csv & DB         ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    
    # Get credentials
    username = config['credentials']['username']
    password = config['credentials']['password']
    
    print("🚀 Starting scraper...")
    
    try:
        # Setup scraper
        scraper_config = config['scraper']
        with PicklesScraper(
            headless=scraper_config['headless'],
            wait_timeout=scraper_config['wait_timeout']
        ) as scraper:
            
            # Login
            print("🔐 Logging in...")
            if not scraper.login(username, password):
                print("❌ Login failed!")
                return 1
            
            print("✅ Login successful!")
            
            # Scrape all categories
            all_auctions = scrape_all_categories(scraper, config, logger)
            
            if not all_auctions:
                print("❌ No auctions found!")
                return 1
            
            print(f"\n📄 Exporting results...")
            
            # Export to CSV (required for step2)
            if export_to_csv(all_auctions, "pickles_auctions_detailed.csv"):
                print(f"✅ CSV export successful")
            else:
                print(f"❌ CSV export failed")
                return 1
            
            # Insert into database
            print(f"\n💾 Inserting data into database...")
            try:
                inserted_count = db_handler.insert_auctions(all_auctions)
                print(f"✅ Database operations completed")
            except Exception as e:
                print(f"❌ Database insert failed: {str(e)}")
                logger.error(f"Database insert error: {str(e)}")
                # Don't return error - CSV is still created for step2
            
            # Summary
            print(f"\n🎉 COMPLETED!")
            print(f"📊 Total auctions scraped: {len(all_auctions)}")
            
            # Count auctions with registration URLs
            with_registration = [a for a in all_auctions if a['auction_registration']]
            print(f"🎫 With registration URLs: {len(with_registration)}")
            
            print(f"\n🔗 pickles_auctions_detailed.csv is ready for step2!")
            print(f"💾 Live schedule data saved to pickles_live_schedule table")
            
            
    except KeyboardInterrupt:
        print("\n👋 Cancelled by user")
    except Exception as e:
        print(f"💥 Error: {str(e)}")
        logger.error(f"Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    if not os.path.exists("config.yaml"):
        print("❌ config.yaml not found!")
        sys.exit(1)
    
    sys.exit(main())