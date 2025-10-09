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
from datetime import datetime
from pickles_login import PicklesScraper
from logger import get_logger


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
    
    # Display banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                PICKLES MULTI-CATEGORY SCRAPER               ║
║            Generates pickles_auctions_detailed.csv          ║
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
            
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Export files
            print(f"\\n📄 Exporting results...")
            
            # Individual category files (if enabled)
            if config['output']['create_individual_files']:
                categories = set(a['category'] for a in all_auctions)
                for category in categories:
                    cat_auctions = [a for a in all_auctions if a['category'] == category]
                    safe_name = category.lower().replace(" ", "_").replace("&", "and")
                    filename = f"pickles_auctions_{safe_name}_{timestamp}.csv"
                    export_to_csv(cat_auctions, filename)
            
            # Combined file (if enabled)
            if config['output']['create_combined_file']:
                combined_name = config['output']['combined_csv_filename']
                if config['output']['include_timestamp']:
                    name, ext = os.path.splitext(combined_name)
                    combined_name = f"{name}_{timestamp}{ext}"
                export_to_csv(all_auctions, combined_name)
            
            # ALWAYS create the file for step2
            export_to_csv(all_auctions, "pickles_auctions_detailed.csv")
            
            # Summary
            print(f"\\n🎉 COMPLETED!")
            print(f"📊 Total auctions: {len(all_auctions)}")
            
            # Count auctions with registration URLs
            with_registration = [a for a in all_auctions if a['auction_registration']]
            print(f"🎫 With registration URLs: {len(with_registration)}")
            
            print(f"\\n🔗 pickles_auctions_detailed.csv is ready for step2!")
            
            
    except KeyboardInterrupt:
        print("\\n👋 Cancelled by user")
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