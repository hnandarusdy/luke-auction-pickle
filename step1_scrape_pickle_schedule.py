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


def format_filename(template, category_name="", timestamp=""):
    """Format filename based on template and variables."""
    # Clean category name for filename
    safe_category = category_name.lower().replace(" ", "_").replace("&", "and").replace(",", "")
    
    return template.format(
        category=safe_category,
        timestamp=timestamp
    )


def scrape_auction_category(scraper, category, config, logger):
    """Scrape a single auction category."""
    category_name = category['name']
    category_url = category['url']
    
    print(f"\n🏛️ Processing category: {category_name}")
    print(f"🔗 URL: {category_url}")
    logger.info(f"Starting to scrape category: {category_name}")
    
    # Navigate to auction page
    if not scraper.navigate_to_auction_page(category_url):
        print(f"❌ Failed to navigate to {category_name} page")
        logger.error(f"Failed to navigate to {category_name} page")
        return []
    
    print(f"✅ Successfully navigated to {category_name} page")
    
    # Extract auction details
    print("🔍 Extracting auction details...")
    auctions = scraper.extract_auction_details()
    
    if not auctions:
        print(f"⚠️ No auctions found in {category_name}")
        logger.warning(f"No auctions found in {category_name}")
        return []
    
    print(f"🎯 Found {len(auctions)} auctions in {category_name}")
    
    # Display auction list
    print(f"\n📋 Available Auctions in {category_name}:")
    print("-" * 70)
    for idx, auction in enumerate(auctions, 1):
        print(f"{idx}. {auction['title']}")
        print(f"   📍 Location: {auction['location']}")
        print(f"   📊 Status: {auction['status']}")
        if auction['sale_info_url']:
            print(f"   🔗 Sale Info Available: ✅")
        else:
            print(f"   🔗 Sale Info Available: ❌")
        print()
    
    # Collect detailed information from each Sale Info page
    print(f"\n🔍 Collecting detailed information from {category_name} Sale Info pages...")
    enhanced_auctions = []
    
    for idx, auction in enumerate(auctions, 1):
        enhanced_auction = auction.copy()
        enhanced_auction['category'] = category_name  # Add category field
        
        if auction['sale_info_url']:
            print(f"📄 Processing {idx}/{len(auctions)}: {auction['title']}")
            
            # Navigate to Sale Info page with gentle delay
            if scraper.navigate_to_sale_info(auction['sale_info_url']):
                # Extract additional details
                sale_details = scraper.extract_sale_info_details()
                
                # Add the new fields to the auction data
                enhanced_auction.update(sale_details)
                
                print(f"   ✅ Collected additional details")
                if sale_details['auction_registration']:
                    print(f"   🎫 Registration URL found")
                if sale_details['sale_title']:
                    print(f"   📋 Title: {sale_details['sale_title']}")
                
                # Gentle delay between requests
                time.sleep(config['scraper']['delay_between_requests'])
            else:
                print(f"   ❌ Failed to load Sale Info page")
                # Set default values for failed pages
                enhanced_auction.update({
                    'auction_registration': None,
                    'sale_title': None,
                    'sale_date': None,
                    'sale_occurs': None
                })
        else:
            print(f"⏭️  Skipping {idx}/{len(auctions)}: {auction['title']} (No Sale Info URL)")
            # Set default values for auctions without Sale Info URLs
            enhanced_auction.update({
                'auction_registration': None,
                'sale_title': None,
                'sale_date': None,
                'sale_occurs': None
            })
        
        enhanced_auctions.append(enhanced_auction)
    
    logger.info(f"Completed scraping {category_name}: {len(enhanced_auctions)} auctions processed")
    return enhanced_auctions


def export_to_csv(auctions, filename, logger):
    """Export auction data to CSV file."""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['category', 'title', 'location', 'status', 'sale_info_url', 'auction_registration', 'sale_title', 'sale_date', 'sale_occurs']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write auction data
            for auction in auctions:
                writer.writerow(auction)
            
        print(f"📄 Data exported to: {filename}")
        logger.info(f"Data exported to CSV: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to export CSV {filename}: {str(e)}")
        logger.error(f"Failed to export CSV {filename}: {str(e)}")
        return False


def main():
    """Main application function that handles the Pickles scraping workflow."""
    # Load configuration
    config = load_config()
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Initialize logger
    logger = get_logger("main_app", log_to_file=config['logging']['log_to_file'])
    logger.info("=== Pickles Auction Scraper Started ===")
    logger.info(f"Configuration loaded: {len(config['auction_categories'])} categories configured")
    
    # Get credentials from config
    username = config['credentials']['username']
    password = config['credentials']['password']
    
    print("🚀 Starting Pickles Auction Scraper...")
    print(f"📊 Configured to scrape {len([c for c in config['auction_categories'] if c['enabled']])} categories")
    
    try:
        logger.info("Initializing Pickles scraper...")
        
        # Using context manager for automatic cleanup
        scraper_config = config['scraper']
        with PicklesScraper(
            headless=scraper_config['headless'], 
            wait_timeout=scraper_config['wait_timeout']
        ) as scraper:
            logger.info("Starting login process...")
            print("� Starting login process...")
            
            # Attempt login
            success = scraper.login(username, password)
            
            if success:
                # Login successful
                print("✅ Login successful!")
                logger.info("Login completed successfully")
                
                # Display current status
                current_url = scraper.get_current_url()
                page_title = scraper.get_page_title()
                
                print(f"📍 Current URL: {current_url}")
                print(f"📄 Page title: {page_title}")
                
                logger.info(f"Current URL: {current_url}")
                logger.info(f"Page title: {page_title}")
                
                # Start auction scraping workflow
                print("\n" + "="*70)
                print("🏛️ Starting multi-category auction scraping workflow...")
                logger.info("Starting multi-category auction scraping workflow")
                
                all_auctions = []  # Store all auctions from all categories
                category_results = {}  # Store results per category
                
                # Get enabled categories
                enabled_categories = [cat for cat in config['auction_categories'] if cat['enabled']]
                
                # Loop through each enabled auction category
                for idx, category in enumerate(enabled_categories, 1):
                    print(f"\n{'='*70}")
                    print(f"� Processing Category {idx}/{len(enabled_categories)}")
                    
                    # Scrape the category
                    category_auctions = scrape_auction_category(scraper, category, config, logger)
                    
                    # Store results
                    category_results[category['name']] = category_auctions
                    all_auctions.extend(category_auctions)
                    
                    # Export individual category CSV if enabled
                    if config['output']['create_individual_files'] and category_auctions:
                        individual_filename = format_filename(
                            config['output']['csv_filename_template'],
                            category['name'],
                            timestamp if config['output']['include_timestamp'] else ""
                        )
                        export_to_csv(category_auctions, individual_filename, logger)
                    
                    # Delay between categories (except for the last one)
                    if idx < len(enabled_categories):
                        delay = scraper_config['delay_between_categories']
                        print(f"⏳ Waiting {delay} seconds before next category...")
                        time.sleep(delay)
                
                # Export combined CSV if enabled
                if config['output']['create_combined_file'] and all_auctions:
                    combined_filename = config['output']['combined_csv_filename']
                    if config['output']['include_timestamp']:
                        name, ext = os.path.splitext(combined_filename)
                        combined_filename = f"{name}_{timestamp}{ext}"
                    
                    export_to_csv(all_auctions, combined_filename, logger)
                
                # Display final summary
                print(f"\n{'='*70}")
                print("🎉 SCRAPING COMPLETED!")
                print(f"📊 Final Summary:")
                print(f"   📂 Categories processed: {len(enabled_categories)}")
                print(f"   � Total auctions found: {len(all_auctions)}")
                
                for category_name, auctions in category_results.items():
                    auctions_with_sale_info = [a for a in auctions if a['sale_info_url']]
                    auctions_with_registration = [a for a in auctions if a['auction_registration']]
                    print(f"\n   � {category_name}:")
                    print(f"      📋 Total auctions: {len(auctions)}")
                    print(f"      🔗 With Sale Info URLs: {len(auctions_with_sale_info)}")
                    print(f"      🎫 With Registration URLs: {len(auctions_with_registration)}")
                
                logger.info(f"Scraping completed successfully. Total auctions: {len(all_auctions)}")
                
                # Wait for user input before continuing
                print(f"\n{'='*70}")
                print("� All categories have been processed successfully!")
                print("📄 Check the generated CSV files for detailed auction data.")
                
                # User pause
                try:
                    x = input("\n⏸️  Press Enter to exit (or Ctrl+C to force exit): ")
                    logger.info("User pressed Enter to exit")
                    print("\n👋 Exiting gracefully...")
                    
                except KeyboardInterrupt:
                    print("\n\n👋 User cancelled. Exiting gracefully...")
                    logger.info("User cancelled with Ctrl+C")
                    
            else:
                # Login failed
                print("❌ Login failed!")
                logger.error("Login process failed")
                
                print("\n🔍 Please check:")
                print("   - Your internet connection")
                print("   - Website availability")
                print("   - Login credentials in config.yaml")
                print("   - Website structure changes")
                
                return 1  # Exit code for failure
                
    except KeyboardInterrupt:
        print("\n\n👋 Application interrupted by user. Exiting...")
        logger.info("Application interrupted by user")
        return 0
        
    except Exception as e:
        error_msg = f"Unexpected error occurred: {str(e)}"
        print(f"\n💥 {error_msg}")
        logger.exception(error_msg)
        
        print("\n🆘 Troubleshooting tips:")
        print("   - Check if Chrome browser is installed")
        print("   - Verify internet connection")
        print("   - Check if website is accessible manually")
        print("   - Review config.yaml settings")
        print("   - Review log files in the 'logs' directory")
        
        return 1  # Exit code for error
    
    finally:
        logger.info("=== Pickles Auction Scraper Ended ===")
        print("\n📋 Check the 'logs' directory for detailed execution logs.")
        print("📄 Check the generated CSV files for auction data.")
    
    return 0  # Success exit code


def display_banner():
    """Display application banner."""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                PICKLES MULTI-CATEGORY SCRAPER               ║
║                    Automated Auction Tool                   ║
║                                                              ║
║  • Scrapes all configured auction categories                ║
║  • Exports individual and combined CSV files                ║
║  • Configurable via config.yaml                             ║
║                                                              ║
║  Author: GitHub Copilot                                     ║
║  Date: October 6, 2025                                      ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)





if __name__ == "__main__":
    # Display banner
    display_banner()
    
    # Check if config file exists
    if not os.path.exists("config.yaml"):
        print("❌ config.yaml file not found!")
        print("Please make sure config.yaml is in the same directory as this script.")
        sys.exit(1)
    
    # Run main application
    exit_code = main()
    
    # Exit with appropriate code
    sys.exit(exit_code)