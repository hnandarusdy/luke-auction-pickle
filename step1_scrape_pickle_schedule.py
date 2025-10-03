import sys
import csv
import time
from datetime import datetime
from pickles_login import PicklesScraper
from logger import get_logger


def main():
    """Main application function that handles the Pickles login workflow."""
    # Initialize logger
    logger = get_logger("main_app", log_to_file=True)
    logger.info("=== Pickles Website Login Application Started ===")
    
    # Credentials
    USERNAME = "hnandarusdy2@gmail.com"
    PASSWORD = "123qwe!@#QWE"
    
    try:
        logger.info("Initializing Pickles scraper...")
        
        # Using context manager for automatic cleanup
        with PicklesScraper(headless=False, wait_timeout=15) as scraper:
            logger.info("Starting login process...")
            print("🚀 Starting Pickles login process...")
            
            # Attempt login
            success = scraper.login(USERNAME, PASSWORD)
            
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
                print("\n" + "="*50)
                print("🏛️ Starting auction scraping workflow...")
                logger.info("Starting auction scraping workflow")
                
                # Navigate to salvage auctions page
                auction_url = "https://www.pickles.com.au/upcoming-auctions/salvage"
                print(f"🔗 Navigating to: {auction_url}")
                
                if scraper.navigate_to_auction_page(auction_url):
                    print("✅ Successfully navigated to auction page")
                    
                    # Extract auction details
                    print("🔍 Extracting auction details...")
                    auctions = scraper.extract_auction_details()
                    
                    if auctions:
                        print(f"🎯 Found {len(auctions)} auctions")
                        
                        # Display auction list
                        print("\n📋 Available Auctions:")
                        print("-" * 60)
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
                        print("\n🔍 Collecting detailed information from Sale Info pages...")
                        enhanced_auctions = []
                        
                        for idx, auction in enumerate(auctions, 1):
                            enhanced_auction = auction.copy()
                            
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
                                    time.sleep(2)
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
                        
                        # Export enhanced auction data to CSV
                        
                        
                        csv_filename = f"pickles_auctions_detailed.csv"
                        
                        try:
                            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                                fieldnames = ['title', 'location', 'status', 'sale_info_url', 'auction_registration', 'sale_title', 'sale_date', 'sale_occurs']
                                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                                
                                # Write header
                                writer.writeheader()
                                
                                # Write enhanced auction data
                                for auction in enhanced_auctions:
                                    writer.writerow(auction)
                                
                            print(f"\n📄 Enhanced auction data exported to: {csv_filename}")
                            logger.info(f"Enhanced auction data exported to CSV: {csv_filename}")
                            
                            # Display summary
                            auctions_with_sale_info = [a for a in enhanced_auctions if a['sale_info_url']]
                            auctions_with_registration = [a for a in enhanced_auctions if a['auction_registration']]
                            print(f"\n📊 Export Summary:")
                            print(f"   📋 Total auctions: {len(enhanced_auctions)}")
                            print(f"   🔗 Auctions with Sale Info URLs: {len(auctions_with_sale_info)}")
                            print(f"   🎫 Auctions with Registration URLs: {len(auctions_with_registration)}")
                            print(f"   ❌ Auctions without Sale Info URLs: {len(enhanced_auctions) - len(auctions_with_sale_info)}")
                            
                        except Exception as e:
                            print(f"❌ Failed to export CSV: {str(e)}")
                            logger.error(f"Failed to export CSV: {str(e)}")
                    else:
                        print("⚠️ No auctions found on the page")
                else:
                    print("❌ Failed to navigate to auction page")
                
                # Wait for user input before continuing
                print("\n" + "="*50)
                print("🎉 Login process completed successfully!")
                print("You can now perform additional scraping operations...")
                
                # User pause - this is what you requested
                try:
                    x = input("\n⏸️  Press Enter to continue (or Ctrl+C to exit): ")
                    logger.info("User pressed Enter to continue")
                    print("\n✨ Ready for additional operations!")
                    
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
                print("   - Login credentials")
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
        print("   - Review log files in the 'logs' directory")
        
        return 1  # Exit code for error
    
    finally:
        logger.info("=== Pickles Website Login Application Ended ===")
        print("\n📋 Check the 'logs' directory for detailed execution logs.")
    
    return 0  # Success exit code





if __name__ == "__main__":
    # Run main application
    exit_code = main()
    
    # Exit with appropriate code
    sys.exit(exit_code)