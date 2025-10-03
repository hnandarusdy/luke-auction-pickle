import sys
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
                
                # Take a screenshot as proof of successful login
                screenshot_filename = "login_success.png"
                if scraper.take_screenshot(screenshot_filename):
                    print(f"📸 Screenshot saved: {screenshot_filename}")
                    logger.info(f"Screenshot saved: {screenshot_filename}")
                
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
                
                # Take a screenshot for debugging
                screenshot_filename = "login_failed.png"
                if scraper.take_screenshot(screenshot_filename):
                    print(f"📸 Debug screenshot saved: {screenshot_filename}")
                    logger.info(f"Debug screenshot saved: {screenshot_filename}")
                
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

    """
    once logged in successfully, I want to: 
1. redirect to https://www.pickles.com.au/upcoming-auctions/cars-motorcycles
2. look at sample html downloaded C:\workZ\airtasker\Luke-Python\luke-pickles\sample_html\auction_schedule.html

3. i
"""