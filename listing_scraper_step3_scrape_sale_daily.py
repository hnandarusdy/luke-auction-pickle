#!/usr/bin/env python3
"""
Step 3: Daily Sale Scraper
Scrapes all active sales from pickles_live_schedule where end_sale_date >= current_date
"""

import os
import subprocess
import pandas as pd
from datetime import datetime
from db import MySecondDB
from logger import get_logger


class DailySaleScraper:
    """
    Daily scraper for all active auctions
    """
    
    def __init__(self):
        """Initialize database connection and logger"""
        self.db = MySecondDB()
        self.logger = get_logger("daily_sale_scraper", log_to_file=True)
        self.scraper_script = "listing_scraper_step2_scrape_one_url.py"
        
    def transform_url(self, original_url):
        """
        Transform database URL to search format
        
        From: https://www.pickles.com.au/cars/auction/saleinfo/national-online-motor-vehicle-auction/various-locations--national-national-australia?sale_no=11925
        To: https://www.pickles.com.au/used/search/s/national-online-motor-vehicle-auction/11925
        
        Args:
            original_url (str): Original URL from database
            
        Returns:
            str: Transformed URL for scraping
        """
        try:
            import re
            
            if not original_url:
                return None
            
            # Extract sale_no from URL
            sale_no_match = re.search(r'sale_no=(\d+)', original_url)
            if not sale_no_match:
                self.logger.warning(f"Could not extract sale_no from URL: {original_url}")
                return None
            
            sale_no = sale_no_match.group(1)
            
            # Extract dynamic title from the path
            # Pattern: /auction/saleinfo/TITLE/location-info?sale_no=xxx
            path_match = re.search(r'/auction/saleinfo/([^/]+)/', original_url)
            if not path_match:
                self.logger.warning(f"Could not extract title from URL: {original_url}")
                return None
            
            dynamic_title = path_match.group(1)
            
            # Build the new URL format
            new_url = f"https://www.pickles.com.au/used/search/s/{dynamic_title}/{sale_no}"
            
            self.logger.info(f"Transformed URL: {original_url} -> {new_url}")
            return new_url
            
        except Exception as e:
            self.logger.error(f"Error transforming URL {original_url}: {str(e)}")
            return None
        
    def get_active_sales(self):
        """
        Query active sales from pickles_live_schedule where end_sale_date >= current_date
        
        Returns:
            DataFrame: Active sales data
        """
        try:
            query = """
            SELECT *
            FROM pickles_live_schedule
            WHERE end_sale_date >= CURDATE()
            ORDER BY end_sale_date ASC
            """
            
            print("📊 Querying active sales from database...")
            self.logger.info("Querying active sales from pickles_live_schedule")
            
            df = self.db.read_sql(query)
            
            if df.empty:
                print("❌ No active sales found in database")
                self.logger.warning("No active sales found")
                return df
            
            print(f"✅ Found {len(df)} active sales")
            self.logger.info(f"Found {len(df)} active sales")
            
            # Display summary
            print(f"📋 Sales summary:")
            print(f"   🎪 Total sales: {len(df)}")
            if 'auction_type' in df.columns:
                auction_types = df['auction_type'].value_counts()
                for auction_type, count in auction_types.items():
                    print(f"   📝 {auction_type}: {count}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error querying active sales: {str(e)}")
            print(f"❌ Error querying database: {str(e)}")
            return pd.DataFrame()
    
    def clean_url(self, url):
        """
        Clean malformed URLs by fixing double question marks
        
        Args:
            url (str): URL to clean
            
        Returns:
            str: Cleaned URL
        """
        if not url:
            return url
        
        try:
            # Fix double question marks - replace second ? with &
            if url.count('?') > 1:
                # Find the first ? and replace subsequent ? with &
                parts = url.split('?')
                if len(parts) > 2:
                    # Join first part with ?, then join rest with &
                    cleaned_url = parts[0] + '?' + '&'.join(parts[1:])
                    print(f"       🔧 Fixed malformed URL:")
                    print(f"           Before: {url}")
                    print(f"           After:  {cleaned_url}")
                    self.logger.info(f"Fixed malformed URL: {url} -> {cleaned_url}")
                    return cleaned_url
            
            return url
            
        except Exception as e:
            self.logger.error(f"Error cleaning URL {url}: {str(e)}")
            return url
    
    def scrape_sale_url(self, sale_info_url, sale_title="", index=0, total=0):
        """
        Run the scraper script for a specific sale URL
        
        Args:
            sale_info_url (str): URL to scrape
            sale_title (str): Title for logging
            index (int): Current index for progress
            total (int): Total number of sales
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not sale_info_url:
                print(f"   ⚠️ [{index+1}/{total}] No URL provided, skipping...")
                return False
            
            # Transform the URL from database format to search format
            transformed_url = self.transform_url(sale_info_url)
            if not transformed_url:
                print(f"   ❌ [{index+1}/{total}] Failed to transform URL: {sale_info_url}")
                return False
            
            print(f"   🔗 [{index+1}/{total}] Original: {sale_info_url}")
            print(f"       🔄 Transformed: {transformed_url}")
            if sale_title:
                print(f"       📋 Title: {sale_title[:80]}...")
            
            # Build command
            command = [
                "python", 
                self.scraper_script, 
                transformed_url
            ]
            
            self.logger.info(f"Executing scraper for transformed URL: {transformed_url} (original: {sale_info_url})")
            
            # Execute the scraper script with stdin redirected to avoid pauses
            result = subprocess.run(
                command, 
                capture_output=True, 
                text=True, 
                cwd=os.getcwd(),
                timeout=5,  # 1 minute timeout (reduced for batch processing)
                encoding='utf-8',  # Force UTF-8 encoding
                errors='replace',  # Replace unicode errors instead of failing
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},  # Set Python encoding
                stdin=subprocess.DEVNULL  # Redirect stdin to avoid input() pauses
            )
            
            if result.returncode == 0:
                print(f"       ✅ Success!")
                self.logger.info(f"Successfully scraped: {transformed_url} (original: {sale_info_url})")
                return True
            else:
                print(f"       ❌ Failed (exit code {result.returncode})")
                if result.stderr:
                    print(f"       📝 Error: {result.stderr[:200]}...")
                self.logger.error(f"Scraper failed for {transformed_url} (original: {sale_info_url}): {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"       ⏱️ Timeout after 1 minute")
            self.logger.error(f"Timeout scraping {transformed_url} (original: {sale_info_url})")
            return False
        except Exception as e:
            print(f"       ❌ Error: {str(e)}")
            self.logger.error(f"Error scraping {sale_info_url}: {str(e)}")
            return False
    
    def process_all_sales(self):
        """
        Process all active sales by running scraper for each sale_info_url
        
        Returns:
            dict: Summary statistics
        """
        try:
            # Get active sales
            df = self.get_active_sales()
            
            if df.empty:
                return {
                    'total_sales': 0,
                    'successful': 0,
                    'failed': 0,
                    'skipped': 0
                }
            
            print(f"\n🚀 Starting to scrape {len(df)} active sales...")
            print("=" * 60)
            
            successful = 0
            failed = 0
            skipped = 0
            
            for index, row in df.iterrows():
                sale_info_url = row.get('sale_info_url', '')
                sale_title = row.get('title', '')
                
                print(f"\n📋 Processing sale {index + 1}/{len(df)}")
                
                if not sale_info_url or pd.isna(sale_info_url):
                    print(f"   ⚠️ No sale_info_url found, skipping...")
                    skipped += 1
                    continue
                
                # Scrape the sale
                success = self.scrape_sale_url(
                    sale_info_url=sale_info_url,
                    sale_title=sale_title,
                    index=index,
                    total=len(df)
                )
                
                if success:
                    successful += 1
                else:
                    failed += 1
            
            # Summary statistics
            summary = {
                'total_sales': len(df),
                'successful': successful,
                'failed': failed,
                'skipped': skipped
            }
            
            print("\n" + "=" * 60)
            print("📊 DAILY SCRAPING SUMMARY")
            print("=" * 60)
            print(f"📋 Total sales processed: {summary['total_sales']}")
            print(f"✅ Successful: {summary['successful']}")
            print(f"❌ Failed: {summary['failed']}")
            print(f"⏭️ Skipped: {summary['skipped']}")
            
            success_rate = (successful / len(df) * 100) if len(df) > 0 else 0
            print(f"📈 Success rate: {success_rate:.1f}%")
            
            self.logger.info(f"Daily scraping completed - Total: {len(df)}, Success: {successful}, Failed: {failed}, Skipped: {skipped}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error in process_all_sales: {str(e)}")
            print(f"❌ Error processing sales: {str(e)}")
            return {
                'total_sales': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'error': str(e)
            }


def main():
    """Main function"""
    # Display banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    DAILY SALE SCRAPER                       ║
║             Scrapes all active sales daily                  ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    
    # Initialize scraper
    scraper = DailySaleScraper()
    
    try:
        # Process all active sales
        summary = scraper.process_all_sales()
        
        # Determine success based on results
        if summary.get('error'):
            print(f"\n❌ Daily scraping failed with error!")
            return 1
        elif summary['total_sales'] == 0:
            print(f"\n⚠️ No active sales found to process")
            return 0
        elif summary['successful'] > 0:
            print(f"\n✅ Daily scraping completed successfully!")
            print(f"🎯 Processed {summary['successful']}/{summary['total_sales']} sales successfully")
            return 0
        else:
            print(f"\n❌ Daily scraping completed but all sales failed!")
            return 1
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        scraper.logger.error(f"Unexpected error in main: {str(e)}")
        return 1
    
    finally:
        scraper.db.close()


if __name__ == "__main__":
    import sys
    sys.exit(main())
