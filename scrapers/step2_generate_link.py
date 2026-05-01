#!/usr/bin/env python3
"""
Step 2: Generate Watch URLs from registration URLs.
Converts registration URLs to watch URLs using URL pattern (no browser needed).
"""

import re
import time
import csv
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.whatsapp_notifier import with_error_notification
from utils.logger import get_logger


class AuctionWatchScraper:
    """Extract watch URLs from auction registration pages using URL pattern conversion."""

    def __init__(self):
        self.logger = get_logger("auction_watch_scraper", log_to_file=True)

    def parse_auction_date(self, date_str: str) -> Optional[date]:
        """Parse auction date from various formats."""
        if not date_str or pd.isna(date_str):
            return None
        try:
            date_str = date_str.strip()
            if " - " in date_str:
                start_part = date_str.split(" - ")[0].strip()
                return self.parse_auction_date(start_part)

            timezone_abbreviations = ['AEST', 'AEDT', 'AWST', 'ACST', 'ACDT']
            for tz in timezone_abbreviations:
                if date_str.endswith(f' {tz}'):
                    date_str = date_str[:-len(f' {tz}')].strip()
                    break

            formats = [
                "%A %d/%m/%Y %I:%M%p", "%d/%m/%Y %I:%M%p",
                "%A %d/%m/%Y", "%d/%m/%Y",
                "%A %d/%m/%Y %H:%M", "%d/%m/%Y %H:%M",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue

            date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match = re.search(date_pattern, date_str)
            if match:
                day, month, year = match.groups()
                try:
                    return date(int(year), int(month), int(day))
                except ValueError:
                    pass

            self.logger.warning(f"Could not parse date: {date_str}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {str(e)}")
            return None

    def read_and_filter_csv(self) -> List[str]:
        """Read CSV and filter for today/past dates with auction_registration URLs."""
        try:
            print("📄 Reading pickles_auctions_detailed.csv...")
            df = pd.read_csv("pickles_auctions_detailed.csv")
            print(f"   📊 Total auctions found: {len(df)}")

            today = date.today()
            print(f"   📅 Filtering for current/past dates (today: {today})")

            df['parsed_date'] = df['sale_date'].apply(self.parse_auction_date)
            date_filtered = df[(df['parsed_date'].notna()) & (df['parsed_date'] <= today)]
            print(f"   📅 Auctions with current/past dates: {len(date_filtered)}")

            registration_filtered = date_filtered[
                df['auction_registration'].notna() &
                (df['auction_registration'].str.strip() != '')
            ]
            print(f"   🎫 Auctions with registration URLs: {len(registration_filtered)}")

            registration_urls = registration_filtered['auction_registration'].tolist()
            print(f"   ✅ Final filtered URLs: {len(registration_urls)}")

            for i, url in enumerate(registration_urls, 1):
                print(f"   {i}. {url}")

            return registration_urls
        except Exception as e:
            print(f"❌ Error reading/filtering CSV: {str(e)}")
            self.logger.error(f"Error reading CSV: {str(e)}")
            return []

    def convert_registration_to_watch_url(self, registration_url: str) -> Optional[str]:
        """Convert registration URL to watch URL using URL pattern."""
        try:
            sale_id_match = re.search(r'saleId=(\d+)', registration_url)
            if not sale_id_match:
                print(f"   ❌ Could not extract saleId from: {registration_url}")
                return None

            sale_id = sale_id_match.group(1)
            watch_url = (
                f"https://www.pickles.com.au/group/pickles/bidding/pickles-live/launch?"
                f"p_p_id=PicklesLiveRedirectPortlet_WAR_PWRWeb&"
                f"p_p_lifecycle=1&p_p_state=normal&p_p_mode=view&"
                f"p_p_col_id=main-content&p_p_col_pos=1&p_p_col_count=2&"
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
        """Save results to CSV file."""
        try:
            csv_filename = "pickles_auction_step2.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['auction_registration', 'auction_watch_url']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for result in results:
                    writer.writerow(result)

            print(f"💾 Results saved to: {csv_filename}")
            successful = [r for r in results if r['auction_watch_url']]
            print(f"✅ Successful: {len(successful)} | ❌ Failed: {len(results) - len(successful)}")
        except Exception as e:
            print(f"❌ Error saving CSV: {str(e)}")
            self.logger.error(f"Error saving CSV: {str(e)}")

    def run(self):
        """Main execution method."""
        print("🚀 Starting Auction Watch URL Generation...")
        try:
            print("\n2️⃣ Reading and filtering CSV...")
            registration_urls = self.read_and_filter_csv()
            if not registration_urls:
                print("❌ No registration URLs found. Exiting.")
                return

            print(f"\n3️⃣ Converting {len(registration_urls)} registration URLs to watch URLs...")
            results = []
            for i, reg_url in enumerate(registration_urls, 1):
                print(f"\n📋 Processing {i}/{len(registration_urls)}:")
                watch_url = self.convert_registration_to_watch_url(reg_url)
                results.append({'auction_registration': reg_url, 'auction_watch_url': watch_url})

            print(f"\n4️⃣ Saving results...")
            self.save_results_to_csv(results)
            print("\n🎉 Process completed successfully!")
        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            self.logger.error(f"Unexpected error in run(): {str(e)}")


@with_error_notification()
def main():
    scraper = AuctionWatchScraper()
    scraper.run()


if __name__ == "__main__":
    main()
