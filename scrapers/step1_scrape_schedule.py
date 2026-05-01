#!/usr/bin/env python3
"""
Step 1: Multi-Category Pickles Auction Scraper

Scrapes multiple auction categories from the Pickles website
and generates CSV files + inserts into database.
"""

import sys
import csv
import time
import yaml
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime

# Setup path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.browser import PicklesBrowser
from utils.logger import get_logger
from db.connection import MySecondDB
from utils.duplicate_cleaner import DuplicateCleaner
from utils.whatsapp_notifier import with_error_notification


class PicklesLiveScheduleDB:
    """Database operations for pickles_live_schedule table"""

    def __init__(self):
        self.db = MySecondDB()
        self.logger = get_logger("live_schedule_db", log_to_file=True)

    def auction_exists(self, sale_info_url):
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
        try:
            if not auctions_data:
                return 0

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

            df = pd.DataFrame(new_auctions)

            expected_columns = ['category', 'title', 'location', 'status', 'sale_info_url',
                              'auction_registration', 'sale_title', 'sale_date', 'sale_occurs', 'auction_type',
                              'start_sale_date', 'end_sale_date']

            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None

            df = df[expected_columns]
            self.db.write_to_sql(df, 'pickles_live_schedule', how='append')

            inserted_count = len(new_auctions)
            print(f"✅ Inserted {inserted_count} new auctions into database")
            print(f"⏭️ Skipped {skipped_count} duplicate auctions")
            self.logger.info(f"Inserted {inserted_count} auctions, skipped {skipped_count} duplicates")
            return inserted_count

        except Exception as e:
            self.logger.error(f"Error inserting auctions: {str(e)}")
            print(f"❌ Database insert failed: {str(e)}")
            raise


def parse_sale_dates(sale_date_str):
    """Parse start and end dates from sale_date string."""
    if not sale_date_str:
        return None, None

    try:
        # Pattern 1: "Ends Thursday 23/10/2025 1:00pm ACST"
        pattern1 = r'Ends\s+\w+\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+(\w+)'
        match1 = re.search(pattern1, sale_date_str)
        if match1:
            date_part = match1.group(1)
            time_part = match1.group(2)
            dt = datetime.strptime(f"{date_part} {time_part}", "%d/%m/%Y %I:%M%p")
            return None, dt.strftime('%Y-%m-%d %H:%M:%S')

        # Pattern 2: "Thursday 23/10/2025 12:00pm - Friday 24/10/2025 12:00pm AEST"
        pattern2 = r'(\w+\s+\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+-\s+(\w+\s+\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+(\w+)'
        match2 = re.search(pattern2, sale_date_str)
        if match2:
            start_date_part = match2.group(1).split()[-1]
            start_time_part = match2.group(2)
            end_date_part = match2.group(3).split()[-1]
            end_time_part = match2.group(4)
            start_dt = datetime.strptime(f"{start_date_part} {start_time_part}", "%d/%m/%Y %I:%M%p")
            end_dt = datetime.strptime(f"{end_date_part} {end_time_part}", "%d/%m/%Y %I:%M%p")
            return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Pattern 3: "Monday 13/10/2025 12:00pm AEDT"
        pattern3 = r'\w+\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+(\w+)'
        match3 = re.search(pattern3, sale_date_str)
        if match3:
            date_part = match3.group(1)
            time_part = match3.group(2)
            dt = datetime.strptime(f"{date_part} {time_part}", "%d/%m/%Y %I:%M%p")
            formatted_dt = dt.strftime('%Y-%m-%d %H:%M:%S')
            return formatted_dt, formatted_dt

        return None, None

    except Exception as e:
        print(f"Error parsing date '{sale_date_str}': {e}")
        return None, None


def load_config():
    """Load configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    if not config_path.exists():
        config_path = PROJECT_ROOT / "config.yaml"
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"❌ Configuration file not found!")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"❌ Error parsing configuration file: {e}")
        sys.exit(1)


def scrape_all_categories(scraper, config, logger):
    """Scrape all enabled auction categories."""
    all_auctions = []

    auction_run_types = config.get('auction_run', ['live_auction', 'online_auction'])
    enabled_categories = [
        cat for cat in config['auction_categories']
        if cat['enabled'] and cat.get('type', 'live_auction') in auction_run_types
    ]

    print(f"📊 Processing {len(enabled_categories)} categories...")
    print(f"🎯 Auction types to run: {', '.join(auction_run_types)}")

    for idx, category in enumerate(enabled_categories, 1):
        category_name = category['name']
        category_url = category['url']
        auction_type = category.get('type', 'live_auction')

        print(f"\n📂 Category {idx}/{len(enabled_categories)}: {category_name} ({auction_type})")
        print(f"🔗 URL: {category_url}")

        if not scraper.navigate_to_auction_page(category_url):
            print(f"❌ Failed to navigate to {category_name}")
            continue

        print(f"✅ Navigated to {category_name}")

        auctions = scraper.extract_auction_details()

        if not auctions:
            print(f"⚠️ No auctions found in {category_name}")
            continue

        print(f"🎯 Found {len(auctions)} auctions")

        for i, auction in enumerate(auctions, 1):
            auction['category'] = category_name
            auction['auction_type'] = auction_type

            if auction['sale_info_url']:
                print(f"   📄 Processing {i}/{len(auctions)}: {auction['title'][:50]}...")

                if scraper.navigate_to_sale_info(auction['sale_info_url']):
                    sale_details = scraper.extract_sale_info_details()
                    auction.update(sale_details)

                    start_sale_date, end_sale_date = parse_sale_dates(auction.get('sale_date'))
                    auction['start_sale_date'] = start_sale_date
                    auction['end_sale_date'] = end_sale_date

                    if not end_sale_date:
                        error_msg = f"Empty end_sale_date for auction: {auction['title'][:50]} | sale_date: {auction.get('sale_date', 'N/A')}"
                        logger.error(error_msg)
                        print(f"      ❌ Could not parse end_sale_date: {auction.get('sale_date', 'N/A')}")
                    else:
                        print(f"      ✅ Parsed dates - Start: {start_sale_date or 'N/A'}, End: {end_sale_date}")

                    if sale_details['auction_registration']:
                        print(f"      ✅ Registration URL found")
                else:
                    auction.update({
                        'auction_registration': None, 'sale_title': None,
                        'sale_date': None, 'sale_occurs': None,
                        'start_sale_date': None, 'end_sale_date': None
                    })

                time.sleep(config['scraper']['delay_between_requests'])
            else:
                auction.update({
                    'auction_registration': None, 'sale_title': None,
                    'sale_date': None, 'sale_occurs': None,
                    'start_sale_date': None, 'end_sale_date': None
                })

        all_auctions.extend(auctions)

        if idx < len(enabled_categories):
            delay = config['scraper']['delay_between_categories']
            print(f"⏳ Waiting {delay}s before next category...")
            time.sleep(delay)

    return all_auctions


def export_to_csv(auctions, filename):
    """Export auction data to CSV file."""
    try:
        fieldnames = ['category', 'title', 'location', 'status', 'sale_info_url',
                     'auction_registration', 'sale_title', 'sale_date', 'sale_occurs', 'auction_type',
                     'start_sale_date', 'end_sale_date']

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


def export_auctions_by_type(all_auctions, config):
    """Export auctions to separate CSV files based on auction type."""
    try:
        live_auctions = [a for a in all_auctions if a.get('auction_type', 'live_auction') == 'live_auction']
        online_auctions = [a for a in all_auctions if a.get('auction_type', 'live_auction') == 'online_auction']

        results = []

        live_csv = config.get('output', {}).get('live_auctions_csv', 'pickles_auctions_detailed.csv')
        online_csv = config.get('output', {}).get('online_auctions_csv', 'pickles_auctions_detailed_online.csv')

        if live_auctions:
            if export_to_csv(live_auctions, live_csv):
                results.append(f"✅ Live auctions: {len(live_auctions)} records → {live_csv}")
            else:
                results.append(f"❌ Failed to export live auctions")

        if online_auctions:
            if export_to_csv(online_auctions, online_csv):
                results.append(f"✅ Online auctions: {len(online_auctions)} records → {online_csv}")
            else:
                results.append(f"❌ Failed to export online auctions")

        return results
    except Exception as e:
        print(f"❌ Export by type failed: {str(e)}")
        return [f"❌ Export by type failed: {str(e)}"]


@with_error_notification()
def main():
    """Main function."""
    config = load_config()
    logger = get_logger("pickles_scraper", log_to_file=True)
    logger.info("=== Multi-Category Pickles Scraper Started ===")

    db_handler = PicklesLiveScheduleDB()

    banner = """
╔══════════════════════════════════════════════════════════════╗
║                PICKLES MULTI-CATEGORY SCRAPER               ║
║        Generates pickles_auctions_detailed.csv & DB         ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

    username = config['credentials']['username']
    password = config['credentials']['password']

    print("🚀 Starting scraper...")

    try:
        scraper_config = config['scraper']
        with PicklesBrowser(
            headless=scraper_config['headless'],
            wait_timeout=scraper_config['wait_timeout']
        ) as scraper:

            print("🔐 Logging in...")
            if not scraper.login(username, password):
                print("❌ Login failed!")
                return 1

            print("✅ Login successful!")

            all_auctions = scrape_all_categories(scraper, config, logger)

            if not all_auctions:
                print("❌ No auctions found!")
                return 1

            print(f"\n📄 Exporting results...")
            export_results = export_auctions_by_type(all_auctions, config)
            for result in export_results:
                print(f"   {result}")

            live_csv = config.get('output', {}).get('live_auctions_csv', 'pickles_auctions_detailed.csv')
            if not any(live_csv in result and "✅" in result for result in export_results):
                print(f"❌ Failed to create {live_csv} (required for step2)")
                return 1

            print(f"\n💾 Inserting data into database...")
            try:
                inserted_count = db_handler.insert_auctions(all_auctions)
                print(f"✅ Database operations completed")
            except Exception as e:
                print(f"❌ Database insert failed: {str(e)}")
                logger.error(f"Database insert error: {str(e)}")

            print(f"\n🎉 COMPLETED!")
            print(f"📊 Total auctions scraped: {len(all_auctions)}")

            live_auctions = [a for a in all_auctions if a.get('auction_type', 'live_auction') == 'live_auction']
            online_auctions = [a for a in all_auctions if a.get('auction_type', 'live_auction') == 'online_auction']
            print(f"🔴 Live auctions: {len(live_auctions)}")
            print(f"🟢 Online auctions: {len(online_auctions)}")

            with_registration = [a for a in all_auctions if a['auction_registration']]
            print(f"🎫 With registration URLs: {len(with_registration)}")

            print(f"\n🧹 Cleaning duplicate records...")
            duplicate_cleaner = DuplicateCleaner()
            final_count = duplicate_cleaner.clean_duplicates(
                table_name='pickles_live_schedule',
                partition_by='sale_info_url',
                order_by='created_at'
            )

            if final_count > 0:
                print(f"✅ Final table contains {final_count} unique records")
            else:
                print(f"⚠️ Duplicate cleaning completed with warnings (check logs)")

    except KeyboardInterrupt:
        print("\n👋 Cancelled by user")
    except Exception as e:
        print(f"💥 Error: {str(e)}")
        logger.error(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
