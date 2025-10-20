#!/usr/bin/env python3
"""
URL Converter Script
Reads from both pickles_auctions_detailed_online.csv and pickles_auctions_detailed.csv
Filters by date and converts sale_info_url to search URLs
"""

import pandas as pd
import re
import os
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

def parse_date_from_string(date_string):
    """
    Parse date from various string formats
    
    Args:
        date_string (str): Date string in various formats
        
    Returns:
        datetime: Parsed date or None if parsing fails
    """
    if not date_string or pd.isna(date_string):
        return None
    
    try:
        # Clean the string
        date_string = str(date_string).strip()
        
        # Common patterns to extract dates
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',  # DD/MM/YYYY or D/M/YYYY
            r'(\d{4}-\d{1,2}-\d{1,2})',  # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, date_string)
            if matches:
                date_str = matches[-1]  # Take the last date found (end date for ranges)
                try:
                    # Try DD/MM/YYYY format first
                    return datetime.strptime(date_str, '%d/%m/%Y')
                except ValueError:
                    try:
                        # Try YYYY-MM-DD format
                        return datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        continue
        
        return None
        
    except Exception as e:
        print(f"   ⚠️  Date parsing error for '{date_string}': {e}")
        return None

def extract_end_date_from_online_format(sale_date):
    """
    Extract end date from online auction format
    Handles formats like:
    - "Tuesday 21/10/2025 3:00pm - Thursday 23/10/2025 7:00pm AEDT" -> 23/10/2025
    - "Ends Tuesday 21/10/2025 8:00pm AEDT" -> 21/10/2025
    
    Args:
        sale_date (str): Sale date string
        
    Returns:
        datetime: End date or None if parsing fails
    """
    if not sale_date or pd.isna(sale_date):
        return None
    
    try:
        sale_date_str = str(sale_date).strip()
        
        # Pattern for range format: "day DD/MM/YYYY time - day DD/MM/YYYY time timezone"
        range_pattern = r'.+?(\d{1,2}/\d{1,2}/\d{4}).+?-\s*.+?(\d{1,2}/\d{1,2}/\d{4})'
        range_match = re.search(range_pattern, sale_date_str)
        
        if range_match:
            # Take the second date (end date)
            end_date_str = range_match.group(2)
            return datetime.strptime(end_date_str, '%d/%m/%Y')
        
        # Pattern for single date format: "Ends day DD/MM/YYYY time timezone"
        single_pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
        single_match = re.search(single_pattern, sale_date_str)
        
        if single_match:
            date_str = single_match.group(1)
            return datetime.strptime(date_str, '%d/%m/%Y')
        
        return None
        
    except Exception as e:
        print(f"   ⚠️  Online date parsing error for '{sale_date}': {e}")
        return None

def filter_by_date(df, date_column, is_online_format=False):
    """
    Filter dataframe by date (today or past only)
    
    Args:
        df (DataFrame): Input dataframe
        date_column (str): Name of the date column
        is_online_format (bool): Whether to use online format parsing
        
    Returns:
        DataFrame: Filtered dataframe
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    print(f"🗓️  Filtering by date (today: {today.strftime('%d/%m/%Y')})...")
    
    filtered_rows = []
    
    for index, row in df.iterrows():
        if is_online_format:
            parsed_date = extract_end_date_from_online_format(row[date_column])
        else:
            parsed_date = parse_date_from_string(row[date_column])
        
        if parsed_date:
            # Check if date is today or in the past
            if parsed_date.date() <= today.date():
                filtered_rows.append(row)
                print(f"   ✅ Including: {row.get('title', 'Unknown')[:50]}... (Date: {parsed_date.strftime('%d/%m/%Y')})")
            else:
                print(f"   ⏭️  Skipping future: {row.get('title', 'Unknown')[:50]}... (Date: {parsed_date.strftime('%d/%m/%Y')})")
        else:
            print(f"   ⚠️  Skipping (no date): {row.get('title', 'Unknown')[:50]}...")
    
    filtered_df = pd.DataFrame(filtered_rows)
    print(f"📊 Filtered from {len(df)} to {len(filtered_df)} rows")
    
    return filtered_df

def extract_auction_name_and_sale_no(sale_info_url):
    """
    Extract auction name and sale number from sale_info_url
    
    Args:
        sale_info_url (str): Original sale info URL
        
    Returns:
        tuple: (auction_name, sale_no) or (None, None) if extraction fails
    """
    try:
        # Parse the URL
        parsed_url = urlparse(sale_info_url)
        
        # Extract sale_no from query parameters
        query_params = parse_qs(parsed_url.query)
        sale_no = query_params.get('sale_no', [None])[0]
        
        if not sale_no:
            return None, None
        
        # Extract auction name from path
        # Path format: /{category}/auction/saleinfo/{auction-name}/{location}
        path_parts = parsed_url.path.strip('/').split('/')
        
        if len(path_parts) >= 4 and path_parts[1] == 'auction' and path_parts[2] == 'saleinfo':
            auction_name = path_parts[3]
            return auction_name, sale_no
        else:
            return None, None
            
    except Exception as e:
        print(f"Error parsing URL {sale_info_url}: {e}")
        return None, None

def convert_to_search_url(auction_name, sale_no):
    """
    Convert auction name and sale number to search URL format
    
    Args:
        auction_name (str): Auction name from original URL
        sale_no (str): Sale number
        
    Returns:
        str: Converted search URL
    """
    if not auction_name or not sale_no:
        return None
    
    # Build the new URL format
    search_url = f"https://www.pickles.com.au/used/search/s/{auction_name}/{sale_no}"
    return search_url

def process_combined_csv_files(online_file, offline_file, output_file):
    """
    Process both CSV files, filter by date, and convert URLs
    
    Args:
        online_file (str): Path to online auctions CSV file
        offline_file (str): Path to offline auctions CSV file  
        output_file (str): Path to output CSV file
    """
    try:
        combined_data = []
        
        # Process online auctions file
        if os.path.exists(online_file):
            print(f"\n📁 Reading ONLINE CSV file: {online_file}")
            df_online = pd.read_csv(online_file)
            print(f"📊 Found {len(df_online)} online auction rows")
            
            # Filter by end date (today or past)
            df_online_filtered = filter_by_date(df_online, 'sale_date', is_online_format=True)
            
            if not df_online_filtered.empty:
                # Add source column
                df_online_filtered['source'] = 'online'
                combined_data.append(df_online_filtered)
                print(f"✅ Added {len(df_online_filtered)} online auctions")
            else:
                print("⚠️  No online auctions match date criteria")
        else:
            print(f"⚠️  Online file not found: {online_file}")
        
        # Process offline auctions file  
        if os.path.exists(offline_file):
            print(f"\n📁 Reading OFFLINE CSV file: {offline_file}")
            df_offline = pd.read_csv(offline_file)
            print(f"📊 Found {len(df_offline)} offline auction rows")
            
            # Filter by sale date (today or past)
            df_offline_filtered = filter_by_date(df_offline, 'sale_date', is_online_format=False)
            
            if not df_offline_filtered.empty:
                # Add source column
                df_offline_filtered['source'] = 'offline'
                combined_data.append(df_offline_filtered)
                print(f"✅ Added {len(df_offline_filtered)} offline auctions")
            else:
                print("⚠️  No offline auctions match date criteria")
        else:
            print(f"⚠️  Offline file not found: {offline_file}")
        
        # Combine all data
        if not combined_data:
            print("❌ No data to process after filtering")
            return False
        
        df_combined = pd.concat(combined_data, ignore_index=True)
        print(f"\n📊 Combined total: {len(df_combined)} auctions")
        
        # Create new dataframe with only the columns we want
        result_df = pd.DataFrame()
        result_df['sale_info_url'] = df_combined['sale_info_url'].copy()
        result_df['listing_url'] = ''
        result_df['title'] = df_combined['title'].copy()
        result_df['source'] = df_combined['source'].copy()
        
        successful_conversions = 0
        failed_conversions = 0
        
        # Process each row
        for index, row in df_combined.iterrows():
            sale_info_url = row['sale_info_url']
            title = row.get('title', 'Unknown')
            
            print(f"\n🔄 Processing row {index + 1}: {title[:50]}...")
            
            # Extract auction name and sale number
            auction_name, sale_no = extract_auction_name_and_sale_no(sale_info_url)
            
            if auction_name and sale_no:
                # Convert to search URL (listing_url)
                listing_url = convert_to_search_url(auction_name, sale_no)
                
                # Update result dataframe
                result_df.at[index, 'listing_url'] = listing_url
                
                print(f"   ✅ Converted: {listing_url}")
                successful_conversions += 1
            else:
                result_df.at[index, 'listing_url'] = 'FAILED'
                print(f"   ❌ Failed to extract auction name or sale number")
                failed_conversions += 1
        
        # Save the results
        print(f"\n💾 Saving results to: {output_file}")
        result_df.to_csv(output_file, index=False)
        
        # Print summary
        print(f"\n📊 CONVERSION SUMMARY:")
        print(f"   📁 Online file: {online_file}")
        print(f"   📁 Offline file: {offline_file}")
        print(f"   💾 Output file: {output_file}")
        print(f"   📋 Total rows processed: {len(df_combined)}")
        print(f"   ✅ Successful conversions: {successful_conversions}")
        print(f"   ❌ Failed conversions: {failed_conversions}")
        print(f"   📈 Success rate: {successful_conversions/len(df_combined)*100:.1f}%")
        
        # Show breakdown by source
        online_count = len(result_df[result_df['source'] == 'online'])
        offline_count = len(result_df[result_df['source'] == 'offline'])
        print(f"\n📋 Source breakdown:")
        print(f"   🌐 Online auctions: {online_count}")
        print(f"   🏢 Offline auctions: {offline_count}")
        
        print(f"\n📋 Output file contains:")
        print(f"   • sale_info_url (original)")
        print(f"   • listing_url (converted)")
        print(f"   • title (for reference)")
        print(f"   • source (online/offline)")

        return True
        
    except Exception as e:
        print(f"❌ Error processing CSV files: {e}")
        return False

def process_csv_file(input_file, output_file):
    """
    Process the CSV file and convert URLs
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
    """
    try:
        # Read the CSV file
        print(f"📁 Reading CSV file: {input_file}")
        df = pd.read_csv(input_file)
        
        print(f"📊 Found {len(df)} rows")
        
        # Create new dataframe with only the 2 columns we want
        result_df = pd.DataFrame()
        result_df['sale_info_url'] = df['sale_info_url'].copy()
        result_df['listing_url'] = ''
        
        successful_conversions = 0
        failed_conversions = 0
        
        # Process each row
        for index, row in df.iterrows():
            sale_info_url = row['sale_info_url']
            
            print(f"🔄 Processing row {index + 1}...")
            
            # Extract auction name and sale number
            auction_name, sale_no = extract_auction_name_and_sale_no(sale_info_url)
            
            if auction_name and sale_no:
                # Convert to search URL (listing_url)
                listing_url = convert_to_search_url(auction_name, sale_no)
                
                # Update result dataframe
                result_df.at[index, 'listing_url'] = listing_url
                
                print(f"   ✅ Converted: {listing_url}")
                successful_conversions += 1
            else:
                result_df.at[index, 'listing_url'] = 'FAILED'
                print(f"   ❌ Failed to extract auction name or sale number")
                failed_conversions += 1
        
        # Save only the 2 columns
        print(f"\n💾 Saving results to: {output_file}")
        result_df.to_csv(output_file, index=False)
        
        # Print summary
        print(f"\n📊 CONVERSION SUMMARY:")
        print(f"   📁 Input file: {input_file}")
        print(f"   💾 Output file: {output_file}")
        print(f"   📋 Total rows: {len(df)}")
        print(f"   ✅ Successful conversions: {successful_conversions}")
        print(f"   ❌ Failed conversions: {failed_conversions}")
        print(f"   📈 Success rate: {successful_conversions/len(df)*100:.1f}%")
        print(f"\n📋 Output file contains only 2 columns:")
        print(f"   • sale_info_url (original)")
        print(f"   • listing_url (converted)")
        

        return True
        
    except FileNotFoundError:
        print(f"❌ Error: Input file '{input_file}' not found")
        return False
    except Exception as e:
        print(f"❌ Error processing CSV file: {e}")
        return False

def main():
    """Main function"""
    print("🔄 Pickles Auction URL Converter (Combined)")
    print("=" * 55)
    
    # Define file paths
    online_file = "pickles_auctions_detailed_online.csv"
    offline_file = "pickles_auctions_detailed.csv"
    output_file = "pickles_auction_step2_combined.csv"
    
    # Check if at least one input file exists
    online_exists = os.path.exists(online_file)
    offline_exists = os.path.exists(offline_file)
    
    if not online_exists and not offline_exists:
        print(f"❌ Neither input file found:")
        print(f"   • {online_file}")
        print(f"   • {offline_file}")
        return
    
    if not online_exists:
        print(f"⚠️  Online file not found: {online_file}")
    if not offline_exists:
        print(f"⚠️  Offline file not found: {offline_file}")
    
    # Process the files
    success = process_combined_csv_files(online_file, offline_file, output_file)
    
    if success:
        print(f"\n🎉 Conversion complete!")
        print(f"📁 Check the output file: {output_file}")
        print(f"\n💡 Next steps:")
        print(f"   • Use this file with your online_scraper_step2.py")
        print(f"   • The 'listing_url' column contains the converted URLs")
    else:
        print(f"\n❌ Conversion failed!")

if __name__ == "__main__":
    main()