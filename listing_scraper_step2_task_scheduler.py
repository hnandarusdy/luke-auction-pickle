#!/usr/bin/env python3
"""
Task Scheduler for Listing Scraper Step 2
Creates Windows scheduled tasks for online auctions 15 minutes before end time
"""

import pandas as pd
import re
import subprocess
import socket
import getpass
import os
from datetime import datetime, timedelta
from db import MySecondDB

def parse_end_date(sale_date_str):
    """
    Parse end date from sale_date string
    
    Sample formats:
    - Thursday 23/10/2025 12:00pm - Friday 24/10/2025 12:00pm AEST
    - Ends Thursday 23/10/2025 1:00pm ACST
    - Tuesday 28/10/2025 2:00pm - Wednesday 29/10/2025 2:00pm AEDT
    """
    if not sale_date_str:
        return None
    
    try:
        # Pattern 1: "Ends Thursday 23/10/2025 1:00pm ACST"
        pattern1 = r'Ends\s+\w+\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+(\w+)'
        match1 = re.search(pattern1, sale_date_str)
        
        if match1:
            date_part = match1.group(1)
            time_part = match1.group(2)
            timezone = match1.group(3)
            
            # Convert to datetime
            datetime_str = f"{date_part} {time_part}"
            dt = datetime.strptime(datetime_str, "%d/%m/%Y %I:%M%p")
            return dt
        
        # Pattern 2: "Thursday 23/10/2025 12:00pm - Friday 24/10/2025 12:00pm AEST"
        pattern2 = r'-\s+\w+\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}[ap]m)\s+(\w+)'
        match2 = re.search(pattern2, sale_date_str)
        
        if match2:
            date_part = match2.group(1)
            time_part = match2.group(2)
            timezone = match2.group(3)
            
            # Convert to datetime
            datetime_str = f"{date_part} {time_part}"
            dt = datetime.strptime(datetime_str, "%d/%m/%Y %I:%M%p")
            return dt
        
        return None
        
    except Exception as e:
        print(f"Error parsing date '{sale_date_str}': {e}")
        return None

def extract_sale_id_from_url(sale_info_url):
    """Extract sale ID from sale_info_url"""
    if not sale_info_url:
        return None
    
    try:
        # Pattern 1: ?sale_no=12345
        match1 = re.search(r'sale_no=(\d+)', sale_info_url)
        if match1:
            return match1.group(1)
        
        # Pattern 2: /sale/12345 or /sale/12345/
        match2 = re.search(r'/sale/(\d+)', sale_info_url)
        if match2:
            return match2.group(1)
            
        return None
    except Exception as e:
        print(f"Error extracting sale ID from URL '{sale_info_url}': {e}")
        return None

def build_sale_url(sale_info_url, title):
    """Build the sale URL for the scraper command"""
    if not sale_info_url:
        return None
    
    try:
        # Extract sale ID
        sale_id = extract_sale_id_from_url(sale_info_url)
        if not sale_id:
            return None
        
        # Create a URL slug from title
        title_slug = re.sub(r'[^a-zA-Z0-9\s-]', '', title.lower())
        title_slug = re.sub(r'\s+', '-', title_slug.strip())
        
        # Build the target URL format
        sale_url = f"https://www.pickles.com.au/used/search/s/{title_slug}/{sale_id}"
        return sale_url
        
    except Exception as e:
        print(f"Error building sale URL: {e}")
        return None

def create_windows_task(task_name, command, scheduled_time):
    """Create a Windows scheduled task"""
    try:
        # Check if the task already exists
        check_cmd = [
            "schtasks", "/query", "/tn", task_name
        ]
        check_result = subprocess.run(check_cmd, capture_output=True, text=True, shell=True)
        if check_result.returncode == 0:
            print(f"   ⚠️ Task '{task_name}' already exists in Windows Task Scheduler. Skipping creation.")
            return False

        # Format the scheduled time for schtasks
        schtasks_time = scheduled_time.strftime("%H:%M")
        schtasks_date = scheduled_time.strftime("%d/%m/%Y")

        # Build the schtasks command
        schtasks_cmd = [
            "schtasks", "/create",
            "/tn", task_name,
            "/tr", command,
            "/sc", "once",
            "/st", schtasks_time,
            "/sd", schtasks_date,
            "/f"  # Force overwrite if exists
        ]

        print(f"   🕒 Creating task: {task_name}")
        print(f"   📅 Scheduled for: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   💻 Command: {command}")

        # Execute the command
        result = subprocess.run(schtasks_cmd, capture_output=True, text=True, shell=True)

        if result.returncode == 0:
            print(f"   ✅ Task created successfully")
            return True
        else:
            print(f"   ❌ Failed to create task: {result.stderr}")
            return False

    except Exception as e:
        print(f"   ❌ Error creating Windows task: {e}")
        return False

def save_task_to_database(db, task_data):
    """Save task information to database"""
    try:
        # Create DataFrame with task data
        df = pd.DataFrame([task_data])
        
        # Insert into database
        db.write_to_sql(df, 'pickles_online_auction_scheduled_task_scheduler', how='append')
        print(f"   � Task saved to database")
        return True
        
    except Exception as e:
        print(f"   ❌ Error saving to database: {e}")
        return False

def main():
    """Main function to create scheduled tasks for upcoming auctions"""
    
    print("🚀 Starting Auction Task Scheduler...")
    print("=" * 60)
    
    # Initialize database connection
    db = MySecondDB()
    current_time = datetime.now()
    
    try:
        # Query online auctions
        query = """
        select *
        from (
            SELECT *, row_number() over (partition by sale_info_url order by created_at desc) as row_num
            FROM pickles_live_schedule
            where auction_type is not null
                and end_sale_date is not null
                and end_sale_date >= current_date
        ) as cte
        where row_num = 1        
        """
        
        print("📊 Querying online auctions from database...")
        df = db.read_sql(query)
        
        if df.empty:
            print("❌ No online auctions found in database")
            return
        
        print(f"✅ Found {len(df)} online auctions")
        
        # Add end_sale_datetime column
        print("🕒 Parsing end dates...")
        df['end_sale_datetime'] = df['sale_date'].apply(parse_end_date)
        
        # Filter for future auctions only
        df = df[df['end_sale_datetime'].notna()]
        future_df = df[df['end_sale_datetime'] > current_time]
        
        print(f"🔮 Found {len(future_df)} upcoming auctions (end_date >= now)")
        
        if future_df.empty:
            print("⚠️ No upcoming auctions to schedule")
            return
        
        # Get system info
        pc_name = socket.gethostname()
        created_by = getpass.getuser()
        script_path = os.path.abspath("listing_scraper_step2_scrape_one_url.py")
        
        print(f"💻 PC Name: {pc_name}")
        print(f"👤 Created by: {created_by}")
        print(f"📄 Script path: {script_path}")
        
        print("\n" + "=" * 60)
        print("🎯 CREATING SCHEDULED TASKS")
        print("=" * 60)
        
        tasks_created = 0
        tasks_failed = 0
        
        for idx, row in future_df.iterrows():
            print(f"\n🎪 Processing Auction {idx + 1}/{len(future_df)}")
            print(f"   📋 Title: {row['title'][:60]}...")
            print(f"   📅 End Date: {row['end_sale_datetime']}")
            
            # Extract sale ID
            sale_id = extract_sale_id_from_url(row.get('sale_info_url', ''))
            if not sale_id:
                print(f"   ❌ Could not extract sale ID from URL: {row.get('sale_info_url', 'N/A')}")
                tasks_failed += 1
                continue
            
            # Build sale URL
            sale_url = build_sale_url(row.get('sale_info_url', ''), row.get('title', ''))
            if not sale_url:
                print(f"   ❌ Could not build sale URL")
                tasks_failed += 1
                continue
            
            # Calculate scheduled run time (15 minutes before end)
            end_time = row['end_sale_datetime']
            scheduled_time = end_time - timedelta(minutes=15)
            
            # Check if scheduled time is in the past
            if scheduled_time <= current_time:
                print(f"   ⚠️ Scheduled time is in the past, skipping...")
                tasks_failed += 1
                continue
            
            # Create task name
            task_name = f"{sale_id}_online_pickles"
            
            # Create command
            command = f'python "{script_path}" "{sale_url}"'
            
            print(f"   🆔 Sale ID: {sale_id}")
            print(f"   🔗 Sale URL: {sale_url}")
            print(f"   ⏰ Scheduled: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')} (15 min before end)")
            
            # Create Windows scheduled task
            task_created = create_windows_task(task_name, command, scheduled_time)
            
            if task_created:
                # Prepare task data for database
                task_data = {
                    'sale_id': sale_id,
                    'sale_title': row.get('title', ''),
                    'sale_url': sale_url,
                    'sale_date': row.get('sale_date', ''),
                    'sale_end_date': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'scheduled_run_time': scheduled_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'task_name': task_name,
                    'task_command': command,
                    'task_status': 'CREATED',
                    'pc_name': pc_name,
                    'created_by': created_by
                }
                
                # Save to database
                if save_task_to_database(db, task_data):
                    tasks_created += 1
                    print(f"   🎉 Task successfully created and saved!")
                else:
                    tasks_failed += 1
            else:
                tasks_failed += 1
        
        # Final summary
        print("\n" + "=" * 60)
        print("📊 TASK CREATION SUMMARY")
        print("=" * 60)
        print(f"✅ Tasks created successfully: {tasks_created}")
        print(f"❌ Tasks failed: {tasks_failed}")
        print(f"📊 Total processed: {len(future_df)}")
        
        if tasks_created > 0:
            print(f"\n🎯 {tasks_created} Windows scheduled tasks have been created!")
            print(f"💾 Task details saved to database table: pickles_online_auction_scheduled_task_scheduler")
            print(f"⏰ Tasks will run 15 minutes before each auction ends")
            print(f"🤖 Each task will execute: python listing_scraper_step2_scrape_one_url.py <sale_url>")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        db.close()

if __name__ == "__main__":
    main()
