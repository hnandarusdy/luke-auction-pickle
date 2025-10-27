#!/usr/bin/env python3
"""
Duplicate Cleaner Utility

This module provides functionality to clean duplicate records from database tables
using ROW_NUMBER() window function with configurable partitioning and ordering.

Author: GitHub Copilot
Date: October 26, 2025
"""

import os
import json
import pandas as pd
from datetime import datetime
from db import MySecondDB
from logger import get_logger


class DuplicateCleaner:
    """
    Utility class to clean duplicate records from database tables
    """
    
    def __init__(self):
        """Initialize database connection"""
        self.db = MySecondDB()
        self.logger = get_logger("duplicate_cleaner", log_to_file=True)
    
    def clean_duplicates(self, table_name, partition_by, order_by='created_at'):
        """
        Clean duplicate records from a table using ROW_NUMBER() window function
        
        This method:
        1. Selects unique records (row_num = 1) as DataFrame
        2. Saves backup as JSON file in duplicate_clean_sql folder
        3. Truncates original table and loads deduplicated data
        
        Args:
            table_name (str): Name of the table to clean
            partition_by (str): Column to partition by (identifies duplicates)
            order_by (str): Column to order by (default: 'created_at')
        
        Returns:
            int: Number of records after deduplication, or -1 if error
        """
        try:
            self.logger.info(f"Starting duplicate cleanup for table: {table_name}")
            print(f"🧹 Cleaning duplicates from {table_name}...")
            
            # First, get count before cleanup
            count_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
            before_df = self.db.read_sql(count_query)
            before_count = before_df['total_count'].iloc[0] if not before_df.empty else 0
            
            # Step 1: Select deduplicated records as DataFrame
            self.logger.info(f"Selecting deduplicated records...")
            dedup_query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by} DESC) as row_num
                FROM {table_name}
            ) as cte
            WHERE row_num = 1
            """
            
            deduplicated_df = self.db.read_sql(dedup_query)
            
            if deduplicated_df.empty:
                print(f"⚠️ No data found in {table_name}")
                return 0
            
            # Remove the row_num column
            if 'row_num' in deduplicated_df.columns:
                deduplicated_df = deduplicated_df.drop('row_num', axis=1)
            
            # Step 2: Create backup JSON file in duplicate_clean_sql folder
            backup_folder = "duplicate_clean_sql"
            if not os.path.exists(backup_folder):
                os.makedirs(backup_folder)
                self.logger.info(f"Created backup folder: {backup_folder}")
            
            backup_file = os.path.join(backup_folder, f"{table_name}.json")
            
            # Convert DataFrame to JSON (handle datetime and other types)
            json_data = deduplicated_df.to_dict('records')
            
            # Convert datetime objects to strings for JSON serialization
            for record in json_data:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.strftime('%Y-%m-%d %H:%M:%S') if value else None
            
            # Save backup JSON (replaceable every time)
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"💾 Backup saved: {backup_file}")
            self.logger.info(f"Backup saved with {len(json_data)} records to {backup_file}")
            
            # Step 3: Load deduplicated data back to same table
            self.logger.info(f"Replacing table data with deduplicated records...")
            
            # Truncate the original table (safer than DROP/CREATE)
            truncate_query = f"TRUNCATE TABLE {table_name}"
            self.db.execute_query(truncate_query)
            self.logger.info(f"Truncated table {table_name}")
            
            # Try to insert deduplicated data back
            try:
                result = self.db.write_to_sql(deduplicated_df, table_name, how='append')
                self.logger.info(f"Inserted {len(deduplicated_df)} deduplicated records from DataFrame")
                print(f"✅ Data loaded from DataFrame successfully")
                
            except Exception as df_error:
                # Fallback: Load from JSON file if DataFrame insertion fails
                self.logger.warning(f"DataFrame insertion failed: {str(df_error)}")
                print(f"⚠️ DataFrame insertion failed, trying JSON fallback...")
                
                try:
                    # Load data from JSON backup
                    with open(backup_file, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    if not json_data:
                        raise Exception("JSON backup file is empty")
                    
                    # Convert JSON back to DataFrame
                    fallback_df = pd.DataFrame(json_data)
                    
                    # Convert datetime strings back to proper format if needed
                    datetime_columns = ['created_at', 'updated_at', 'start_sale_date', 'end_sale_date']
                    for col in datetime_columns:
                        if col in fallback_df.columns:
                            fallback_df[col] = pd.to_datetime(fallback_df[col], errors='ignore')
                    
                    # Insert from JSON data
                    result = self.db.write_to_sql(fallback_df, table_name, how='append')
                    self.logger.info(f"Successfully inserted {len(fallback_df)} records from JSON fallback")
                    print(f"✅ Data loaded from JSON backup successfully")
                    
                except Exception as json_error:
                    error_msg = f"Both DataFrame and JSON loading failed. DataFrame error: {str(df_error)}, JSON error: {str(json_error)}"
                    self.logger.error(error_msg)
                    print(f"❌ {error_msg}")
                    
                    # Try to restore from backup if possible
                    print(f"💾 Manual recovery may be needed from: {backup_file}")
                    return -1
            
            # Get final count
            after_df = self.db.read_sql(count_query)
            after_count = after_df['total_count'].iloc[0] if not after_df.empty else 0
            
            # Calculate removed duplicates
            duplicates_removed = before_count - after_count
            
            print(f"✅ Duplicate cleanup completed for {table_name}")
            print(f"   📊 Before: {before_count} records")
            print(f"   📊 After: {after_count} records") 
            print(f"   🗑️ Removed: {duplicates_removed} duplicates")
            print(f"   💾 Backup: {backup_file}")
            
            self.logger.info(f"Cleanup completed - Before: {before_count}, After: {after_count}, Removed: {duplicates_removed}")
            
            return after_count
            
        except Exception as e:
            error_msg = f"Error cleaning duplicates from {table_name}: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            return -1
    
    def restore_from_json(self, table_name):
        """
        Manual recovery method to restore table data from JSON backup
        
        Args:
            table_name (str): Name of the table to restore
            
        Returns:
            int: Number of records restored, or -1 if error
        """
        try:
            backup_file = os.path.join("duplicate_clean_sql", f"{table_name}.json")
            
            if not os.path.exists(backup_file):
                print(f"❌ Backup file not found: {backup_file}")
                return -1
            
            print(f"🔄 Restoring {table_name} from JSON backup...")
            self.logger.info(f"Starting manual restore from {backup_file}")
            
            # Load data from JSON backup
            with open(backup_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            if not json_data:
                print(f"❌ JSON backup file is empty")
                return -1
            
            # Convert JSON back to DataFrame
            restore_df = pd.DataFrame(json_data)
            
            # Convert datetime strings back to proper format if needed
            datetime_columns = ['created_at', 'updated_at', 'start_sale_date', 'end_sale_date']
            for col in datetime_columns:
                if col in restore_df.columns:
                    restore_df[col] = pd.to_datetime(restore_df[col], errors='ignore')
            
            # Truncate table first
            truncate_query = f"TRUNCATE TABLE {table_name}"
            self.db.execute_query(truncate_query)
            
            # Insert restored data
            result = self.db.write_to_sql(restore_df, table_name, how='append')
            
            print(f"✅ Restored {len(restore_df)} records from JSON backup")
            self.logger.info(f"Successfully restored {len(restore_df)} records from JSON backup")
            
            return len(restore_df)
            
        except Exception as e:
            error_msg = f"Error restoring from JSON: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            return -1
    
    def get_duplicate_count(self, table_name, partition_by):
        """
        Get count of duplicate records without cleaning them
        
        Args:
            table_name (str): Name of the table to check
            partition_by (str): Column to partition by (identifies duplicates)
        
        Returns:
            dict: {'total_records': int, 'unique_records': int, 'duplicates': int}
        """
        try:
            # Get total count
            total_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
            total_df = self.db.read_sql(total_query)
            total_count = total_df['total_count'].iloc[0] if not total_df.empty else 0
            
            # Get unique count
            unique_query = f"SELECT COUNT(DISTINCT {partition_by}) as unique_count FROM {table_name}"
            unique_df = self.db.read_sql(unique_query)
            unique_count = unique_df['unique_count'].iloc[0] if not unique_df.empty else 0
            
            duplicates = total_count - unique_count
            
            return {
                'total_records': total_count,
                'unique_records': unique_count,
                'duplicates': duplicates
            }
            
        except Exception as e:
            self.logger.error(f"Error getting duplicate count for {table_name}: {str(e)}")
            return {'total_records': 0, 'unique_records': 0, 'duplicates': 0}