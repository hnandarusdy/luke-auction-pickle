#!/usr/bin/env python3
"""
Step 4: Load Sale Info JSON Data to Database

This script loads all JSON files from json_data_online folder,
flattens the data according to sale_format.json schema,
and inserts into pickles_sale_info table.

Author: GitHub Copilot
Date: October 26, 2025
"""

import os
import json
import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from db import MySecondDB
from logger import get_logger
from duplicate_cleaner import DuplicateCleaner
from whatsapp_notifier import with_error_notification


class SaleInfoLoader:
    """
    Loads sale info JSON data into pickles_sale_info database table
    """
    
    def __init__(self):
        """Initialize database connection and logger"""
        self.db = MySecondDB()
        self.logger = get_logger("sale_info_loader", log_to_file=True)
        self.json_folder = "json_data_online"
        self.uploaded_folder = "json_data_online_uploaded_to_db"
        self.schema_file = "data_format/sale_format.json"
        self.table_name = "pickles_sale_info"
        
    def load_schema(self):
        """Load the schema from sale_format.json"""
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            # Get column names from schema (keys are column names)
            schema_columns = list(schema.keys())
            
            self.logger.info(f"Loaded schema with {len(schema_columns)} columns")
            print(f"📋 Schema loaded: {len(schema_columns)} columns")
            
            return schema_columns
            
        except Exception as e:
            self.logger.error(f"Error loading schema: {str(e)}")
            print(f"❌ Error loading schema: {str(e)}")
            return []
    
    def load_json_files(self):
        """Load all JSON files from json_data_online folder"""
        try:
            if not os.path.exists(self.json_folder):
                print(f"❌ JSON folder not found: {self.json_folder}")
                return []
            
            json_files = []
            for file_name in os.listdir(self.json_folder):
                if file_name.endswith('.json'):
                    file_path = os.path.join(self.json_folder, file_name)
                    json_files.append(file_path)
            
            print(f"📂 Found {len(json_files)} JSON files")
            self.logger.info(f"Found {len(json_files)} JSON files to process")
            
            return json_files
            
        except Exception as e:
            self.logger.error(f"Error loading JSON files: {str(e)}")
            print(f"❌ Error loading JSON files: {str(e)}")
            return []
    
    def flatten_json_data(self, json_file_path, schema_columns):
        """
        Flatten JSON data and extract records according to schema
        
        Args:
            json_file_path (str): Path to JSON file
            schema_columns (list): List of expected column names
            
        Returns:
            list: List of flattened records
        """
        try:
            print(f"   📄 Processing: {os.path.basename(json_file_path)}")
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            flattened_records = []
            
            # Check if this is the new JSON structure with 'sale' and 'lots'
            if isinstance(data, dict) and 'sale' in data and 'lots' in data:
                print(f"      📋 Processing auction format with sale info and lots")
                
                # Extract sale information from root level
                sale_info = data.get('sale', {})
                lots = data.get('lots', [])
                
                print(f"      📊 Found sale info and {len(lots)} lots")
                
                for lot in lots:
                    if not isinstance(lot, dict):
                        continue
                    
                    # Create a record combining sale info and lot info
                    record = {}
                    
                    # Start with all schema columns as None
                    for col in schema_columns:
                        record[col] = None
                    
                    # Map sale information to schema columns
                    record['saleId'] = sale_info.get('saleId')
                    record['saleNumber'] = sale_info.get('saleNumber')
                    record['saleName'] = sale_info.get('saleName')
                    record['saleStatus'] = sale_info.get('saleStatus')
                    record['saleStart'] = sale_info.get('saleStart')
                    record['saleEnd'] = sale_info.get('saleEnd')
                    record['saleEventLocation'] = sale_info.get('saleEventLocation')
                    record['businessUnitName'] = sale_info.get('businessUnitName')
                    record['businessUnitId'] = sale_info.get('businessUnitId')
                    record['sellingChannelId'] = sale_info.get('sellingChannelId')
                    record['saleStartTimezone'] = sale_info.get('saleStartTimezone')
                    record['saleEndTimezone'] = sale_info.get('saleEndTimezone')
                    
                    # Map lot information to schema columns
                    record['id'] = lot.get('id')
                    record['assetId'] = lot.get('assetId')
                    record['assetExternalId'] = lot.get('assetExternalId')
                    record['assetType'] = lot.get('assetType')
                    record['stockNumber'] = lot.get('stockNumber')
                    record['title'] = lot.get('title')
                    record['shortDescription'] = lot.get('shortDescription')
                    record['description'] = lot.get('description')
                    record['make'] = lot.get('make')
                    record['model'] = lot.get('model')
                    record['badge'] = lot.get('badge')
                    record['series'] = lot.get('series')
                    record['year'] = lot.get('year')
                    record['built'] = lot.get('built')
                    record['complianceDate'] = lot.get('complianceDate')
                    record['body'] = lot.get('body')
                    record['doors'] = lot.get('doors')
                    record['seats'] = lot.get('seats')
                    record['colour'] = lot.get('colour')
                    record['colourManufacturer'] = lot.get('colourManufacturer')
                    record['trimType'] = lot.get('trimType')
                    record['trimColour'] = lot.get('trimColour')
                    record['transmission'] = lot.get('transmission')
                    record['driveType'] = lot.get('driveType')
                    record['fuelType'] = lot.get('fuelType')
                    record['fuelSystem'] = lot.get('fuelSystem')
                    record['engineCapacity'] = lot.get('engineCapacity')
                    record['engineCapacityInLitres'] = lot.get('engineCapacityInLitres')
                    record['engineCapacityUnit'] = lot.get('engineCapacityUnit')
                    record['cylinders'] = lot.get('cylinders')
                    record['induction'] = lot.get('induction')
                    record['gears'] = lot.get('gears')
                    record['power'] = lot.get('power')
                    record['horsePower'] = lot.get('horsePower')
                    record['maxPowerRPM'] = lot.get('maxPowerRPM')
                    record['kilometres'] = lot.get('kilometres')
                    record['odometer'] = lot.get('odometer')
                    record['odometerUnit'] = lot.get('odometerUnit')
                    record['hours'] = lot.get('hours')
                    record['fuelEconomy'] = lot.get('fuelEconomy')
                    record['greenStarRating'] = lot.get('greenStarRating')
                    record['ancapSafetyRating'] = lot.get('ancapSafetyRating')
                    record['vFactsClass'] = lot.get('vFactsClass')
                    record['registrationNumber'] = lot.get('registrationNumber')
                    record['registrationJurisdiction'] = lot.get('registrationJurisdiction')
                    record['registrationExpiry'] = lot.get('registrationExpiry')
                    record['vin'] = lot.get('vin')
                    record['redbookCode'] = lot.get('redbookCode')
                    record['redbookDescription'] = lot.get('redbookDescription')
                    record['salvage'] = lot.get('salvage')
                    record['salvageStatus'] = lot.get('salvageStatus')
                    record['driveable'] = lot.get('driveable')
                    record['engineStarts'] = lot.get('engineStarts')
                    record['car_keys'] = lot.get('keys') or lot.get('car_keys')  # Handle both field names
                    record['spareKeys'] = lot.get('spareKeys')
                    record['plates'] = lot.get('plates')
                    record['platesNumber'] = lot.get('platesNumber')
                    record['sellingPlates'] = lot.get('sellingPlates')
                    record['pPlateApproved'] = lot.get('pPlateApproved')
                    record['ownersManual'] = lot.get('ownersManual')
                    record['serviceHistory'] = lot.get('serviceHistory')
                    record['towingBraked'] = lot.get('towingBraked')
                    record['gcm'] = lot.get('gcm')
                    record['gvm'] = lot.get('gvm')
                    record['tare'] = lot.get('tare')
                    record['length'] = lot.get('length')
                    record['width'] = lot.get('width')
                    record['height'] = lot.get('height')
                    record['productLine'] = lot.get('productLine')
                    record['productTypeFilter'] = lot.get('productTypeFilter')
                    record['itemLoB'] = lot.get('itemLoB')
                    record['vendorName'] = lot.get('vendorName')
                    record['businessUnitSelling'] = lot.get('businessUnitSelling')
                    record['buyMethod'] = lot.get('buyMethod')
                    record['sellingMethodName'] = lot.get('sellingMethodName')
                    record['forSale'] = lot.get('forSale')
                    record['publiclySearchable'] = lot.get('publiclySearchable')
                    record['price'] = lot.get('price')
                    record['minimumBid'] = lot.get('minimumBid')
                    record['buyNowPrice'] = lot.get('buyNowPrice')
                    record['highestBid'] = lot.get('highestBid')
                    record['productBidEnd'] = lot.get('productBidEnd')
                    record['lotNumber'] = lot.get('lotNumber')
                    record['lotNumberPrefix'] = lot.get('lotNumberPrefix')
                    record['lotNumberSuffix'] = lot.get('lotNumberSuffix')
                    record['saleLottingComplete'] = lot.get('saleLottingComplete')
                    record['productInSaleId'] = lot.get('productInSaleId')
                    record['productInSaleExternalId'] = lot.get('productInSaleExternalId')
                    record['productLocationCity'] = lot.get('productLocationCity')
                    record['productLocationSuburb'] = lot.get('productLocationSuburb')
                    record['productLocationState'] = lot.get('productLocationState')
                    record['productLocationTimeZone'] = lot.get('productLocationTimeZone')
                    record['productTypeCode'] = lot.get('productTypeCode')
                    record['productTypeTitle'] = lot.get('productTypeTitle')
                    record['etag'] = lot.get('etag')
                    record['expiryDate'] = lot.get('expiryDate')
                    
                    # Add timestamps if not present
                    if not record.get('createdAt'):
                        record['createdAt'] = datetime.now()
                    if not record.get('updatedAt'):
                        record['updatedAt'] = datetime.now()
                    
                    flattened_records.append(record)
                    
            else:
                # Handle the old JSON structure (direct items)
                print(f"      📋 Processing direct items format")
                
                # Extract the actual data items from the JSON structure
                items = []
                if 'value' in data:
                    items = data['value']
                elif isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and 'items' in data:
                    items = data['items']
                else:
                    # If data is a single object, treat it as one item
                    items = [data]
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    # Create a record with schema columns
                    record = {}
                    
                    for col in schema_columns:
                        # Handle nested sale information
                        if col in ['saleId', 'saleNumber', 'saleName', 'saleStatus', 'saleStart', 'saleEnd', 
                                   'saleEventLocation', 'businessUnitName', 'businessUnitId', 'sellingChannelId',
                                   'saleStartTimezone', 'saleEndTimezone']:
                            # Get from nested sale object with field name mapping
                            if 'sale' in item and isinstance(item['sale'], dict):
                                if col == 'saleName':
                                    record[col] = item['sale'].get('name', None)
                                elif col == 'saleStatus':
                                    record[col] = item['sale'].get('status', None)
                                else:
                                    record[col] = item['sale'].get(col, None)
                            else:
                                record[col] = item.get(col, None)
                        # Handle nested productLocation information
                        elif col in ['productLocationCity', 'productLocationSuburb', 'productLocationState', 'productLocationTimeZone']:
                            if 'productLocation' in item and isinstance(item['productLocation'], dict):
                                if col == 'productLocationCity':
                                    record[col] = item['productLocation'].get('city', None)
                                elif col == 'productLocationSuburb':
                                    record[col] = item['productLocation'].get('suburb', None)
                                elif col == 'productLocationState':
                                    record[col] = item['productLocation'].get('state', None)
                                elif col == 'productLocationTimeZone':
                                    record[col] = item['productLocation'].get('timeZone', None)
                            else:
                                record[col] = item.get(col, None)
                        # Handle nested productType information
                        elif col in ['productTypeCode', 'productTypeTitle']:
                            if 'productType' in item and isinstance(item['productType'], dict):
                                if col == 'productTypeCode':
                                    record[col] = item['productType'].get('code', None)
                                elif col == 'productTypeTitle':
                                    record[col] = item['productType'].get('title', None)
                            else:
                                record[col] = item.get(col, None)
                        # Handle the keys -> car_keys mapping
                        elif col == 'car_keys':
                            record[col] = item.get('keys') or item.get('car_keys', None)
                        else:
                            record[col] = item.get(col, None)
                    
                    # Add timestamps if not present
                    if not record.get('createdAt'):
                        record['createdAt'] = datetime.now()
                    if not record.get('updatedAt'):
                        record['updatedAt'] = datetime.now()
                    
                    flattened_records.append(record)
            
            print(f"      ✅ Extracted {len(flattened_records)} records")
            self.logger.info(f"Flattened {len(flattened_records)} records from {json_file_path}")
            
            return flattened_records
            
        except Exception as e:
            self.logger.error(f"Error flattening {json_file_path}: {str(e)}")
            print(f"      ❌ Error: {str(e)}")
            return []
    
    def convert_data_types(self, df, schema_columns):
        """
        Convert DataFrame columns to appropriate data types
        
        Args:
            df (DataFrame): Input DataFrame
            schema_columns (list): List of schema columns
            
        Returns:
            DataFrame: DataFrame with converted types
        """
        try:
            # Define datetime columns that need conversion
            datetime_columns = [
                'registrationExpiry', 'productBidEnd', 'saleStart', 'saleEnd',
                'expiryDate', 'createdAt', 'updatedAt'
            ]
            
            # Convert datetime columns
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert boolean columns
            boolean_columns = [
                'salvageStatus', 'driveable', 'engineStarts', 'car_keys', 'spareKeys',
                'plates', 'forSale', 'publiclySearchable', 'saleLottingComplete'
            ]
            
            for col in boolean_columns:
                if col in df.columns:
                    # Convert various boolean representations
                    df[col] = df[col].map({
                        True: True, False: False, 1: True, 0: False,
                        'true': True, 'false': False, 'True': True, 'False': False,
                        'yes': True, 'no': False, 'Yes': True, 'No': False
                    })
            
            # Convert numeric columns with error handling
            numeric_columns = [
                'assetExternalId', 'year', 'doors', 'seats', 'cylinders', 'gears',
                'power', 'maxPowerRPM', 'kilometres', 'odometer', 'hours',
                'ancapSafetyRating', 'platesNumber', 'towingBraked', 'gcm', 'gvm',
                'tare', 'lotNumberPrefix', 'productInSaleId', 'productInSaleExternalId',
                'saleId', 'saleNumber'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert decimal columns
            decimal_columns = [
                'engineCapacity', 'engineCapacityInLitres', 'fuelEconomy',
                'greenStarRating', 'length', 'width', 'height', 'price',
                'minimumBid', 'buyNowPrice', 'highestBid'
            ]
            
            for col in decimal_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            self.logger.info("Data type conversion completed")
            return df
            
        except Exception as e:
            self.logger.error(f"Error converting data types: {str(e)}")
            print(f"❌ Error converting data types: {str(e)}")
            return df
    
    def check_table_exists(self):
        """Check if pickles_sale_info table exists"""
        try:
            check_query = f"""
            SELECT COUNT(*) as table_count 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = '{self.table_name}'
            """
            result = self.db.read_sql(check_query)
            exists = result['table_count'].iloc[0] > 0 if not result.empty else False
            
            if exists:
                print(f"✅ Table {self.table_name} exists")
            else:
                print(f"❌ Table {self.table_name} does not exist!")
                print(f"   Please run: SQL_DDL/pickles_sale_info.sql")
            
            return exists
            
        except Exception as e:
            self.logger.error(f"Error checking table existence: {str(e)}")
            print(f"❌ Error checking table: {str(e)}")
            return False
    
    def insert_data(self, df):
        """
        Insert DataFrame into pickles_sale_info table
        
        Args:
            df (DataFrame): Data to insert
            
        Returns:
            int: Number of records inserted
        """
        try:
            if df.empty:
                print("⚠️ No data to insert")
                return 0
            
            print(f"💾 Inserting {len(df)} records into {self.table_name}...")
            
            # Insert data (allowing duplicates)
            result = self.db.write_to_sql(df, self.table_name, how='append')
            
            inserted_count = len(df)
            print(f"✅ Successfully inserted {inserted_count} records")
            self.logger.info(f"Inserted {inserted_count} records into {self.table_name}")
            
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error inserting data: {str(e)}")
            print(f"❌ Error inserting data: {str(e)}")
            return -1
    
    def create_uploaded_folder(self):
        """Create the uploaded folder if it doesn't exist"""
        try:
            if not os.path.exists(self.uploaded_folder):
                os.makedirs(self.uploaded_folder)
                print(f"📁 Created folder: {self.uploaded_folder}")
                self.logger.info(f"Created uploaded folder: {self.uploaded_folder}")
            return True
        except Exception as e:
            self.logger.error(f"Error creating uploaded folder: {str(e)}")
            print(f"❌ Error creating folder: {str(e)}")
            return False
    
    def move_processed_file(self, json_file_path):
        """
        Move successfully processed JSON file to uploaded folder
        
        Args:
            json_file_path (str): Path to the JSON file to move
            
        Returns:
            bool: True if moved successfully, False otherwise
        """
        try:
            # Ensure uploaded folder exists
            if not self.create_uploaded_folder():
                return False
            
            # Get filename
            file_name = os.path.basename(json_file_path)
            destination_path = os.path.join(self.uploaded_folder, file_name)
            
            # Handle filename conflicts by adding timestamp
            if os.path.exists(destination_path):
                name, ext = os.path.splitext(file_name)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"{name}_{timestamp}{ext}"
                destination_path = os.path.join(self.uploaded_folder, file_name)
            
            # Move the file
            shutil.move(json_file_path, destination_path)
            
            print(f"      📦 Moved to: {self.uploaded_folder}/{file_name}")
            self.logger.info(f"Moved processed file {json_file_path} to {destination_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error moving file {json_file_path}: {str(e)}")
            print(f"      ❌ Failed to move file: {str(e)}")
            return False
    
    def process_all_files(self):
        """Process all JSON files and load into database"""
        try:
            print("🚀 Starting Sale Info Data Loading...")
            
            # Check if table exists
            if not self.check_table_exists():
                return False
            
            # Load schema
            schema_columns = self.load_schema()
            if not schema_columns:
                return False
            
            # Load JSON files
            json_files = self.load_json_files()
            if not json_files:
                return False
            
            all_records = []
            successfully_processed_files = []
            
            # Process each JSON file
            for json_file in json_files:
                records = self.flatten_json_data(json_file, schema_columns)
                if records:  # Only track files that had valid records
                    all_records.extend(records)
                    successfully_processed_files.append(json_file)
            
            if not all_records:
                print("❌ No records found in JSON files")
                return False
            
            print(f"📊 Total records collected: {len(all_records)}")
            
            # Convert to DataFrame
            df = pd.DataFrame(all_records)
            
            # Ensure all schema columns exist
            for col in schema_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Select only schema columns in correct order
            df = df[schema_columns]
            
            # Rename 'keys' column to 'car_keys' to match database schema
            if 'keys' in df.columns:
                df = df.rename(columns={'keys': 'car_keys'})
                print(f"✅ Renamed 'keys' column to 'car_keys' for database compatibility")
                self.logger.info("Renamed 'keys' column to 'car_keys'")
                
                # Update schema_columns list for data type conversion
                schema_columns_for_db = [col if col != 'keys' else 'car_keys' for col in schema_columns]
            else:
                schema_columns_for_db = schema_columns
            
            # Remove auto_increment_id from DataFrame if it exists (database will auto-generate)
            if 'auto_increment_id' in df.columns:
                df = df.drop('auto_increment_id', axis=1)
                print(f"✅ Removed auto_increment_id column (database will auto-generate)")
                self.logger.info("Removed auto_increment_id column for auto-generation")
            
            # Convert data types
            df = self.convert_data_types(df, schema_columns_for_db)
            
            # Insert into database
            inserted_count = self.insert_data(df)
            
            if inserted_count > 0:
                print(f"🎉 SUCCESS! Inserted {inserted_count} records into {self.table_name}")
                
                # Move successfully processed files to uploaded folder
                print(f"\n📦 Moving processed files to {self.uploaded_folder}...")
                moved_count = 0
                for json_file in successfully_processed_files:
                    if self.move_processed_file(json_file):
                        moved_count += 1
                
                print(f"✅ Moved {moved_count}/{len(successfully_processed_files)} files")
                
                if moved_count == len(successfully_processed_files):
                    print(f"🎯 All files successfully processed and moved!")
                else:
                    print(f"⚠️ {len(successfully_processed_files) - moved_count} files could not be moved")
                
                return True
            elif inserted_count == 0:
                print(f"⚠️ No new records were inserted (all duplicates) - files not moved")
                return False
            else:
                print(f"❌ Database insertion failed - files not moved")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in process_all_files: {str(e)}")
            print(f"❌ Error: {str(e)}")
            return False


@with_error_notification()
def main():
    """Main function"""
    # Display banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║               SALE INFO JSON TO DATABASE LOADER             ║
║            Loads JSON data into pickles_sale_info           ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    
    # Initialize loader
    loader = SaleInfoLoader()
    
    # Process all files
    success = loader.process_all_files()
    
    # Always run duplicate cleaner regardless of whether new files were processed
    print("\n🧹 Running duplicate cleaner...")
    try:
        from duplicate_cleaner import DuplicateCleaner
        duplicate_cleaner = DuplicateCleaner()
        final_count = duplicate_cleaner.clean_duplicates(
            table_name='pickles_sale_info',
            partition_by='id',
            order_by='updatedAt'
        )
        print(f"✅ Duplicate cleaning completed. Final record count: {final_count}")
    except Exception as e:
        print(f"⚠️ Duplicate cleaner failed: {str(e)}")
        loader.logger.error(f"Duplicate cleaner error: {str(e)}")
    
    if success:
        print("\n✅ Data loading completed successfully!")
        return 0
    else:
        print("\n❌ Data loading failed, but duplicate cleaning was attempted!")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())