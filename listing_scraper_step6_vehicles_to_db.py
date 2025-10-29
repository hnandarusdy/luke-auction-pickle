#!/usr/bin/env python3
"""
Vehicle Tracking Data to Database Script
Processes JSON files from vehicles_tracking folder and inserts into pickles_vehicle_tracking table
"""

import os
import json
import shutil
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
import logging

# Import database connection
from db import MySecondDB

class VehicleTrackingProcessor:
    """Process vehicle tracking JSON files and insert into database"""
    
    def __init__(self):
        self.db = MySecondDB()
        self.setup_logging()
        
        # Base directories
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.tracking_dir = os.path.join(self.script_dir, 'vehicles_tracking')
        self.uploaded_dir = os.path.join(self.script_dir, 'vehicles_tracking_uploaded_to_db')
        
        # Load vehicle tracking format schema
        self.load_vehicle_tracking_schema()
        
        # Ensure directories exist
        os.makedirs(self.tracking_dir, exist_ok=True)
        os.makedirs(self.uploaded_dir, exist_ok=True)
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, 'vehicle_tracking_to_db.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_vehicle_tracking_schema(self):
        """Load vehicle tracking schema from JSON file"""
        try:
            schema_file = os.path.join(self.script_dir, 'data_format', 'vehicle_tracking.json')
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)
            
            self.logger.info(f"Loaded schema with {len(self.schema)} fields")
            
        except Exception as e:
            self.logger.error(f"Error loading schema: {str(e)}")
            raise
    
    def flatten_api_response(self, api_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert API response to flat format matching schema
        
        Args:
            api_response: Original API response
            
        Returns:
            Flattened response
        """
        if 'data' not in api_response:
            raise ValueError("Invalid API response: missing 'data' section")
        
        data = api_response['data']
        flattened = {}
        
        # Extract item section
        if 'item' in data and data['item'] is not None:
            for key, value in data['item'].items():
                flattened[f"item_{key}"] = value
        
        # Extract bidding section
        if 'bidding' in data and data['bidding'] is not None:
            for key, value in data['bidding'].items():
                if key == 'userItemBidStatus':
                    # Convert object to JSON string
                    flattened[f"bidding_{key}"] = json.dumps(value) if value else None
                else:
                    flattened[f"bidding_{key}"] = value
        
        # Extract sale section
        if 'sale' in data and data['sale'] is not None:
            for key, value in data['sale'].items():
                flattened[f"sale_{key}"] = value
        
        # Add currentServerTime directly
        if 'currentServerTime' in data:
            flattened['currentServerTime'] = data['currentServerTime']
        
        return flattened
    
    def convert_timestamps(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert timestamp fields to datetime format"""
        converted = data.copy()
        
        # Convert timestamp fields from milliseconds
        timestamp_fields = [
            'item_itemBidEndTimestamp',
            'sale_saleStartTimestamp', 
            'sale_saleEndTimestamp'
        ]
        
        for field in timestamp_fields:
            if field in converted and converted[field] is not None:
                try:
                    timestamp_ms = int(converted[field])
                    dt = datetime.fromtimestamp(timestamp_ms / 1000)
                    converted[field] = dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to convert timestamp {field}: {str(e)}")
        
        # Convert currentServerTime from ISO format
        if 'currentServerTime' in converted and converted['currentServerTime']:
            try:
                dt = datetime.fromisoformat(converted['currentServerTime'].replace('Z', '+00:00'))
                converted['currentServerTime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Failed to convert currentServerTime: {str(e)}")
        
        return converted
    
    def convert_data_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert values to appropriate data types based on schema"""
        converted = {}
        
        for field_name in self.schema.keys():
            if field_name in data:
                value = data[field_name]
                data_type = self.schema[field_name]
                
                try:
                    if value is None:
                        converted[field_name] = None
                    elif data_type.startswith('BOOLEAN'):
                        converted[field_name] = bool(value)
                    elif data_type.startswith('INT') or data_type.startswith('BIGINT'):
                        converted[field_name] = int(value) if value != '' else None
                    elif data_type.startswith('DECIMAL'):
                        converted[field_name] = float(value) if value != '' else None
                    elif data_type.startswith('VARCHAR') or data_type.startswith('TEXT') or data_type == 'DATETIME':
                        converted[field_name] = str(value) if value != '' else None
                    elif data_type == 'JSON':
                        converted[field_name] = str(value) if value else None
                    else:
                        converted[field_name] = value
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to convert {field_name}: {str(e)}")
                    converted[field_name] = value
            else:
                # Field not in data, set to None
                converted[field_name] = None
        
        return converted
    
    def process_json_file(self, json_file_path: str) -> Dict[str, Any]:
        """
        Process a single JSON file and convert to database format
        
        Args:
            json_file_path: Path to JSON file
            
        Returns:
            Processed data ready for database insertion
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                api_response = json.load(f)
            
            # Convert to flat format
            flattened = self.flatten_api_response(api_response)
            
            # Convert timestamps
            flattened = self.convert_timestamps(flattened)
            
            # Convert data types
            processed = self.convert_data_types(flattened)
            
            return processed
            
        except Exception as e:
            self.logger.error(f"Error processing {json_file_path}: {str(e)}")
            raise
    
    def insert_to_database(self, data_list: List[Dict[str, Any]]) -> bool:
        """
        Insert data to pickles_vehicle_tracking table using DataFrame
        
        Args:
            data_list: List of processed data dictionaries
            
        Returns:
            Success status
        """
        try:
            if not data_list:
                self.logger.warning("No data to insert")
                return False
            
            # Create DataFrame
            df = pd.DataFrame(data_list)
            
            # Filter DataFrame to only include columns that exist in the schema
            schema_columns = list(self.schema.keys())
            df_columns = list(df.columns)
            
            # Keep only columns that are in the schema
            columns_to_keep = [col for col in df_columns if col in schema_columns]
            df_filtered = df[columns_to_keep]
            
            self.logger.info(f"Original DataFrame had {len(df_columns)} columns")
            self.logger.info(f"Filtered DataFrame has {len(columns_to_keep)} columns")
            self.logger.info(f"Inserting {len(df_filtered)} records to pickles_vehicle_tracking table")
            self.logger.info(f"Final columns: {list(df_filtered.columns)}")
            
            # Insert using database connection write_to_sql method
            self.db.write_to_sql(df_filtered, 'pickles_vehicle_tracking', how='append', index=False)
            
            self.logger.info(f"Successfully inserted {len(df_filtered)} records")
            return True
                
        except Exception as e:
            self.logger.error(f"Database insertion error: {str(e)}")
            return False
    
    def move_processed_file(self, source_path: str) -> bool:
        """
        Move processed file to uploaded directory
        
        Args:
            source_path: Source file path
            
        Returns:
            Success status
        """
        try:
            filename = os.path.basename(source_path)
            destination_path = os.path.join(self.uploaded_dir, filename)
            
            # If destination exists, add timestamp
            if os.path.exists(destination_path):
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{name}_{timestamp}{ext}"
                destination_path = os.path.join(self.uploaded_dir, filename)
            
            shutil.move(source_path, destination_path)
            self.logger.info(f"Moved {source_path} to {destination_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error moving file {source_path}: {str(e)}")
            return False
    
    def get_json_files(self) -> List[str]:
        """Get list of JSON files in vehicles_tracking directory"""
        json_files = []
        
        if not os.path.exists(self.tracking_dir):
            self.logger.warning(f"Directory not found: {self.tracking_dir}")
            return json_files
        
        for filename in os.listdir(self.tracking_dir):
            if filename.endswith('.json') and not filename.endswith('_flat.json'):
                file_path = os.path.join(self.tracking_dir, filename)
                json_files.append(file_path)
        
        return sorted(json_files)
    
    def process_all_files(self):
        """Main processing function"""
        self.logger.info("Starting vehicle tracking data processing...")
        
        # Get all JSON files
        json_files = self.get_json_files()
        
        if not json_files:
            self.logger.info("No JSON files found to process")
            return
        
        self.logger.info(f"Found {len(json_files)} JSON files to process")
        
        processed_data = []
        successful_files = []
        
        # Process each file
        for json_file in json_files:
            try:
                self.logger.info(f"Processing: {os.path.basename(json_file)}")
                
                # Process the file
                processed = self.process_json_file(json_file)
                processed_data.append(processed)
                successful_files.append(json_file)
                
                self.logger.info(f"Successfully processed: {os.path.basename(json_file)}")
                
            except Exception as e:
                self.logger.error(f"Failed to process {json_file}: {str(e)}")
                continue
        
        # Insert all processed data to database
        if processed_data:
            self.logger.info(f"Inserting {len(processed_data)} records to database...")
            
            if self.insert_to_database(processed_data):
                # Move successfully processed files
                for json_file in successful_files:
                    self.move_processed_file(json_file)
                
                self.logger.info(f"Successfully processed and uploaded {len(successful_files)} files")
            else:
                self.logger.error("Database insertion failed - files not moved")
        else:
            self.logger.warning("No data to insert")
        
        self.logger.info("Vehicle tracking data processing completed")

def main():
    """Main function"""
    try:
        processor = VehicleTrackingProcessor()
        processor.process_all_files()
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
