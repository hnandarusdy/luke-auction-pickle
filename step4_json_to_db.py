#!/usr/bin/env python3
"""
Step 4: JSON to Database Loader
Loads Pickles auction JSON files into the relational database
"""

import os
import json
import pandas as pd
import glob
import shutil
from datetime import datetime
from pickles_db_schema import PicklesDBSchema
from db import MySecondDB
from logger import get_logger
from whatsapp_notifier import with_error_notification

class PicklesJSONLoader:
    """
    Loads JSON files from step3 into the relational database
    """
    
    def __init__(self):
        """Initialize the loader"""
        self.logger = get_logger("json_to_db_loader", log_to_file=True)
        self.db = MySecondDB()
        self.schema_manager = PicklesDBSchema()
        
    def load_single_json_file(self, json_file_path, force_reload=False):
        """
        Load a single JSON file into the database
        
        Args:
            json_file_path (str): Path to the JSON file to load
            force_reload (bool): If True, reload even if event exists
            
        Returns:
            bool: True if successful
        """
        try:
            print(f"📂 Loading JSON file: {json_file_path}")
            
            # Check if file exists
            if not os.path.exists(json_file_path):
                print(f"❌ File not found: {json_file_path}")
                return False
            
            # Extract event_id from filename (e.g., "4220_20251007_224151.json" -> "4220")
            filename = os.path.basename(json_file_path)
            event_id = filename.split('_')[0]
            
            print(f"🎯 Event ID: {event_id}")
            
            # Check if event already exists in database
            if not force_reload and self._event_exists(event_id):
                print(f"⚠️ Event {event_id} already exists in database - skipping file")
                print(f"   Use force reload option if you want to reload this event")
                return False
            
            # Load and validate JSON
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'items' not in data:
                print(f"❌ Invalid JSON structure - missing 'items' array")
                return False
            
            items = data['items']
            print(f"📦 Found {len(items)} items to process")
            
            # Process each item
            loaded_count = 0
            failed_count = 0
            
            for i, item in enumerate(items, 1):
                item_id = item.get('ID', 'Unknown')
                description = item.get('Description', 'No description')[:50] + "..."
                
                print(f"   Processing item {i}/{len(items)}: ID={item_id}")
                print(f"   📋 {description}")
                
                try:
                    # Check if item already exists
                    if self._item_exists(item_id):
                        print(f"   ⚠️ Item {item_id} already exists - skipping")
                        continue
                    
                    # Insert main item record
                    self._insert_auction_item(item, event_id)
                    
                    # Insert related records
                    self._insert_item_consignors(item)
                    self._insert_item_equipment(item)
                    self._insert_item_images(item)
                    self._insert_item_damage(item)
                    
                    loaded_count += 1
                    print(f"   ✅ Successfully loaded item {item_id}")
                    
                except Exception as e:
                    failed_count += 1
                    print(f"   ❌ Failed to load item {item_id}: {str(e)}")
                    self.logger.error(f"Error loading item {item_id}: {str(e)}")
                    continue
            
            # Summary
            print(f"\n📊 Load Summary for {filename}:")
            print(f"   ✅ Successfully loaded: {loaded_count} items")
            print(f"   ❌ Failed to load: {failed_count} items")
            print(f"   📁 Source file: {json_file_path}")
            
            success = loaded_count > 0
            
            # Move file to uploaded folder if successful
            if success:
                self._move_to_uploaded_folder(json_file_path)
            
            return success
            
        except Exception as e:
            print(f"💥 Error loading JSON file: {str(e)}")
            self.logger.error(f"Error loading {json_file_path}: {str(e)}")
            return False
    
    def load_all_json_files(self, json_folder="json_data", force_reload=False):
        """
        Load all JSON files from the specified folder
        
        Args:
            json_folder (str): Folder containing JSON files
            force_reload (bool): If True, reload even if events exist
            
        Returns:
            dict: Summary of loading results
        """
        try:
            print(f"🚀 Starting bulk JSON loading from '{json_folder}' folder...")
            
            # Find all JSON files
            json_pattern = os.path.join(json_folder, "*.json")
            json_files = glob.glob(json_pattern)
            
            if not json_files:
                print(f"❌ No JSON files found in {json_folder}")
                return {"success": False, "files_processed": 0}
            
            print(f"📂 Found {len(json_files)} JSON files to process:")
            for i, file_path in enumerate(json_files, 1):
                filename = os.path.basename(file_path)
                print(f"   {i}. {filename}")
            
            # Process each file
            results = {
                "files_processed": 0,
                "files_successful": 0,
                "files_failed": 0,
                "files_skipped": 0,
                "total_items_loaded": 0,
                "processed_files": []
            }
            
            for i, json_file in enumerate(json_files, 1):
                filename = os.path.basename(json_file)
                print(f"\n{'='*60}")
                print(f"📄 Processing file {i}/{len(json_files)}: {filename}")
                print(f"{'='*60}")
                
                file_result = {
                    "filename": filename,
                    "path": json_file,
                    "success": False,
                    "skipped": False,
                    "items_loaded": 0
                }
                
                load_result = self.load_single_json_file(json_file, force_reload)
                
                if load_result is False and not force_reload:
                    # Check if it was skipped due to existing event
                    event_id = filename.split('_')[0]
                    if self._event_exists(event_id):
                        results["files_skipped"] += 1
                        file_result["skipped"] = True
                        print(f"⏭️ Skipped {filename} (event already exists)")
                    else:
                        results["files_failed"] += 1
                        print(f"❌ Failed to process {filename}")
                elif load_result:
                    results["files_successful"] += 1
                    file_result["success"] = True
                    print(f"✅ Successfully processed and moved {filename}")
                else:
                    results["files_failed"] += 1
                    print(f"❌ Failed to process {filename}")
                
                results["files_processed"] += 1
                results["processed_files"].append(file_result)
            
            # Final summary
            print(f"\n🎉 Bulk loading completed!")
            print(f"📊 Final Summary:")
            print(f"   📂 Total files found: {len(json_files)}")
            print(f"   ✅ Successfully processed: {results['files_successful']}")
            print(f"   ⏭️ Skipped (already exist): {results['files_skipped']}")
            print(f"   ❌ Failed to process: {results['files_failed']}")
            print(f"   📦 Total items loaded: {results['total_items_loaded']}")
            
            return results
            
        except Exception as e:
            print(f"💥 Error during bulk loading: {str(e)}")
            self.logger.error(f"Bulk loading error: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _move_to_uploaded_folder(self, json_file_path):
        """
        Move successfully processed JSON file to uploaded folder with timestamp suffix
        
        Args:
            json_file_path (str): Path to the original JSON file
        """
        try:
            # Create uploaded folder if it doesn't exist
            uploaded_folder = os.path.join(os.path.dirname(json_file_path), "uploaded")
            if not os.path.exists(uploaded_folder):
                os.makedirs(uploaded_folder)
                print(f"   📁 Created uploaded folder: {uploaded_folder}")
            
            # Generate new filename with timestamp suffix
            original_filename = os.path.basename(json_file_path)
            name_without_ext = os.path.splitext(original_filename)[0]
            timestamp = datetime.now().strftime("%Y%m%d")
            new_filename = f"{name_without_ext}_uploaded_{timestamp}.json"
            
            # Destination path
            destination_path = os.path.join(uploaded_folder, new_filename)
            
            # Move the file
            shutil.move(json_file_path, destination_path)
            print(f"   📦 Moved file to: {destination_path}")
            
        except Exception as e:
            print(f"   ⚠️ Warning: Could not move file to uploaded folder: {str(e)}")
            self.logger.warning(f"Error moving file {json_file_path}: {str(e)}")

    def _event_exists(self, event_id):
        """Check if an event already exists in the database"""
        try:
            query = f"SELECT COUNT(*) as count FROM pickles_auction_items WHERE event_id = '{event_id}'"
            df = self.db.read_sql(query)
            count = df['count'].iloc[0] if not df.empty else 0
            return count > 0
        except Exception as e:
            self.logger.warning(f"Error checking event existence: {str(e)}")
            return False
    
    def _item_exists(self, item_id):
        """Check if an item already exists in the database"""
        try:
            query = f"SELECT id FROM pickles_auction_items WHERE id = {item_id} LIMIT 1"
            df = self.db.read_sql(query)
            return not df.empty
        except:
            return False
    
    def _insert_auction_item(self, item, event_id):
        """Insert main auction item record with event_id"""
        try:
            item_data = {
                'id': item.get('ID'),
                'event_id': event_id,  # Add event_id to the record
                'external_id': item.get('ExternalID'),
                'item_num': item.get('ItemNum'),
                'sort_order': item.get('Sort'),
                'description': item.get('Description'),
                'country': item.get('Country'),
                'announcement': item.get('Announcement'),
                'lights': item.get('Lights'),
                'auto_bids_disabled': int(item.get('AutoBidsDisabled', 0)),
                'current_bid_type': item.get('CurrentBidType'),
                'moved': int(item.get('Moved', 0)),
                'current_bid_badge': item.get('CurrentBidBadge'),
                'current_bid_amount': item.get('CurrentBidAmount'),
                'auction_vehicle_id': item.get('AuctionVehicleID'),
                'stock_number': item.get('StockNumber'),
                'vin': item.get('VIN'),
                'vehicle_year': item.get('VehicleYear'),
                'make': item.get('Make'),
                'model': item.get('Model'),
                'trim': item.get('Trim'),
                'miles': item.get('Miles'),
                'odo_status': item.get('OdoStatus'),
                'engine': item.get('Engine'),
                'transmission': item.get('Transmission'),
                'color': item.get('Color'),
                'int_color': item.get('IntColor'),
                'int_material': item.get('IntMaterial'),
                'location': item.get('Location'),
                'fuel': item.get('Fuel'),
                'wovr_status': item.get('WOVRStatus'),
                'body': item.get('Body'),
                'service_history': item.get('ServiceHistory'),
                'odo_units': item.get('OdoUnits'),
                'additional_description': item.get('AdditionalDescription'),
                'display_location': item.get('DisplayLocation'),
                'link': item.get('Link'),
                'registration_plate': item.get('RegistrationPlate'),
                'item_link': item.get('ItemLink'),
                'fees_text': item.get('feesText'),
                'fees_link': item.get('feesLink')
            }
            
            item_df = pd.DataFrame([item_data])
            self.db.write_to_sql(item_df, 'pickles_auction_items', how='append')
            
        except Exception as e:
            raise Exception(f"Error inserting auction item: {str(e)}")
    
    def _insert_item_consignors(self, item):
        """Insert item consignors"""
        try:
            if 'Consignors' in item and item['Consignors']:
                consignors_data = []
                for consignor in item['Consignors']:
                    consignors_data.append({
                        'item_id': item.get('ID'),
                        'consignor_id': str(consignor)
                    })
                
                if consignors_data:
                    consignors_df = pd.DataFrame(consignors_data)
                    self.db.write_to_sql(consignors_df, 'pickles_item_consignors', how='append')
                    
        except Exception as e:
            self.logger.warning(f"Error inserting consignors for item {item.get('ID')}: {str(e)}")
    
    def _insert_item_equipment(self, item):
        """Insert item equipment"""
        try:
            if 'Equipment' in item and item['Equipment']:
                equipment_data = []
                for equipment in item['Equipment']:
                    equipment_data.append({
                        'item_id': item.get('ID'),
                        'equipment_name': str(equipment)[:255]  # Truncate if too long
                    })
                
                if equipment_data:
                    equipment_df = pd.DataFrame(equipment_data)
                    self.db.write_to_sql(equipment_df, 'pickles_item_equipment', how='append')
                    
        except Exception as e:
            self.logger.warning(f"Error inserting equipment for item {item.get('ID')}: {str(e)}")
    
    def _insert_item_images(self, item):
        """Insert item images"""
        try:
            if 'Images' in item and item['Images']:
                images_data = []
                for i, image_url in enumerate(item['Images']):
                    images_data.append({
                        'item_id': item.get('ID'),
                        'image_url': str(image_url),
                        'image_order': i + 1
                    })
                
                if images_data:
                    images_df = pd.DataFrame(images_data)
                    self.db.write_to_sql(images_df, 'pickles_item_images', how='append')
                    
        except Exception as e:
            self.logger.warning(f"Error inserting images for item {item.get('ID')}: {str(e)}")
    
    def _insert_item_damage(self, item):
        """Insert item damage"""
        try:
            if 'DamageItems' in item and item['DamageItems']:
                damage_data = []
                for damage in item['DamageItems']:
                    damage_data.append({
                        'item_id': item.get('ID'),
                        'damage_type': str(damage.get('Damage', ''))[:255],
                        'damage_item': str(damage.get('DItem', ''))[:255]
                    })
                
                if damage_data:
                    damage_df = pd.DataFrame(damage_data)
                    self.db.write_to_sql(damage_df, 'pickles_item_damage', how='append')
                    
        except Exception as e:
            self.logger.warning(f"Error inserting damage for item {item.get('ID')}: {str(e)}")
    
    def get_database_stats(self):
        """Get current database statistics"""
        try:
            print("📊 Database Statistics:")
            
            # Main items count
            items_df = self.db.read_sql("SELECT COUNT(*) as count FROM pickles_auction_items")
            items_count = items_df['count'].iloc[0] if not items_df.empty else 0
            print(f"   📦 Total auction items: {items_count}")
            
            # Equipment count
            equipment_df = self.db.read_sql("SELECT COUNT(*) as count FROM pickles_item_equipment")
            equipment_count = equipment_df['count'].iloc[0] if not equipment_df.empty else 0
            print(f"   🔧 Equipment records: {equipment_count}")
            
            # Images count
            images_df = self.db.read_sql("SELECT COUNT(*) as count FROM pickles_item_images")
            images_count = images_df['count'].iloc[0] if not images_df.empty else 0
            print(f"   📸 Image records: {images_count}")
            
            # Damage count
            damage_df = self.db.read_sql("SELECT COUNT(*) as count FROM pickles_item_damage")
            damage_count = damage_df['count'].iloc[0] if not damage_df.empty else 0
            print(f"   🔧 Damage records: {damage_count}")
            
            # Consignors count
            consignors_df = self.db.read_sql("SELECT COUNT(*) as count FROM pickles_item_consignors")
            consignors_count = consignors_df['count'].iloc[0] if not consignors_df.empty else 0
            print(f"   👥 Consignor records: {consignors_count}")
            
            # Sample data
            if items_count > 0:
                print("\n📋 Sample Data:")
                sample_df = self.db.read_sql("""
                    SELECT id, description, current_bid_amount, current_bid_type, vehicle_year 
                    FROM pickles_auction_items 
                    ORDER BY current_bid_amount DESC 
                    LIMIT 5
                """)
                
                for index, row in sample_df.iterrows():
                    desc = row['description'][:50] + "..." if len(str(row['description'])) > 50 else row['description']
                    print(f"   • ID: {row['id']} | Bid: ${row['current_bid_amount']} | {desc}")
            
        except Exception as e:
            print(f"❌ Error getting database stats: {str(e)}")
            self.logger.error(f"Database stats error: {str(e)}")

@with_error_notification()
def main():
    """Main function - automatically process all JSON files"""
    print("🚀 Pickles JSON to Database Loader - Step 4")
    print("="*60)
    
    loader = PicklesJSONLoader()
    
    # Show current database stats
    print("📊 Current Database Status:")
    loader.get_database_stats()
    
    print("\n" + "="*60)
    print("🎯 Auto-processing all JSON files from json_data folder...")
    print("   • Files will be processed automatically")
    print("   • Successfully processed files will be moved to uploaded/ folder")
    print("   • Files with existing event_ids will be skipped")
    print("="*60)
    
    # Automatically load all files (skip existing events)
    results = loader.load_all_json_files("json_data", force_reload=False)
    
    if results.get("success", True):
        print("\n✅ Auto-processing completed successfully!")
    else:
        print("\n❌ Auto-processing encountered errors!")
    
    # Show final database stats
    print("\n" + "="*60)
    print("📊 Final Database Status:")
    loader.get_database_stats()
    
    print("\n✅ Step 4 completed!")

if __name__ == "__main__":
    main()