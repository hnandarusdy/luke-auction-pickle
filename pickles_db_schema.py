#!/usr/bin/env python3
"""
Pickles Auction Database Schema Generator
Generates DDL for storing Pickles auction JSON data in relational database
"""

import json
import pandas as pd
from db import MySecondDB
import logging
from datetime import datetime

logger = logging.getLogger('pickles_db_schema')

class PicklesDBSchema:
    """
    Database schema manager for Pickles auction data
    """
    
    def __init__(self):
        self.db = MySecondDB()
    
    def generate_ddl_queries(self):
        """
        Generate DDL queries for all Pickles auction tables
        
        Returns:
            str: Complete DDL script
        """
        
        ddl_script = f"""
-- =====================================================
-- Pickles Auction Database Schema DDL
-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- =====================================================

-- Drop tables if they exist (in reverse dependency order)
DROP TABLE IF EXISTS pickles_item_damage;
DROP TABLE IF EXISTS pickles_item_equipment;
DROP TABLE IF EXISTS pickles_item_images;
DROP TABLE IF EXISTS pickles_item_consignors;
DROP TABLE IF EXISTS pickles_auction_items;
DROP TABLE IF EXISTS pickles_events;

-- =====================================================
-- 1. PICKLES_EVENTS TABLE
-- Stores auction event information
-- =====================================================
CREATE TABLE pickles_events (
    event_id VARCHAR(50) PRIMARY KEY,
    event_name VARCHAR(255),
    event_date DATE,
    location VARCHAR(500),
    display_location VARCHAR(255),
    country VARCHAR(100) DEFAULT 'Australia',
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_event_date (event_date),
    INDEX idx_location (location(100)),
    INDEX idx_status (status)
);

-- =====================================================
-- 2. PICKLES_AUCTION_ITEMS TABLE  
-- Main table for individual auction items/vehicles
-- =====================================================
CREATE TABLE pickles_auction_items (
    id BIGINT PRIMARY KEY,
    external_id VARCHAR(50),
    item_num VARCHAR(20),
    sort_order VARCHAR(50),
    description TEXT,
    country VARCHAR(100) DEFAULT 'Australia',
    announcement TEXT,
    lights TEXT,
    auto_bids_disabled TINYINT(1) DEFAULT 0,
    current_bid_type VARCHAR(20),
    moved TINYINT(1) DEFAULT 0,
    current_bid_badge VARCHAR(50),
    current_bid_amount DECIMAL(12,2),
    
    -- Vehicle specific fields
    auction_vehicle_id VARCHAR(50),
    stock_number VARCHAR(50),
    vin VARCHAR(50),
    vehicle_year VARCHAR(20),
    make VARCHAR(100),
    model VARCHAR(100),
    trim TEXT,
    miles INT,
    odo_status VARCHAR(50),
    engine VARCHAR(100),
    transmission VARCHAR(150),
    color VARCHAR(100),
    int_color VARCHAR(100),
    int_material VARCHAR(100),
    location TEXT,
    fuel VARCHAR(100),
    wovr_status VARCHAR(100),
    body VARCHAR(100),
    service_history VARCHAR(100),
    odo_units VARCHAR(100),
    additional_description TEXT,
    display_location VARCHAR(255),
    
    -- Links
    link VARCHAR(500),
    registration_plate VARCHAR(100),
    item_link VARCHAR(500),
    fees_text TEXT,
    fees_link VARCHAR(500),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes for performance
    INDEX idx_external_id (external_id),
    INDEX idx_item_num (item_num),
    INDEX idx_stock_number (stock_number),
    INDEX idx_vin (vin),
    INDEX idx_make_model (make, model),
    INDEX idx_vehicle_year (vehicle_year),
    INDEX idx_current_bid_type (current_bid_type),
    INDEX idx_current_bid_amount (current_bid_amount),
    INDEX idx_body (body),
    INDEX idx_fuel (fuel),
    INDEX idx_location (location(100))
);

-- =====================================================
-- 3. PICKLES_ITEM_CONSIGNORS TABLE
-- Many-to-many relationship for item consignors
-- =====================================================
CREATE TABLE pickles_item_consignors (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id BIGINT NOT NULL,
    consignor_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_id) REFERENCES pickles_auction_items(id) ON DELETE CASCADE,
    UNIQUE KEY unique_item_consignor (item_id, consignor_id),
    INDEX idx_consignor_id (consignor_id)
);

-- =====================================================
-- 4. PICKLES_ITEM_EQUIPMENT TABLE
-- Stores equipment/features for each vehicle
-- =====================================================
CREATE TABLE pickles_item_equipment (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id BIGINT NOT NULL,
    equipment_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_id) REFERENCES pickles_auction_items(id) ON DELETE CASCADE,
    INDEX idx_item_equipment (item_id, equipment_name),
    INDEX idx_equipment_name (equipment_name)
);

-- =====================================================
-- 5. PICKLES_ITEM_IMAGES TABLE
-- Stores image URLs for each item
-- =====================================================
CREATE TABLE pickles_item_images (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id BIGINT NOT NULL,
    image_url TEXT NOT NULL,
    image_order INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_id) REFERENCES pickles_auction_items(id) ON DELETE CASCADE,
    INDEX idx_item_images (item_id, image_order)
);

-- =====================================================
-- 6. PICKLES_ITEM_DAMAGE TABLE
-- Stores damage information for each vehicle
-- =====================================================
CREATE TABLE pickles_item_damage (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id BIGINT NOT NULL,
    damage_type VARCHAR(255) NOT NULL,
    damage_item VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_id) REFERENCES pickles_auction_items(id) ON DELETE CASCADE,
    INDEX idx_item_damage (item_id),
    INDEX idx_damage_type (damage_type),
    INDEX idx_damage_item (damage_item)
);

-- =====================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================

-- Complete item view with aggregated data
CREATE VIEW vw_pickles_complete_items AS
SELECT 
    ai.*,
    GROUP_CONCAT(DISTINCT ic.consignor_id) AS consignors,
    COUNT(DISTINCT ie.id) AS equipment_count,
    COUNT(DISTINCT ii.id) AS image_count,
    COUNT(DISTINCT id_dmg.id) AS damage_count,
    GROUP_CONCAT(DISTINCT id_dmg.damage_type SEPARATOR '; ') AS damage_summary
FROM pickles_auction_items ai
LEFT JOIN pickles_item_consignors ic ON ai.id = ic.item_id
LEFT JOIN pickles_item_equipment ie ON ai.id = ie.item_id
LEFT JOIN pickles_item_images ii ON ai.id = ii.item_id
LEFT JOIN pickles_item_damage id_dmg ON ai.id = id_dmg.item_id
GROUP BY ai.id;

-- Summary statistics view
CREATE VIEW vw_pickles_auction_stats AS
SELECT 
    COUNT(*) AS total_items,
    COUNT(DISTINCT make) AS unique_makes,
    COUNT(DISTINCT model) AS unique_models,
    AVG(current_bid_amount) AS avg_bid_amount,
    MAX(current_bid_amount) AS max_bid_amount,
    MIN(current_bid_amount) AS min_bid_amount,
    COUNT(CASE WHEN current_bid_type = 'SOLD' THEN 1 END) AS sold_items,
    COUNT(CASE WHEN current_bid_type = 'IF' THEN 1 END) AS if_items,
    AVG(miles) AS avg_mileage
FROM pickles_auction_items;

-- =====================================================
-- SAMPLE QUERIES FOR DATA ANALYSIS
-- =====================================================

/*
-- Find all Toyota vehicles
SELECT * FROM pickles_auction_items WHERE make = 'Toyota' OR description LIKE '%Toyota%';

-- Get items with highest bids
SELECT id, description, current_bid_amount, current_bid_type 
FROM pickles_auction_items 
ORDER BY current_bid_amount DESC 
LIMIT 10;

-- Count items by make
SELECT 
    CASE 
        WHEN description LIKE '%Toyota%' THEN 'Toyota'
        WHEN description LIKE '%Ford%' THEN 'Ford'
        WHEN description LIKE '%Subaru%' THEN 'Subaru'
        WHEN description LIKE '%Hyundai%' THEN 'Hyundai'
        WHEN description LIKE '%Nissan%' THEN 'Nissan'
        WHEN description LIKE '%Kia%' THEN 'Kia'
        ELSE 'Other'
    END AS vehicle_make,
    COUNT(*) AS item_count,
    AVG(current_bid_amount) AS avg_bid
FROM pickles_auction_items 
GROUP BY vehicle_make 
ORDER BY item_count DESC;

-- Items with most damage
SELECT ai.id, ai.description, COUNT(id_dmg.id) AS damage_count
FROM pickles_auction_items ai
LEFT JOIN pickles_item_damage id_dmg ON ai.id = id_dmg.item_id
GROUP BY ai.id, ai.description
ORDER BY damage_count DESC
LIMIT 10;

-- Equipment analysis
SELECT equipment_name, COUNT(*) AS item_count
FROM pickles_item_equipment
GROUP BY equipment_name
ORDER BY item_count DESC
LIMIT 20;
*/

-- =====================================================
-- END OF DDL SCRIPT
-- =====================================================
"""
        
        return ddl_script
    
    def execute_ddl(self, ddl_script):
        """
        Execute DDL script on the database
        
        Args:
            ddl_script (str): DDL script to execute
        """
        try:
            print("🗄️ Executing DDL script...")
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in ddl_script.split(';') if stmt.strip()]
            
            for i, statement in enumerate(statements, 1):
                if statement.upper().startswith(('CREATE', 'DROP', 'ALTER')):
                    print(f"   Executing statement {i}: {statement[:50]}...")
                    self.db.execute_query(statement)
            
            print("✅ DDL script executed successfully!")
            
        except Exception as e:
            print(f"❌ Error executing DDL: {str(e)}")
            logger.error(f"DDL execution error: {str(e)}")
            raise
    
    def load_json_to_db(self, json_file_path, event_id=None):
        """
        Load JSON data into the database tables
        
        Args:
            json_file_path (str): Path to JSON file
            event_id (str): Optional event ID, extracted from filename if not provided
        """
        try:
            print(f"📊 Loading JSON data from {json_file_path}...")
            
            # Extract event_id from filename if not provided
            if not event_id:
                import os
                filename = os.path.basename(json_file_path)
                event_id = filename.split('_')[0]  # Get first part before underscore
            
            # Load JSON data
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'items' not in data:
                raise ValueError("JSON file must contain 'items' array")
            
            items = data['items']
            print(f"   📦 Found {len(items)} items to process")
            
            # Process each item
            for i, item in enumerate(items, 1):
                print(f"   Processing item {i}/{len(items)}: {item.get('ID', 'Unknown')}")
                
                # Insert main item record
                self._insert_auction_item(item)
                
                # Insert related records
                self._insert_item_consignors(item)
                self._insert_item_equipment(item)
                self._insert_item_images(item)
                self._insert_item_damage(item)
            
            print(f"✅ Successfully loaded {len(items)} items into database!")
            
        except Exception as e:
            print(f"❌ Error loading JSON data: {str(e)}")
            logger.error(f"JSON loading error: {str(e)}")
            raise
    
    def _insert_auction_item(self, item):
        """Insert main auction item record"""
        item_df = pd.DataFrame([{
            'id': item.get('ID'),
            'external_id': item.get('ExternalID'),
            'item_num': item.get('ItemNum'),
            'sort_order': item.get('Sort'),
            'description': item.get('Description'),
            'country': item.get('Country'),
            'announcement': item.get('Announcement'),
            'lights': item.get('Lights'),
            'auto_bids_disabled': item.get('AutoBidsDisabled', 0),
            'current_bid_type': item.get('CurrentBidType'),
            'moved': item.get('Moved', 0),
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
        }])
        
        self.db.write_to_sql(item_df, 'pickles_auction_items', how='append')
    
    def _insert_item_consignors(self, item):
        """Insert item consignors"""
        if 'Consignors' in item and item['Consignors']:
            consignors_data = []
            for consignor in item['Consignors']:
                consignors_data.append({
                    'item_id': item.get('ID'),
                    'consignor_id': consignor
                })
            
            if consignors_data:
                consignors_df = pd.DataFrame(consignors_data)
                self.db.write_to_sql(consignors_df, 'pickles_item_consignors', how='append')
    
    def _insert_item_equipment(self, item):
        """Insert item equipment"""
        if 'Equipment' in item and item['Equipment']:
            equipment_data = []
            for equipment in item['Equipment']:
                equipment_data.append({
                    'item_id': item.get('ID'),
                    'equipment_name': equipment
                })
            
            if equipment_data:
                equipment_df = pd.DataFrame(equipment_data)
                self.db.write_to_sql(equipment_df, 'pickles_item_equipment', how='append')
    
    def _insert_item_images(self, item):
        """Insert item images"""
        if 'Images' in item and item['Images']:
            images_data = []
            for i, image_url in enumerate(item['Images']):
                images_data.append({
                    'item_id': item.get('ID'),
                    'image_url': image_url,
                    'image_order': i + 1
                })
            
            if images_data:
                images_df = pd.DataFrame(images_data)
                self.db.write_to_sql(images_df, 'pickles_item_images', how='append')
    
    def _insert_item_damage(self, item):
        """Insert item damage"""
        if 'DamageItems' in item and item['DamageItems']:
            damage_data = []
            for damage in item['DamageItems']:
                damage_data.append({
                    'item_id': item.get('ID'),
                    'damage_type': damage.get('Damage', ''),
                    'damage_item': damage.get('DItem', '')
                })
            
            if damage_data:
                damage_df = pd.DataFrame(damage_data)
                self.db.write_to_sql(damage_df, 'pickles_item_damage', how='append')
    
    def generate_sample_queries(self):
        """
        Generate sample analysis queries
        
        Returns:
            str: Sample queries for data analysis
        """
        
        queries = """
-- =====================================================
-- SAMPLE ANALYSIS QUERIES
-- =====================================================

-- 1. Summary statistics
SELECT * FROM vw_pickles_auction_stats;

-- 2. Vehicle makes analysis
SELECT 
    CASE 
        WHEN description LIKE '%Toyota%' THEN 'Toyota'
        WHEN description LIKE '%Ford%' THEN 'Ford'
        WHEN description LIKE '%Subaru%' THEN 'Subaru'
        WHEN description LIKE '%Hyundai%' THEN 'Hyundai'
        WHEN description LIKE '%Nissan%' THEN 'Nissan'
        WHEN description LIKE '%Kia%' THEN 'Kia'
        ELSE 'Other'
    END AS vehicle_make,
    COUNT(*) AS item_count,
    AVG(current_bid_amount) AS avg_bid,
    MAX(current_bid_amount) AS max_bid,
    MIN(current_bid_amount) AS min_bid
FROM pickles_auction_items 
GROUP BY vehicle_make 
ORDER BY item_count DESC;

-- 3. Top 10 highest bids
SELECT 
    id, 
    description, 
    current_bid_amount, 
    current_bid_type,
    vehicle_year,
    miles
FROM pickles_auction_items 
WHERE current_bid_amount > 0
ORDER BY current_bid_amount DESC 
LIMIT 10;

-- 4. Items by body type
SELECT 
    body,
    COUNT(*) AS count,
    AVG(current_bid_amount) AS avg_bid
FROM pickles_auction_items 
WHERE body IS NOT NULL
GROUP BY body 
ORDER BY count DESC;

-- 5. Fuel type analysis
SELECT 
    fuel,
    COUNT(*) AS count,
    AVG(current_bid_amount) AS avg_bid
FROM pickles_auction_items 
WHERE fuel IS NOT NULL
GROUP BY fuel 
ORDER BY count DESC;

-- 6. Most common equipment features
SELECT 
    equipment_name,
    COUNT(*) AS item_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT item_id) FROM pickles_item_equipment), 2) AS percentage
FROM pickles_item_equipment
GROUP BY equipment_name
ORDER BY item_count DESC
LIMIT 20;

-- 7. Most common damage types
SELECT 
    damage_type,
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT item_id) AS affected_items
FROM pickles_item_damage
GROUP BY damage_type
ORDER BY occurrence_count DESC
LIMIT 15;

-- 8. Items with no damage
SELECT 
    ai.id,
    ai.description,
    ai.current_bid_amount,
    ai.vehicle_year,
    ai.miles
FROM pickles_auction_items ai
LEFT JOIN pickles_item_damage id_dmg ON ai.id = id_dmg.item_id
WHERE id_dmg.id IS NULL
ORDER BY ai.current_bid_amount DESC;

-- 9. Vehicle age vs bid amount correlation
SELECT 
    CASE 
        WHEN vehicle_year LIKE '%2023%' OR vehicle_year LIKE '%2024%' THEN '2023-2024'
        WHEN vehicle_year LIKE '%2022%' THEN '2022'
        WHEN vehicle_year LIKE '%2021%' THEN '2021'
        WHEN vehicle_year LIKE '%2020%' THEN '2020'
        WHEN vehicle_year LIKE '%2019%' OR vehicle_year LIKE '%2018%' THEN '2018-2019'
        ELSE 'Older'
    END AS year_group,
    COUNT(*) AS count,
    AVG(current_bid_amount) AS avg_bid,
    AVG(miles) AS avg_mileage
FROM pickles_auction_items
WHERE vehicle_year IS NOT NULL AND current_bid_amount > 0
GROUP BY year_group
ORDER BY avg_bid DESC;

-- 10. Detailed item analysis with all related data
SELECT 
    ai.id,
    ai.description,
    ai.current_bid_amount,
    ai.current_bid_type,
    ai.vehicle_year,
    ai.miles,
    ai.fuel,
    ai.body,
    COUNT(DISTINCT ie.id) AS equipment_count,
    COUNT(DISTINCT ii.id) AS image_count,
    COUNT(DISTINCT id_dmg.id) AS damage_count,
    GROUP_CONCAT(DISTINCT SUBSTRING(id_dmg.damage_type, 1, 30) SEPARATOR ', ') AS damage_summary
FROM pickles_auction_items ai
LEFT JOIN pickles_item_equipment ie ON ai.id = ie.item_id
LEFT JOIN pickles_item_images ii ON ai.id = ii.item_id
LEFT JOIN pickles_item_damage id_dmg ON ai.id = id_dmg.item_id
GROUP BY ai.id, ai.description, ai.current_bid_amount, ai.current_bid_type, 
         ai.vehicle_year, ai.miles, ai.fuel, ai.body
ORDER BY ai.current_bid_amount DESC;
"""
        
        return queries

def main():
    """Main function to demonstrate schema creation"""
    schema_manager = PicklesDBSchema()
    
    # Generate and print DDL
    print("🗄️ Generating DDL queries for Pickles auction database...")
    ddl = schema_manager.generate_ddl_queries()
    
    # Save DDL to file
    with open('pickles_db_ddl.sql', 'w', encoding='utf-8') as f:
        f.write(ddl)
    
    print("✅ DDL saved to pickles_db_ddl.sql")
    
    # Generate sample queries
    queries = schema_manager.generate_sample_queries()
    with open('pickles_sample_queries.sql', 'w', encoding='utf-8') as f:
        f.write(queries)
    
    print("✅ Sample queries saved to pickles_sample_queries.sql")
    
    # Ask user if they want to execute DDL
    response = input("\n❓ Do you want to execute the DDL on the database? (y/N): ")
    if response.lower() == 'y':
        schema_manager.execute_ddl(ddl)
        
        # Ask if they want to load sample data
        json_response = input("\n❓ Do you want to load the sample JSON data? (y/N): ")
        if json_response.lower() == 'y':
            json_file = "json_data/4220_20251007_224151.json"
            schema_manager.load_json_to_db(json_file)

if __name__ == "__main__":
    main()