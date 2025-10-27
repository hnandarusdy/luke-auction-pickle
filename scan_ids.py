#!/usr/bin/env python3
"""
ID Scanner Script
Scans JSON files from Pickles auction responses and counts IDs
"""

import json
import sys
import os
from datetime import datetime

def scan_json_file(filepath):
    """
    Scan a JSON file and count IDs
    
    Args:
        filepath (str): Path to the JSON file
        
    Returns:
        dict: Analysis results
    """
    try:
        # Check if file exists
        if not os.path.exists(filepath):
            return {
                'error': f"File not found: {filepath}",
                'success': False
            }
        
        # Read and parse JSON
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract key information
        odata_count = data.get('@odata.count', 0)
        value_array = data.get('value', [])
        actual_items = len(value_array)
        
        # Count IDs
        items_with_ids = []
        for item in value_array:
            if 'id' in item and item['id']:
                items_with_ids.append(item['id'])
        
        unique_ids = list(set(items_with_ids))
        unique_count = len(unique_ids)
        
        # Get file info
        file_size = os.path.getsize(filepath)
        file_size_mb = round(file_size / (1024 * 1024), 2)
        
        return {
            'success': True,
            'filepath': filepath,
            'filename': os.path.basename(filepath),
            'file_size_mb': file_size_mb,
            'odata_count': odata_count,
            'actual_items': actual_items,
            'items_with_ids': len(items_with_ids),
            'unique_ids_count': unique_count,
            'unique_ids': unique_ids,
            'has_duplicates': len(items_with_ids) != unique_count,
            'missing_ids': actual_items - len(items_with_ids),
            'pagination_needed': odata_count > actual_items if odata_count > 0 else False,
            'remaining_items': max(0, odata_count - actual_items) if odata_count > 0 else 0
        }
        
    except json.JSONDecodeError as e:
        return {
            'error': f"Invalid JSON format: {e}",
            'success': False
        }
    except Exception as e:
        return {
            'error': f"Error reading file: {e}",
            'success': False
        }

def print_analysis(result):
    """
    Print formatted analysis results
    
    Args:
        result (dict): Analysis results from scan_json_file
    """
    if not result['success']:
        print(f"❌ Error: {result['error']}")
        return
    
    print("=" * 70)
    print("📊 JSON ID SCANNER RESULTS")
    print("=" * 70)
    
    print(f"📁 File: {result['filename']}")
    print(f"📍 Path: {result['filepath']}")
    print(f"💾 Size: {result['file_size_mb']} MB")
    
    print("\n📋 DATA SUMMARY:")
    print(f"   🎯 Total Available (@odata.count): {result['odata_count']:,}")
    print(f"   📦 Items Returned (value array): {result['actual_items']:,}")
    print(f"   🔑 Items with ID field: {result['items_with_ids']:,}")
    print(f"   ✅ Unique IDs: {result['unique_ids_count']:,}")
    
    if result['missing_ids'] > 0:
        print(f"   ⚠️  Items missing ID: {result['missing_ids']:,}")
    
    if result['has_duplicates']:
        duplicates = result['items_with_ids'] - result['unique_ids_count']
        print(f"   🔄 Duplicate IDs: {duplicates:,}")
    
    print(f"\n📈 PAGINATION STATUS:")
    if result['pagination_needed']:
        print(f"   🔀 Pagination Active: YES")
        print(f"   📊 Progress: {result['actual_items']:,} / {result['odata_count']:,} ({result['actual_items']/result['odata_count']*100:.1f}%)")
        print(f"   📋 Remaining Items: {result['remaining_items']:,}")
        
        # Calculate estimated pages needed
        if result['actual_items'] > 0:
            items_per_page = result['actual_items']
            estimated_pages = (result['remaining_items'] + items_per_page - 1) // items_per_page
            print(f"   📄 Estimated Pages Needed: {estimated_pages:,}")
    else:
        print(f"   🔀 Pagination Active: NO")
        print(f"   ✅ All data retrieved in this response")
    
    print(f"\n🔍 SAMPLE IDs:")
    sample_size = min(10, len(result['unique_ids']))
    for i in range(sample_size):
        print(f"   {i+1:2d}. {result['unique_ids'][i]}")
    
    if len(result['unique_ids']) > sample_size:
        print(f"   ... and {len(result['unique_ids']) - sample_size} more IDs")
    
    print("=" * 70)

def scan_multiple_files(filepaths):
    """
    Scan multiple JSON files and provide summary
    
    Args:
        filepaths (list): List of file paths to scan
    """
    results = []
    total_unique_ids = set()
    
    print("🔍 SCANNING MULTIPLE FILES...")
    print("=" * 70)
    
    for filepath in filepaths:
        result = scan_json_file(filepath)
        results.append(result)
        
        if result['success']:
            total_unique_ids.update(result['unique_ids'])
            print(f"✅ {os.path.basename(filepath)}: {result['unique_ids_count']:,} IDs")
        else:
            print(f"❌ {os.path.basename(filepath)}: {result['error']}")
    
    successful_scans = [r for r in results if r['success']]
    
    if successful_scans:
        print("\n📊 COMBINED SUMMARY:")
        print(f"   📁 Files Scanned: {len(successful_scans):,}")
        print(f"   🔑 Total Unique IDs Across All Files: {len(total_unique_ids):,}")
        
        total_items = sum(r['actual_items'] for r in successful_scans)
        total_odata = sum(r['odata_count'] for r in successful_scans if r['odata_count'] > 0)
        
        print(f"   📦 Total Items Retrieved: {total_items:,}")
        if total_odata > 0:
            print(f"   🎯 Total Available Items: {total_odata:,}")
    
    return results, total_unique_ids

def main():
    """
    Main function - handle command line arguments or interactive mode
    """
    if len(sys.argv) > 1:
        # Command line mode - scan provided files
        filepaths = sys.argv[1:]
        
        if len(filepaths) == 1:
            # Single file
            result = scan_json_file(filepaths[0])
            print_analysis(result)
        else:
            # Multiple files
            scan_multiple_files(filepaths)
    else:
        # Interactive mode
        print("🔍 JSON ID Scanner")
        print("Enter file path(s) to scan (or 'quit' to exit)")
        print("You can enter multiple paths separated by spaces")
        print("=" * 50)
        
        while True:
            user_input = input("\n📁 File path(s): ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not user_input:
                continue
            
            # Split input into multiple paths
            filepaths = [path.strip().strip('"').strip("'") for path in user_input.split()]
            
            if len(filepaths) == 1:
                result = scan_json_file(filepaths[0])
                print_analysis(result)
            else:
                scan_multiple_files(filepaths)

if __name__ == "__main__":
    main()