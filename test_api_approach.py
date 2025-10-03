"""
Test script to verify the API-based approach for auction detail extraction.

This script tests the new extract_auction_detail_id function that makes
direct API calls to the Velocicast endpoint.
"""

import json
import os
from datetime import datetime
from step2_generate_link import AuctionLiveScraper

def main():
    """Test the API-based auction detail extraction."""
    print("🧪 Testing API-based Auction Detail Extraction")
    print("=" * 60)
    
    try:
        # Initialize scraper
        scraper = AuctionLiveScraper()
        
        # Test registration URL (example)
        test_url = "https://www.pickles.com.au/register/auctions/1234567"
        
        print(f"📍 Testing URL: {test_url}")
        print()
        
        # Attempt to extract auction detail ID
        auction_id = scraper.extract_auction_detail_id(test_url)
        
        if auction_id:
            print(f"✅ Successfully extracted auction detail ID: {auction_id}")
        else:
            print("❌ Failed to extract auction detail ID")
            
        # Check if any API response files were saved
        sample_dir = "sample_html"
        if os.path.exists(sample_dir):
            api_files = [f for f in os.listdir(sample_dir) if f.startswith("api_response_")]
            if api_files:
                print(f"\n📁 API response files saved ({len(api_files)} files):")
                for file in sorted(api_files)[-3:]:  # Show last 3 files
                    filepath = os.path.join(sample_dir, file)
                    size = os.path.getsize(filepath)
                    print(f"   📄 {file} ({size:,} bytes)")
        
    except Exception as e:
        print(f"💥 Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n🏁 Test completed")

if __name__ == "__main__":
    main()