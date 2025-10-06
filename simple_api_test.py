#!/usr/bin/env python3
"""
Simple Pickles API Test Script

This script logs into Pickles website and makes an API request to fetch bidding data.
Reuses the PicklesScraper class from pickles_login.py

Author: GitHub Copilot
Date: October 6, 2025
"""

import requests
from pickles_login import PicklesScraper
from logger import get_logger


def make_api_request(scraper, api_url, method="GET", payload=None):
    """
    Make an API request using the authenticated session from the scraper.
    
    Args:
        scraper: Authenticated PicklesScraper instance
        api_url: URL to make the API request to
        method: HTTP method (GET or POST)
        payload: Data to send for POST requests
        
    Returns:
        tuple: (success: bool, response_data: dict/str)
    """
    try:
        # Get cookies from the selenium driver
        selenium_cookies = scraper.driver.get_cookies()
        
        # Convert selenium cookies to requests session cookies
        session = requests.Session()
        for cookie in selenium_cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        
        # Set headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.pickles.com.au/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        }
        
        # Add Content-Type for POST requests
        if method.upper() == "POST":
            headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        
        print(f"🌐 Making {method} request to: {api_url}")
        if payload:
            print(f"📦 Payload: {payload[:100]}..." if len(payload) > 100 else f"📦 Payload: {payload}")
        
        # Make the API request
        if method.upper() == "POST":
            response = session.post(api_url, headers=headers, data=payload, timeout=30)
        else:
            response = session.get(api_url, headers=headers, timeout=30)
        
        print(f"📊 Response Status Code: {response.status_code}")
        print(f"📝 Response Headers: {dict(response.headers)}")
        
        # Check if request was successful
        if response.status_code == 200:
            try:
                # Try to parse as JSON
                json_data = response.json()
                return True, json_data
            except ValueError:
                # If not JSON, return text
                return True, response.text
        else:
            error_msg = f"API request failed with status {response.status_code}"
            print(f"❌ {error_msg}")
            return False, error_msg
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"❌ {error_msg}")
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"💥 {error_msg}")
        return False, error_msg


def print_json_response(data, max_depth=3, current_depth=0):
    """
    Pretty print JSON response with limited depth to avoid overwhelming output.
    
    Args:
        data: JSON data to print
        max_depth: Maximum nesting depth to display
        current_depth: Current nesting level
    """
    indent = "  " * current_depth
    
    if current_depth >= max_depth:
        print(f"{indent}... (truncated - max depth reached)")
        return
    
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                print(f"{indent}{key}:")
                print_json_response(value, max_depth, current_depth + 1)
            else:
                # Truncate long string values
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"{indent}{key}: {value}")
    elif isinstance(data, list):
        print(f"{indent}[Array with {len(data)} items]")
        if data and current_depth < max_depth - 1:
            print(f"{indent}First item:")
            print_json_response(data[0], max_depth, current_depth + 1)
            if len(data) > 1:
                print(f"{indent}... ({len(data) - 1} more items)")
    else:
        print(f"{indent}{data}")


def main():
    """Main function."""
    # Initialize logger
    logger = get_logger("api_test", log_to_file=True)
    logger.info("=== Pickles API Test Started ===")
    
    # Display banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    PICKLES API TEST TOOL                    ║
║                   Login + API Request                       ║
║                                                              ║
║  Author: GitHub Copilot                                     ║
║  Date: October 6, 2025                                      ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    
    # Credentials (you can move these to config.yaml if needed)
    USERNAME = "hnandarusdy2@gmail.com"
    PASSWORD = "123qwe!@#QWE"
    
    # API URLs to test
    API_URLS = [
        {
            "name": "Bidding Lot Details",
            "url": "https://www.pickles.com.au/api-website/buyer/ms-bidding-controller/v2/api/bidding/62043834/lot-details/",
            "method": "GET",
            "payload": None
        },
        {
            "name": "Product Search", 
            "url": "https://www.pickles.com.au/api-website/buyer/ms-web-asset-search/v2/api/product/public/11862/search",
            "method": "POST",
            "payload": "data=eyJzZWFyY2giOiIqIiwiZmFjZXRzIjpbInByb2R1Y3RUeXBlL3RpdGxlLHNvcnQ6dmFsdWUiLCJidXlNZXRob2Qsc29ydDp2YWx1ZSIsInNhbHZhZ2Usc29ydDp2YWx1ZSIsIndvdnIsc29ydDp2YWx1ZSIsImJpa2VUeXBlLGNvdW50Ojk5OTksc29ydDp2YWx1ZSIsImJvZHksY291bnQ6OTk5OSxzb3J0OnZhbHVlIiwidHJhbnNtaXNzaW9uLHNvcnQ6dmFsdWUiLCJkcml2ZVR5cGUsc29ydDp2YWx1ZSIsImZ1ZWxUeXBlLHNvcnQ6dmFsdWUiLCJpbmR1Y3Rpb24sc29ydDp2YWx1ZSIsImN5bGluZGVycyxzb3J0OnZhbHVlIiwicFBsYXRlQXBwcm92ZWQsc29ydDp2YWx1ZSIsInZGYWN0c0NsYXNzLHNvcnQ6dmFsdWUiLCJzZWF0cyxzb3J0OnZhbHVlIiwiZG9vcnMsc29ydDp2YWx1ZSIsImVuZ2luZUNhcGFjaXR5SW5MaXRyZXMiLCJ5ZWFyIiwib2RvbWV0ZXIiLCJwb3dlciIsInRvd2luZ0JyYWtlZCIsImFuY2FwU2FmZXR5UmF0aW5nIiwiZnVlbEVjb25vbXkiLCJob3VycyIsImdyZWVuU3RhclJhdGluZyIsIm1ha2UsY291bnQ6OTk5OSxzb3J0OnZhbHVlIiwibW9kZWxGYWNldCxjb3VudDo5OTk5LHNvcnQ6dmFsdWUiLCJiYWRnZUZhY2V0LGNvdW50Ojk5OTksc29ydDp2YWx1ZSIsInNlcmllc0ZhY2V0LGNvdW50Ojk5OTksc29ydDp2YWx1ZSIsInByb2R1Y3RMb2NhdGlvbi9zdGF0ZSxzb3J0OnZhbHVlIiwicHJvZHVjdExvY2F0aW9uL2NpdHlGYWNldCxjb3VudDo5OTk5LHNvcnQ6dmFsdWUiLCJwcm9kdWN0TG9jYXRpb24vc3VidXJiRmFjZXQsY291bnQ6OTk5OSxzb3J0OnZhbHVlIiwiY29sb3VyLGNvdW50Ojk5OTksc29ydDp2YWx1ZSIsImNvbG91ck1hbnVmYWN0dXJlckZhY2V0LGNvdW50Ojk5OTksc29ydDp2YWx1ZSIsImNvbG91clVuaXF1ZUZhY2V0LGNvdW50Ojk5OTksc29ydDp2YWx1ZSJdLCJjb3VudCI6dHJ1ZSwidG9wIjozMCwic2tpcCI6MCwib3JkZXJieSI6IiIsImZpbHRlciI6IiJ9"
        }
    ]
    
    print("🚀 Starting Pickles API test...")
    print(f"🎯 Testing {len(API_URLS)} API endpoints:")
    for i, api in enumerate(API_URLS, 1):
        method = api.get('method', 'GET')
        print(f"   {i}. {api['name']} ({method}): {api['url']}")
    
    try:
        # Initialize scraper
        with PicklesScraper(headless=False, wait_timeout=15) as scraper:
            logger.info("Starting login process...")
            print("\\n🔐 Logging into Pickles...")
            
            # Attempt login
            success = scraper.login(USERNAME, PASSWORD)
            
            if success:
                print("✅ Login successful!")
                logger.info("Login completed successfully")
                
                # Display current status
                current_url = scraper.get_current_url()
                print(f"📍 Current URL: {current_url}")
                
                # Test all API endpoints
                print(f"\\n🌐 Testing {len(API_URLS)} API endpoints...")
                
                successful_requests = 0
                failed_requests = 0
                
                for i, api_info in enumerate(API_URLS, 1):
                    api_name = api_info['name']
                    api_url = api_info['url']
                    api_method = api_info.get('method', 'GET')
                    api_payload = api_info.get('payload', None)
                    
                    print(f"\\n{'='*70}")
                    print(f"🔍 TEST {i}/{len(API_URLS)}: {api_name}")
                    print(f"🌐 URL: {api_url}")
                    print(f"📋 Method: {api_method}")
                    if api_payload:
                        print(f"📦 Has Payload: Yes")
                    print(f"{'='*70}")
                    
                    success, response_data = make_api_request(scraper, api_url, api_method, api_payload)
                    
                    if success:
                        print(f"\\n✅ {api_name} - Request Successful!")
                        successful_requests += 1
                        
                        print("\\n📋 RESPONSE DATA:")
                        print("-"*50)
                        
                        # Print response in a readable format
                        if isinstance(response_data, dict):
                            print_json_response(response_data)
                        elif isinstance(response_data, str):
                            # If it's a string, try to parse as JSON first
                            try:
                                import json
                                json_data = json.loads(response_data)
                                print_json_response(json_data)
                            except:
                                # If not JSON, print as text (truncated if too long)
                                if len(response_data) > 2000:
                                    print(response_data[:2000])
                                    print("\\n... (response truncated)")
                                else:
                                    print(response_data)
                        else:
                            print(f"Response type: {type(response_data)}")
                            print(response_data)
                        
                        logger.info(f"{api_name} API request completed successfully")
                        
                    else:
                        print(f"❌ {api_name} - Request Failed!")
                        print(f"Error: {response_data}")
                        failed_requests += 1
                        logger.error(f"{api_name} API request failed: {response_data}")
                    
                    # Small delay between API requests
                    if i < len(API_URLS):
                        print("\\n⏳ Waiting 2 seconds before next request...")
                        import time
                        time.sleep(2)
                
                # Final summary
                print(f"\\n{'='*70}")
                print("🎉 API TESTING COMPLETED!")
                print(f"📊 Summary:")
                print(f"   ✅ Successful: {successful_requests}/{len(API_URLS)}")
                print(f"   ❌ Failed: {failed_requests}/{len(API_URLS)}")
                print(f"{'='*70}")
                
            else:
                print("❌ Login failed!")
                logger.error("Login process failed")
                
                print("\\n🔍 Please check:")
                print("   - Your internet connection")
                print("   - Website availability")
                print("   - Login credentials")
                print("   - Website structure changes")
                
                return 1
                
    except KeyboardInterrupt:
        print("\\n\\n👋 Application interrupted by user. Exiting...")
        logger.info("Application interrupted by user")
        return 0
        
    except Exception as e:
        error_msg = f"Unexpected error occurred: {str(e)}"
        print(f"\\n💥 {error_msg}")
        logger.exception(error_msg)
        
        print("\\n🆘 Troubleshooting tips:")
        print("   - Check if Chrome browser is installed")
        print("   - Verify internet connection")
        print("   - Check if API endpoint is correct")
        print("   - Review log files in the 'logs' directory")
        
        return 1
    
    finally:
        logger.info("=== Pickles API Test Ended ===")
        print("\\n📋 Check the 'logs' directory for detailed execution logs.")
        
        # Wait for user input before exit
        try:
            input("\\n⏸️  Press Enter to exit...")
        except KeyboardInterrupt:
            print("\\n👋 Exiting...")
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)