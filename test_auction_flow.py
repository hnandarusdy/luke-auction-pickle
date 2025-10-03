#!/usr/bin/env python3
"""
Simple test script to follow the auction flow step by step.
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Import the login functionality from existing script
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pickles_login import PicklesScraper

def main():
    print("🚀 Starting auction flow test...")
    
    # Credentials
    USERNAME = "hnandarusdy2@gmail.com"
    PASSWORD = "123qwe!@#QWE"
    
    # Step 1: Login to Pickles (reusing from pickles_login.py)
    print("\n1️⃣ Logging into Pickles...")
    scraper = PicklesScraper()
    
    if not scraper.login(USERNAME, PASSWORD):
        print("❌ Login failed!")
        scraper.cleanup()
        return
    
    print("✅ Successfully logged in!")
    
    # Step 2: Navigate to the specific registration URL
    registration_url = "https://www.pickles.com.au/damaged-salvage/pickles-live/registration?p_p_id=PicklesLiveRegistrationPortlet_WAR_PWRWeb&p_p_lifecycle=1&p_p_state=normal&saleId=11999"
    print(f"\n2️⃣ Navigating to registration page...")
    print(f"URL: {registration_url}")
    
    scraper.driver.get(registration_url)
    time.sleep(3)
    
    print("✅ Reached registration page!")
    
    # Step 3: Find the "Just Watch" link
    print("\n3️⃣ Looking for 'Just Watch' link...")
    
    try:
        wait = WebDriverWait(scraper.driver, 15)
        
        # Try multiple possible selectors for the "Just Watch" link
        selectors_to_try = [
            "//a[contains(text(), 'Just Watch')]",
            "//a[contains(text(), 'Just watch')]", 
            "//a[contains(text(), 'JUST WATCH')]",
            "//button[contains(text(), 'Just Watch')]",
            "//button[contains(text(), 'Just watch')]",
            "//input[@value='Just Watch']",
            "//input[@value='Just watch']"
        ]
        
        just_watch_element = None
        for selector in selectors_to_try:
            try:
                just_watch_element = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                print(f"✅ Found 'Just Watch' element using selector: {selector}")
                break
            except TimeoutException:
                continue
        
        if not just_watch_element:
            print("❌ Could not find 'Just Watch' link!")
            
            # Let's see what links are available on the page
            print("\n🔍 Available links on the page:")
            links = scraper.driver.find_elements(By.TAG_NAME, "a")
            for i, link in enumerate(links[:20]):  # Show first 20 links
                text = link.text.strip()
                href = link.get_attribute("href")
                if text:
                    print(f"   {i+1}. '{text}' -> {href}")
            
            # Also check buttons
            print("\n🔍 Available buttons on the page:")
            buttons = scraper.driver.find_elements(By.TAG_NAME, "button")
            for i, button in enumerate(buttons[:10]):  # Show first 10 buttons
                text = button.text.strip()
                if text:
                    print(f"   {i+1}. '{text}'")
            
            input("❌ Press Enter to continue anyway...")
            scraper.cleanup()
            return
        
        # Get the URL before clicking
        just_watch_url = just_watch_element.get_attribute("href")
        print(f"🔗 Just Watch URL: {just_watch_url}")
        
        # Step 4: Click the "Just Watch" link
        print("\n4️⃣ Clicking 'Just Watch' link...")
        just_watch_element.click()
        
        print("✅ Clicked 'Just Watch' link!")
        
        # Step 5: Wait 5 seconds then open new tab with API URL
        print("\n5️⃣ Waiting 5 seconds...")
        time.sleep(5)
        
        api_url = "https://api.pickles-au.velocicast.io/api/events/refresh/user-events"
        print(f"🆕 Opening new tab for API URL: {api_url}")
        
        # Open new tab
        scraper.driver.execute_script("window.open('');")
        
        # Switch to new tab
        tabs = scraper.driver.window_handles
        scraper.driver.switch_to.window(tabs[-1])
        
        # Navigate to the API URL in the new tab
        scraper.driver.get(api_url)
        
        print("✅ Opened new tab and navigated to API URL!")
        
        # Step 6: Print the response and wait for enter
        print("\n6️⃣ Getting API response...")
        time.sleep(3)  # Wait for page to load
        
        print("\n" + "="*80)
        print("📄 API RESPONSE:")
        print("="*80)
        print(f"URL: {scraper.driver.current_url}")
        print(f"Title: {scraper.driver.title}")
        
        # Get the page source (API response)
        page_source = scraper.driver.page_source
        
        # Try to parse JSON and extract EventID
        try:
            import json
            import re
            
            # Extract JSON from the page source (remove HTML wrapper if present)
            json_match = re.search(r'\[.*\]', page_source)
            if json_match:
                json_text = json_match.group(0)
                data = json.loads(json_text)
                
                print("\n🎯 EXTRACTED EVENT IDs:")
                print("-" * 30)
                
                if isinstance(data, list):
                    for i, item in enumerate(data, 1):
                        if isinstance(item, dict) and 'EventID' in item:
                            event_id = item['EventID']
                            print(f"   {i}. EventID: {event_id}")
                        else:
                            print(f"   {i}. No EventID found in item: {item}")
                else:
                    print(f"   Unexpected data format: {type(data)}")
                    
            else:
                print("\n❌ No JSON array found in response")
                print("Raw response:")
                print("-" * 50)
                print(page_source[:1000])
                
        except Exception as e:
            print(f"\n❌ Error parsing JSON: {str(e)}")
            print("Raw response:")
            print("-" * 50)
            print(page_source[:1000])
        
        print("="*80)
        
        # Also check for any API calls or network activity
        print("\n🍪 Current cookies:")
        cookies = scraper.driver.get_cookies()
        for cookie in cookies:
            print(f"   {cookie['name']}: {cookie['value'][:50]}...")
        
        print(f"\n⏸️ Waiting for Enter to continue...")
        input("Press Enter to continue...")
        
    except Exception as e:
        print(f"💥 Error during flow: {str(e)}")
        
    finally:
        print("\n🧹 Cleaning up...")
        scraper.cleanup()
        print("✅ Done!")

if __name__ == "__main__":
    main()
