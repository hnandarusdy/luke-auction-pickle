"""
Pickles Website Scraper with Login Functionality

This module provides a reusable class for scraping the Pickles website
with automatic login functionality using Selenium WebDriver.

Author: GitHub Copilot
Date: October 3, 2025
"""

import time
from typing import Optional
from selenium import webdriver
from logger import get_logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager


class PicklesScraper:
    """
    A web scraper class for the Pickles website with automatic login functionality.
    
    This class provides methods to:
    - Initialize a Chrome WebDriver
    - Navigate to the Pickles website
    - Perform automatic login
    - Handle common web scraping scenarios
    """
    
    def __init__(self, headless: bool = False, wait_timeout: int = 10):
        """
        Initialize the PicklesScraper.
        
        Args:
            headless (bool): Run browser in headless mode (invisible)
            wait_timeout (int): Default timeout for WebDriverWait operations
        """
        self.base_url = "https://www.pickles.com.au/"
        self.wait_timeout = wait_timeout
        self.driver: Optional[webdriver.Chrome] = None
        self.wait: Optional[WebDriverWait] = None
        self.headless = headless
        
        # Setup logging using custom logger
        self.logger = get_logger("pickles_scraper", log_to_file=True)
        
        # Login selectors
        self.login_button_xpath = '//*[@id="pickles-login-widget-space"]/ul/li[2]/a'
        self.username_input_id = 'pickles-sign-in-username'
        self.password_input_id = 'pickles-sign-in-password'
        self.login_submit_xpath = '/html/body/form/div[4]/div[1]/div/div[2]/div/div/div[2]/button'
    
    def setup_driver(self) -> None:
        """
        Setup and configure the Chrome WebDriver.
        
        Raises:
            WebDriverException: If driver setup fails
        """
        try:
            # Chrome options
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Additional options for stability
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            # Additional options to avoid detection
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            try:
                # First try with automatic ChromeDriverManager
                self.logger.info("Attempting to use ChromeDriverManager...")
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                
            except Exception as chrome_manager_error:
                self.logger.warning(f"ChromeDriverManager failed: {chrome_manager_error}")
                self.logger.info("Attempting to use system Chrome driver...")
                
                # Fallback: try to use Chrome driver from system PATH
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                except Exception as system_driver_error:
                    self.logger.error(f"System Chrome driver failed: {system_driver_error}")
                    
                    # Final fallback: provide detailed error message
                    error_msg = f"""
Chrome WebDriver setup failed with both methods:
1. ChromeDriverManager: {chrome_manager_error}
2. System driver: {system_driver_error}

Please try one of these solutions:
1. Update Chrome browser to the latest version
2. Manually download ChromeDriver from: https://sites.google.com/chromium.org/driver/
3. Install ChromeDriver via: pip install --upgrade webdriver-manager
4. Add ChromeDriver to your system PATH
                    """
                    raise WebDriverException(error_msg)
            
            # Setup WebDriverWait
            self.wait = WebDriverWait(self.driver, self.wait_timeout)
            
            # Disable automation indicators
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info("Chrome WebDriver setup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome WebDriver: {str(e)}")
            raise WebDriverException(f"Driver setup failed: {str(e)}")
    
    def navigate_to_site(self) -> None:
        """
        Navigate to the Pickles website.
        
        Raises:
            WebDriverException: If navigation fails
        """
        try:
            if not self.driver:
                raise WebDriverException("Driver not initialized. Call setup_driver() first.")
            
            self.logger.info(f"Navigating to {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            self.logger.info("Successfully navigated to Pickles website")
            
        except TimeoutException:
            self.logger.error("Timeout while loading the website")
            raise
        except Exception as e:
            self.logger.error(f"Failed to navigate to website: {str(e)}")
            raise
    
    def click_login_button(self) -> None:
        """
        Click the login button on the main page.
        
        Raises:
            TimeoutException: If login button is not found within timeout
            NoSuchElementException: If login button element is not found
        """
        try:
            self.logger.info("Looking for login button...")
            
            # Wait for login button to be clickable
            login_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, self.login_button_xpath))
            )
            
            # Scroll to button if needed
            self.driver.execute_script("arguments[0].scrollIntoView(true);", login_button)
            time.sleep(1)  # Brief pause for smooth scrolling
            
            # Click the login button
            login_button.click()
            self.logger.info("Login button clicked successfully")
            
        except TimeoutException:
            self.logger.error("Login button not found within timeout period")
            raise
        except Exception as e:
            self.logger.error(f"Failed to click login button: {str(e)}")
            raise
    
    def wait_for_login_form(self) -> None:
        """
        Wait for the login form to appear after clicking login button.
        
        Raises:
            TimeoutException: If login form doesn't appear within timeout
        """
        try:
            self.logger.info("Waiting for login form to appear...")
            
            # Wait for username input field to be present and visible
            self.wait.until(
                EC.presence_of_element_located((By.ID, self.username_input_id))
            )
            
            # Also wait for password field
            self.wait.until(
                EC.presence_of_element_located((By.ID, self.password_input_id))
            )
            
            self.logger.info("Login form appeared successfully")
            
        except TimeoutException:
            self.logger.error("Login form did not appear within timeout period")
            raise
    
    def enter_credentials(self, username: str, password: str) -> None:
        """
        Enter username and password in the login form.
        
        Args:
            username (str): Username/email for login
            password (str): Password for login
            
        Raises:
            NoSuchElementException: If input fields are not found
        """
        try:
            self.logger.info("Entering login credentials...")
            
            # Find and fill username field
            username_field = self.driver.find_element(By.ID, self.username_input_id)
            username_field.clear()
            username_field.send_keys(username)
            
            # Find and fill password field
            password_field = self.driver.find_element(By.ID, self.password_input_id)
            password_field.clear()
            password_field.send_keys(password)
            
            self.logger.info("Credentials entered successfully")
            
        except NoSuchElementException as e:
            self.logger.error(f"Login form fields not found: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to enter credentials: {str(e)}")
            raise
    
    def submit_login(self) -> None:
        """
        Submit the login form by clicking the login button.
        
        Raises:
            TimeoutException: If login button is not found or clickable
        """
        try:
            self.logger.info("Submitting login form...")
            
            # Wait for login submit button to be clickable
            login_submit = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, self.login_submit_xpath))
            )
            
            # Click the submit button
            login_submit.click()
            self.logger.info("Login form submitted")
            
            # Wait a moment for the login to process
            time.sleep(2)
            
        except TimeoutException:
            self.logger.error("Login submit button not found or not clickable")
            raise
        except Exception as e:
            self.logger.error(f"Failed to submit login: {str(e)}")
            raise
    
    def login(self, username: str, password: str) -> bool:
        """
        Complete login process: navigate, click login, enter credentials, submit.
        
        Args:
            username (str): Username/email for login
            password (str): Password for login
            
        Returns:
            bool: True if login appears successful, False otherwise
        """
        try:
            # Ensure driver is setup
            if not self.driver:
                self.setup_driver()
            
            # Navigate to site
            self.navigate_to_site()
            
            # Click login button
            self.click_login_button()
            
            # Wait for login form
            self.wait_for_login_form()
            
            # Enter credentials
            self.enter_credentials(username, password)
            
            # Submit login
            self.submit_login()
            
            # Check if login was successful (you might want to customize this)
            time.sleep(3)  # Wait for redirect/page update
            
            # Simple check - look for login button disappearing or user menu appearing
            try:
                # If we can still find the login button, login probably failed
                self.driver.find_element(By.XPATH, self.login_button_xpath)
                self.logger.warning("Login button still present - login may have failed")
                return False
            except NoSuchElementException:
                # Login button gone - likely successful
                self.logger.info("Login appears successful")
                return True
                
        except Exception as e:
            self.logger.error(f"Login process failed: {str(e)}")
            return False
    
    def get_page_title(self) -> str:
        """
        Get the current page title.
        
        Returns:
            str: Current page title
        """
        if self.driver:
            return self.driver.title
        return ""
    
    def get_current_url(self) -> str:
        """
        Get the current page URL.
        
        Returns:
            str: Current page URL
        """
        if self.driver:
            return self.driver.current_url
        return ""
    
    def take_screenshot(self, filename: str = "screenshot.png") -> bool:
        """
        Take a screenshot of the current page.
        
        Args:
            filename (str): Filename for the screenshot
            
        Returns:
            bool: True if screenshot saved successfully
        """
        try:
            if self.driver:
                self.driver.save_screenshot(filename)
                self.logger.info(f"Screenshot saved as {filename}")
                return True
        except Exception as e:
            self.logger.error(f"Failed to take screenshot: {str(e)}")
        return False
    
    def close(self) -> None:
        """
        Close the browser and cleanup resources.
        """
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("Browser closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing browser: {str(e)}")
            finally:
                self.driver = None
                self.wait = None
    
    def __enter__(self):
        """Context manager entry."""
        self.setup_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()



