
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
    
    def navigate_to_auction_page(self, auction_url: str = "https://www.pickles.com.au/upcoming-auctions/salvage") -> bool:
        """
        Navigate to the auction page.
        
        Args:
            auction_url (str): The auction page URL to navigate to
            
        Returns:
            bool: True if navigation was successful, False otherwise
        """
        try:
            self.logger.info(f"Navigating to auction page: {auction_url}")
            self.driver.get(auction_url)
            
            # Wait for the auction list to load
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "auctions-list_auctions-list__XP23h")))
            self.logger.info("Successfully navigated to auction page")
            return True
            
        except TimeoutException:
            self.logger.error("Timeout waiting for auction page to load")
            return False
        except Exception as e:
            self.logger.error(f"Failed to navigate to auction page: {str(e)}")
            return False
    
    def extract_auction_details(self) -> list:
        """
        Extract auction details from the current auction listing page.
        
        Returns:
            list: List of dictionaries containing auction information
        """
        auctions = []
        
        try:
            # Find the auction list container
            auction_container = self.driver.find_element(By.CLASS_NAME, "auctions-list_auctions-list__XP23h")
            self.logger.info("Found auction container")
            
            # Find all auction articles
            auction_articles = auction_container.find_elements(By.CSS_SELECTOR, "article[data-testid='auction-card']")
            self.logger.info(f"Found {len(auction_articles)} auction articles")
            
            for idx, article in enumerate(auction_articles, 1):
                try:
                    auction_info = {}
                    
                    # Get auction title
                    title_element = article.find_element(By.CSS_SELECTOR, ".auction-card_acb-title__Etjpm")
                    auction_info['title'] = title_element.text.strip() if title_element else "N/A"
                    
                    # Get auction location
                    location_elements = article.find_elements(By.CSS_SELECTOR, ".ac-location-status_value__PLM4O")
                    auction_info['location'] = location_elements[0].text.strip() if location_elements else "N/A"
                    
                    # Get auction status
                    auction_info['status'] = location_elements[1].text.strip() if len(location_elements) > 1 else "N/A"
                    
                    # Find Sale Info link
                    sale_info_link = None
                    try:
                        # Look for the footer and then the Sale Info link
                        footer = article.find_element(By.CSS_SELECTOR, "footer[data-testid='ac-footer']")
                        sale_info_links = footer.find_elements(By.XPATH, ".//a[.//span[text()='Sale Info']]")
                        
                        if sale_info_links:
                            sale_info_link = sale_info_links[0].get_attribute('href')
                            auction_info['sale_info_url'] = sale_info_link
                        else:
                            auction_info['sale_info_url'] = None
                            
                    except NoSuchElementException:
                        self.logger.warning(f"No Sale Info link found for auction {idx}")
                        auction_info['sale_info_url'] = None
                    
                    auctions.append(auction_info)
                    self.logger.info(f"Extracted auction {idx}: {auction_info['title']}")
                    
                except Exception as e:
                    self.logger.error(f"Error extracting details for auction {idx}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(auctions)} auction details")
            return auctions
            
        except NoSuchElementException:
            self.logger.error("Could not find auction list container")
            return []
        except Exception as e:
            self.logger.error(f"Error extracting auction details: {str(e)}")
            return []
    
    def navigate_to_sale_info(self, sale_info_url: str) -> bool:
        """
        Navigate to a specific Sale Info page.
        
        Args:
            sale_info_url (str): The Sale Info URL to navigate to
            
        Returns:
            bool: True if navigation was successful, False otherwise
        """
        try:
            self.logger.info(f"Navigating to Sale Info page: {sale_info_url}")
            self.driver.get(sale_info_url)
            
            # Wait for the page to load (looking for common elements)
            time.sleep(3)  # Gentle delay for page load
            
            self.logger.info("Successfully navigated to Sale Info page")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to navigate to Sale Info page: {str(e)}")
            return False
    
    def extract_sale_info_details(self) -> dict:
        """
        Extract additional details from the Sale Info page.
        
        Returns:
            dict: Dictionary containing sale info details
        """
        sale_details = {
            'auction_registration': None,
            'sale_title': None,
            'sale_date': None,
            'sale_occurs': None
        }
        
        try:
            # Look for bidLiveButton
            try:
                bid_live_button = self.driver.find_element(By.ID, "bidLiveButton")
                auction_registration_href = bid_live_button.get_attribute('href')
                sale_details['auction_registration'] = auction_registration_href
                self.logger.info(f"Found bidLiveButton href: {auction_registration_href}")
            except NoSuchElementException:
                self.logger.info("No bidLiveButton found on page")
                sale_details['auction_registration'] = None
            
            # Extract information from sale summary rows with the new structure
            try:
                # Look for sale summary rows with the pattern: label in first <p>, value in second <p>
                sale_summary_rows = self.driver.find_elements(By.CSS_SELECTOR, "div.sale-summary-row")
                
                for row in sale_summary_rows:
                    try:
                        # Get all <p> elements in this row
                        p_elements = row.find_elements(By.TAG_NAME, "p")
                        
                        if len(p_elements) >= 2:
                            label = p_elements[0].text.strip().lower().replace(':', '')
                            value = p_elements[1].text.strip()
                            
                            self.logger.info(f"Found sale info - Label: '{label}', Value: '{value}'")
                            
                            # Map labels to our fields
                            if 'date' in label:
                                sale_details['sale_date'] = value
                            elif any(keyword in label for keyword in ['location', 'address', 'venue', 'occurs', 'sale occurs']):
                                sale_details['sale_occurs'] = value
                            elif any(keyword in label for keyword in ['title', 'sale', 'auction']):
                                if not sale_details['sale_title']:  # Only set if not already set
                                    sale_details['sale_title'] = value
                    
                    except Exception as e:
                        self.logger.warning(f"Error processing sale summary row: {str(e)}")
                        continue
                
                # If we still don't have a sale title, try to get it from page title or heading
                if not sale_details['sale_title']:
                    try:
                        # Try to find a main heading
                        possible_titles = self.driver.find_elements(By.CSS_SELECTOR, "h1, h2, .page-title, .sale-title")
                        if possible_titles:
                            sale_details['sale_title'] = possible_titles[0].text.strip()
                    except Exception:
                        pass
                
                # Fallback: try the original XPath div if no structured data found
                if not any([sale_details['sale_date'], sale_details['sale_occurs'], sale_details['sale_title']]):
                    try:
                        sale_info_div = self.driver.find_element(By.XPATH, '//*[@id="portlet_SaleDetailsPortlet_WAR_PWRWeb"]/div/div/div/div/div/div[2]/div[1]/div')
                        div_text = sale_info_div.text.strip()
                        self.logger.info(f"Fallback - Sale info div text: {div_text}")
                        
                        lines = [line.strip() for line in div_text.split('\n') if line.strip()]
                        if lines:
                            sale_details['sale_title'] = lines[0] if not sale_details['sale_title'] else sale_details['sale_title']
                            
                            for line in lines:
                                if any(keyword in line.lower() for keyword in ['date', 'starts', 'ends', 'oct', 'nov', 'dec', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep']) and not sale_details['sale_date']:
                                    sale_details['sale_date'] = line
                                elif any(keyword in line.lower() for keyword in ['location', 'address', 'occurs', 'venue', 'at:']) and not sale_details['sale_occurs']:
                                    sale_details['sale_occurs'] = line
                    
                    except NoSuchElementException:
                        self.logger.warning("Could not find the fallback sale info div either")
                
                self.logger.info(f"Final extracted sale details: {sale_details}")
                
            except Exception as e:
                self.logger.error(f"Error extracting structured sale info: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"Error extracting sale info details: {str(e)}")
        
        return sale_details
    
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



