"""
Core Playwright browser module for Pickles website automation.
Replaces all Selenium/ChromeDriver usage with Playwright (sync API).
"""

import time
import re
import json
from typing import Optional, List, Dict, Callable
from pathlib import Path
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright

# Try to import stealth - optional dependency
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False


def _get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def _load_config() -> dict:
    """Load config.yaml from config/ directory."""
    import yaml
    config_path = _get_project_root() / "config" / "config.yaml"
    if not config_path.exists():
        # Fallback to root
        config_path = _get_project_root() / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


class PicklesBrowser:
    """
    Playwright-based browser automation for Pickles website.
    Replaces PicklesScraper (Selenium-based).
    
    Usage:
        with PicklesBrowser() as browser:
            browser.login(username, password)
            browser.page.goto("https://...")
    """

    def __init__(self, headless: Optional[bool] = None, wait_timeout: Optional[int] = None,
                 stealth: Optional[bool] = None, enable_network_logging: bool = False):
        config = _load_config()
        scraper_cfg = config.get('scraper', {})

        self.headless = headless if headless is not None else scraper_cfg.get('headless', True)
        self.wait_timeout = (wait_timeout if wait_timeout is not None
                            else scraper_cfg.get('wait_timeout', 15)) * 1000  # Playwright uses ms
        self.use_stealth = stealth if stealth is not None else scraper_cfg.get('stealth', True)
        self.browser_type = scraper_cfg.get('browser', 'chromium')
        self.enable_network_logging = enable_network_logging

        self.base_url = "https://www.pickles.com.au/"

        # Login selectors
        self.login_button_xpath = '//*[@id="pickles-login-widget-space"]/ul/li[2]/a'
        self.username_input_id = 'pickles-sign-in-username'
        self.password_input_id = 'pickles-sign-in-password'
        self.login_submit_xpath = '/html/body/form/div[4]/div[1]/div/div[2]/div/div/div[2]/button'

        # Playwright objects
        self._playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        # Network logging storage
        self._captured_requests: List[Dict] = []
        self._captured_responses: List[Dict] = []
        self._response_handlers: List[Callable] = []

        # Logger
        import sys, os
        sys.path.insert(0, str(_get_project_root()))
        sys.path.insert(0, str(_get_project_root() / "utils"))
        from utils.logger import get_logger
        self.logger = get_logger("pickles_browser", log_to_file=True)

    def setup(self) -> None:
        """Launch browser and create page."""
        self._playwright = sync_playwright().start()

        launcher = getattr(self._playwright, self.browser_type)
        self.browser = launcher.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )

        self.context = self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        self.context.set_default_timeout(self.wait_timeout)

        self.page = self.context.new_page()

        # Apply stealth
        if self.use_stealth and STEALTH_AVAILABLE:
            stealth_sync(self.page)
            self.logger.info("Playwright stealth applied")
        elif self.use_stealth and not STEALTH_AVAILABLE:
            self.logger.warning("playwright-stealth not installed, skipping stealth mode")

        # Setup network logging if enabled
        if self.enable_network_logging:
            self._setup_network_logging()

        self.logger.info(f"Playwright browser launched (headless={self.headless}, type={self.browser_type})")

    def _setup_network_logging(self) -> None:
        """Setup network request/response capturing."""
        def on_request(request):
            self._captured_requests.append({
                'url': request.url,
                'method': request.method,
                'headers': dict(request.headers),
                'post_data': request.post_data,
                'timestamp': time.time()
            })

        def on_response(response):
            entry = {
                'url': response.url,
                'status': response.status,
                'method': response.request.method,
                'headers': dict(response.headers),
                'timestamp': time.time()
            }
            self._captured_responses.append(entry)
            # Call any registered handlers
            for handler in self._response_handlers:
                try:
                    handler(response)
                except Exception:
                    pass

        self.page.on("request", on_request)
        self.page.on("response", on_response)

    def on_response(self, handler: Callable) -> None:
        """Register a response handler for network interception."""
        self._response_handlers.append(handler)

    def get_captured_requests(self, method: str = None, url_pattern: str = None) -> List[Dict]:
        """Get captured requests, optionally filtered."""
        results = self._captured_requests
        if method:
            results = [r for r in results if r['method'] == method]
        if url_pattern:
            results = [r for r in results if url_pattern in r['url']]
        return results

    def get_captured_post_requests(self, url_pattern: str = None) -> List[Dict]:
        """Get captured POST requests, optionally filtered by URL pattern."""
        return self.get_captured_requests(method='POST', url_pattern=url_pattern)

    def clear_captured_requests(self) -> None:
        """Clear all captured network logs."""
        self._captured_requests.clear()
        self._captured_responses.clear()

    def get_cookies_dict(self) -> dict:
        """Get cookies as a simple name:value dictionary for use with requests library."""
        cookies = self.context.cookies()
        return {c['name']: c['value'] for c in cookies}

    def get_cookies(self) -> list:
        """Get all cookies as a list of dicts."""
        return self.context.cookies()

    def navigate_to_site(self) -> None:
        """Navigate to Pickles homepage."""
        self.logger.info(f"Navigating to {self.base_url}")
        self.page.goto(self.base_url, wait_until="domcontentloaded")
        self.page.wait_for_selector("body")
        self.logger.info("Successfully navigated to Pickles website")

    def click_login_button(self) -> None:
        """Click the login button on the main page."""
        self.logger.info("Looking for login button...")
        login_btn = self.page.locator(f'xpath={self.login_button_xpath}')
        login_btn.scroll_into_view_if_needed()
        time.sleep(1)
        login_btn.click()
        self.logger.info("Login button clicked successfully")

    def wait_for_login_form(self) -> None:
        """Wait for login form to appear."""
        self.logger.info("Waiting for login form...")
        self.page.wait_for_selector(f'#{self.username_input_id}')
        self.page.wait_for_selector(f'#{self.password_input_id}')
        self.logger.info("Login form appeared")

    def enter_credentials(self, username: str, password: str) -> None:
        """Fill in login credentials."""
        self.logger.info("Entering credentials...")
        username_field = self.page.locator(f'#{self.username_input_id}')
        username_field.fill("")
        username_field.fill(username)

        password_field = self.page.locator(f'#{self.password_input_id}')
        password_field.fill("")
        password_field.fill(password)
        self.logger.info("Credentials entered")

    def submit_login(self) -> None:
        """Submit the login form."""
        self.logger.info("Submitting login form...")
        submit_btn = self.page.locator(f'xpath={self.login_submit_xpath}')
        submit_btn.click()
        time.sleep(2)
        self.logger.info("Login form submitted")

    def login(self, username: str, password: str) -> bool:
        """Complete login flow."""
        try:
            self.navigate_to_site()
            self.click_login_button()
            self.wait_for_login_form()
            self.enter_credentials(username, password)
            self.submit_login()
            time.sleep(3)

            # Check if login succeeded (login button should be gone)
            login_btn = self.page.locator(f'xpath={self.login_button_xpath}')
            if login_btn.count() > 0:
                self.logger.warning("Login may have failed - login button still visible")
                return False

            self.logger.info("Login successful")
            return True
        except Exception as e:
            self.logger.error(f"Login failed: {str(e)}")
            return False

    def navigate_to_auction_page(self, auction_url: str) -> bool:
        """Navigate to an auction listing page."""
        try:
            self.page.goto(auction_url, wait_until="domcontentloaded")
            self.page.wait_for_selector(".auctions-list_auctions-list__XP23h", timeout=self.wait_timeout)
            return True
        except Exception:
            return False

    def extract_auction_details(self) -> list:
        """Extract auction card details from current page."""
        auctions = []
        try:
            articles = self.page.locator("article[data-testid='auction-card']").all()
            for article in articles:
                try:
                    auction_info = {}
                    title_el = article.locator(".auction-card_acb-title__Etjpm")
                    auction_info['title'] = title_el.text_content().strip() if title_el.count() > 0 else "N/A"

                    loc_elements = article.locator(".ac-location-status_value__PLM4O").all()
                    auction_info['location'] = loc_elements[0].text_content().strip() if loc_elements else "N/A"
                    auction_info['status'] = loc_elements[1].text_content().strip() if len(loc_elements) > 1 else "N/A"

                    try:
                        footer = article.locator("footer[data-testid='ac-footer']")
                        sale_info_link = footer.locator("a:has(span:text('Sale Info'))").first
                        auction_info['sale_info_url'] = sale_info_link.get_attribute('href') if sale_info_link.count() > 0 else None
                    except Exception:
                        auction_info['sale_info_url'] = None

                    auctions.append(auction_info)
                except Exception:
                    continue
        except Exception:
            pass
        return auctions

    def navigate_to_sale_info(self, sale_info_url: str) -> bool:
        """Navigate to a sale info page."""
        try:
            self.page.goto(sale_info_url, wait_until="domcontentloaded")
            time.sleep(3)
            return True
        except Exception:
            return False

    def extract_sale_info_details(self) -> dict:
        """Extract sale info details from current page."""
        sale_details = {
            'auction_registration': None,
            'sale_title': None,
            'sale_date': None,
            'sale_occurs': None
        }
        try:
            bid_live = self.page.locator("#bidLiveButton")
            if bid_live.count() > 0:
                sale_details['auction_registration'] = bid_live.get_attribute('href')
        except Exception:
            pass

        try:
            rows = self.page.locator("div.sale-summary-row").all()
            for row in rows:
                p_elements = row.locator("p").all()
                if len(p_elements) >= 2:
                    label = p_elements[0].text_content().strip().lower().replace(':', '')
                    value = p_elements[1].text_content().strip()
                    if 'date' in label:
                        sale_details['sale_date'] = value
                    elif any(k in label for k in ['location', 'address', 'venue', 'occurs']):
                        sale_details['sale_occurs'] = value
                    elif any(k in label for k in ['title', 'sale', 'auction']):
                        if not sale_details['sale_title']:
                            sale_details['sale_title'] = value
        except Exception:
            pass

        return sale_details

    def open_new_tab(self, url: str = None) -> Page:
        """Open a new tab and optionally navigate to URL. Returns the new page."""
        new_page = self.context.new_page()
        if self.use_stealth and STEALTH_AVAILABLE:
            stealth_sync(new_page)
        if url:
            new_page.goto(url, wait_until="domcontentloaded")
        return new_page

    def get_page_json(self, url: str) -> Optional[dict]:
        """Navigate to URL and extract JSON from page content."""
        try:
            new_page = self.open_new_tab(url)
            time.sleep(2)
            content = new_page.content()
            new_page.close()

            # Try to extract JSON
            json_match = re.search(r'<pre[^>]*>(.*?)</pre>', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1).strip())

            # Try raw JSON
            json_match = re.search(r'\{.*\}|\[.*\]', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))

            return None
        except Exception as e:
            self.logger.error(f"Error getting page JSON from {url}: {e}")
            return None

    def take_screenshot(self, filename: str = "screenshot.png") -> bool:
        """Take a screenshot of the current page."""
        try:
            self.page.screenshot(path=filename)
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close browser and cleanup."""
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self._playwright:
                self._playwright.stop()
        except Exception:
            pass
        finally:
            self.page = None
            self.context = None
            self.browser = None
            self._playwright = None

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Backward-compatible alias
PicklesScraper = PicklesBrowser
