from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def init_selenium_driver():
    """
    Initialize a selenium driver. Each thread for scraping profile pages will have its own driver.
    The email data contained in the profile page is protected with obfuscation and requires 
    JavaScript to render. Scraping these pages through Selenium will allow us to access 
    the email data after the page has fully loaded.
    """
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3") 
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver