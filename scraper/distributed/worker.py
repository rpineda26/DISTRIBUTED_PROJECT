#!/usr/bin/env python3
import json
import pika
import time
import urllib.parse
import requests
import re
import threading
import logging
import argparse
from bs4 import BeautifulSoup
from datetime import datetime

from scraper.utils.data_models import ContactInfo
from scraper.utils.logging_config import ColoredLogger, ColoredFormatter
from scraper.utils.selenium_utils import init_selenium_driver
from selenium.webdriver.common.by import By

class WorkerNode:
    def __init__(self, node_id, rabbitmq_host='localhost'):
        self.node_id = node_id
        self.rabbitmq_host = rabbitmq_host
        self.processed_faculty_urls = {} # Tracks URLs processed *by this worker*
        self.credentials = pika.PlainCredentials('rabbituser', 'rabbit1234') # Should be in environment

        # Set up logging
        self.logger = logging.getLogger(f"WorkerNode-{self.node_id}")
        self.logger.setLevel(logging.INFO)
        # Prevent adding handlers multiple times if logger is reused
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            # Use ColoredFormatter if available, otherwise basic Formatter
            formatter = ColoredFormatter() if 'ColoredFormatter' in globals() else logging.Formatter("%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Add custom log levels if ColoredLogger was intended
        # logging.addLevelName(logging.SUCCESS, 'SUCCESS') # Example

        # Initialize selenium driver (may return None if utils are missing)
        self.driver = init_selenium_driver() 
        if self.driver is None:
             self.logger.warning("Selenium driver not initialized. Profile scraping might fail.")

        # Thread safety for status updates
        self._status_connection = None
        self._status_channel = None
        self._status_lock = threading.Lock()
        
        # Flag to signal threads to stop
        self._stop_event = threading.Event()

    def _get_status_channel(self):
        """Safely gets or creates the status channel."""
        with self._status_lock:
            try:
                # Check if connection is valid
                if not self._status_connection or self._status_connection.is_closed:
                    self.logger.debug("Creating new status connection.")
                    self._status_connection = pika.BlockingConnection(
                        pika.ConnectionParameters(
                            host=self.rabbitmq_host,
                            port=5672,
                            credentials=self.credentials,
                            heartbeat=60, # Keep connection alive
                            blocked_connection_timeout=300
                        )
                    )
                    self._status_channel = self._status_connection.channel()
                    self._status_channel.queue_declare(queue='status_updates', durable=True)
                
                # Check if channel is valid
                if not self._status_channel or self._status_channel.is_closed:
                     self.logger.debug("Recreating status channel.")
                     self._status_channel = self._status_connection.channel()
                     # Re-declare queue just in case
                     self._status_channel.queue_declare(queue='status_updates', durable=True)

            except pika.exceptions.AMQPConnectionError as e:
                 self.logger.error(f"Status Connection Error: {e}. Will retry later.")
                 # Ensure cleanup happens
                 if self._status_connection and self._status_connection.is_open:
                      self._status_connection.close()
                 self._status_connection = None
                 self._status_channel = None
                 return None # Indicate failure to get channel
                 
            return self._status_channel

    def _close_status_connection(self):
        """Closes the status connection if open."""
        with self._status_lock:
            if self._status_connection and self._status_connection.is_open:
                self.logger.info("Closing status connection.")
                try:
                    self._status_connection.close()
                except Exception as e:
                    self.logger.warning(f"Error closing status connection: {e}")
            self._status_connection = None
            self._status_channel = None

    def _publish_message(self, queue_name, body_dict):
        """Helper to publish a persistent message to a specified queue."""
        conn = None
        try:
            # Create a short-lived connection just for publishing
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host, port=5672, credentials=self.credentials))
            ch = conn.channel()
            ch.queue_declare(queue=queue_name, durable=True) # Ensure queue exists
            ch.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(body_dict),
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
            self.logger.debug(f"Published message to {queue_name}: {body_dict.get('task_type', 'N/A')}")
        except pika.exceptions.AMQPConnectionError as e:
             self.logger.error(f"Connection Error publishing to {queue_name}: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Failed to publish message to {queue_name}: {e}", exc_info=True)
        finally:
            if conn and conn.is_open:
                conn.close()

    # --- Consumer Target Functions ---

    def _consume_tasks(self, queue_name, callback_method):
        """Generic consumer loop for a given queue and callback."""
        thread_name = threading.current_thread().name
        self.logger.info(f"{thread_name} starting...")
        
        while not self._stop_event.is_set():
            connection = None
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.rabbitmq_host,
                        port=5672,
                        credentials=self.credentials,
                        heartbeat=60,
                        blocked_connection_timeout=300
                    )
                )
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_qos(prefetch_count=1)
                self.logger.info(f"{thread_name} connected, consuming from '{queue_name}'.")

                # Consume messages one by one with a timeout
                for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout=1):
                    if self._stop_event.is_set():
                        break # Exit loop if stop event is set

                    if method_frame: # Check if a message was received
                        self.logger.debug(f"{thread_name} received task.")
                        try:
                            callback_method(channel, method_frame, properties, body)
                        except Exception as e:
                             self.logger.error(f"Error processing message in {thread_name}: {e}", exc_info=True)
                             # Decide whether to Nack or Ack based on error (Nack with requeue=False is often safer)
                             channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
                    
                    # If loop continues, connection is likely still healthy

            except pika.exceptions.StreamLostError:
                self.logger.warning(f"{thread_name} Stream lost. Reconnecting...")
            except pika.exceptions.AMQPConnectionError:
                self.logger.warning(f"{thread_name} Connection error. Reconnecting...")
            except Exception as e:
                self.logger.error(f"Unexpected error in {thread_name} consumer loop: {e}", exc_info=True)
            finally:
                if connection and connection.is_open:
                    connection.close()
                    self.logger.info(f"{thread_name} connection closed.")
            
            # Wait before retrying connection if not stopping
            if not self._stop_event.is_set():
                 self.logger.info(f"{thread_name} waiting 5 seconds before retry...")
                 time.sleep(5)
                 
        self.logger.info(f"{thread_name} stopped.")


    def start(self):
        """Start the worker node's consumer threads."""
        self.logger.info(f"Worker node {self.node_id} starting up")
        self._stop_event.clear() # Ensure stop event is not set initially

        program_thread = threading.Thread(target=self._consume_tasks, args=('program_tasks', self.process_program_task), name="ProgramConsumer")
        directory_thread = threading.Thread(target=self._consume_tasks, args=('directory_tasks', self.process_directory_task), name="DirectoryConsumer")
        profile_thread = threading.Thread(target=self._consume_tasks, args=('profile_tasks', self.process_profile_task), name="ProfileConsumer")

        threads = [program_thread, directory_thread, profile_thread]

        for t in threads:
            t.start()

        # Keep main thread alive to handle termination signals
        try:
            while any(t.is_alive() for t in threads):
                # Can add health checks or other monitoring here if needed
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received, initiating shutdown...")
        finally:
            self.logger.info("Signalling consumer threads to stop...")
            self._stop_event.set() # Signal threads to stop consuming

            self.logger.info("Waiting for consumer threads to finish...")
            for t in threads:
                t.join(timeout=10) # Wait for threads to finish cleanly
                if t.is_alive():
                     self.logger.warning(f"Thread {t.name} did not finish cleanly.")

            self.logger.info("Cleaning up resources...")
            self._close_status_connection() # Close the shared status connection
            
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("Selenium driver quit.")
                except Exception as e:
                     self.logger.warning(f"Error quitting selenium driver: {e}")
            
            self.logger.info(f"Worker node {self.node_id} finished.")

    # --- Task Processing Callbacks ---

    def send_status_update(self, job_id, stat_type, program_name=None):
        """Send status update to coordinator (thread-safe)."""
        update = {
            'job_id': job_id,
            'node_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'stat_type': stat_type
        }
        if program_name:
            update['program_name'] = program_name
            
        status_channel = self._get_status_channel()
        if status_channel:
            try:
                status_channel.basic_publish(
                    exchange='',
                    routing_key='status_updates',
                    body=json.dumps(update),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    )
                )
                self.logger.debug(f"Sent status update: {stat_type} for {program_name or job_id}")
            except Exception as e:
                self.logger.error(f"Failed to send status update via channel: {e}", exc_info=True)
                # Consider closing the connection here if publish fails repeatedly
                # self._close_status_connection() 
        else:
             self.logger.warning("Could not get status channel to send update.")

    def process_program_task(self, ch, method, properties, body):
        """Process a program task message."""
        task = json.loads(body)
        job_id = task.get('job_id', 'unknown_job')
        task_type = task.get('task_type', 'unknown_type')
        base_url = task.get('base_url')
        self.logger.info(f"Processing program task: {task_type} for job {job_id}")

        try:
            if task_type == 'get_college_program_urls':
                if not base_url:
                     raise ValueError("Missing 'base_url' in get_college_program_urls task")
                # This method now internally calls _publish_message for each program found
                self.get_college_program_urls(base_url, job_id)
                ch.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge task completion

            elif task_type == 'process_program_page':
                college_name = task.get('college_name')
                program_name = task.get('program_name')
                program_url = task.get('program_url')
                if not all([college_name, program_name, program_url, base_url]):
                     raise ValueError("Missing required fields in process_program_page task")

                self.send_status_update(job_id, "program", program_name)
                faculty_url = self.get_faculty_page(program_url, program_name, base_url)
                
                if faculty_url:
                    self.send_status_update(job_id, "faculty_url_success", program_name)
                    # Publish a new directory task using the helper
                    self._publish_message('directory_tasks', {
                        'job_id': job_id,
                        'task_type': 'process_directory_page',
                        'college_name': college_name,
                        'program_name': program_name,
                        'directory_url': faculty_url,
                        'base_url': base_url
                    })
                else:
                    self.send_status_update(job_id, "faculty_url_failure", program_name)
                    self.logger.warning(f"No faculty directory URL found for {program_name} at {program_url}")
                
                ch.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge task completion
            
            else:
                 self.logger.warning(f"Unknown program task type: {task_type}")
                 # Acknowledge unknown tasks to prevent requeue loops
                 ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.logger.error(f"Error processing program task (job {job_id}, type {task_type}): {e}", exc_info=True)
            # Nack without requeue to avoid poison messages
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def process_directory_task(self, ch, method, properties, body):
        """Process a directory task message."""
        task = json.loads(body)
        job_id = task.get('job_id', 'unknown_job')
        task_type = task.get('task_type', 'unknown_type')
        self.logger.info(f"Processing directory task: {task_type} for job {job_id}")

        try:
            if task_type == 'process_directory_page':
                college_name = task.get('college_name')
                program_name = task.get('program_name')
                directory_url = task.get('directory_url')
                base_url = task.get('base_url') # Make sure base_url is passed along
                if not all([college_name, program_name, directory_url, base_url]):
                     raise ValueError("Missing required fields in process_directory_page task")

                self.logger.info(f"Scraping directory page for {program_name}: {directory_url}")
                contacts = self.scrape_directory_page(directory_url, college_name, program_name, base_url) # Pass base_url
                
                if contacts:
                    self.logger.info(f"Found {len(contacts)} potential contacts for {program_name}. Publishing profile tasks.")
                    for contact in contacts:
                         # Publish a new profile task for each contact using the helper
                         self._publish_message('profile_tasks', {
                             'job_id': job_id,
                             'task_type': 'process_profile_page',
                             'contact': contact.__dict__, # Send contact data
                             'base_url': base_url # Pass base_url again if needed in profile scrape
                         })
                         self.send_status_update(job_id, "personnel_found", program_name)
                else:
                     self.logger.warning(f"No contacts found or extracted for {program_name} from {directory_url}")

                ch.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge task completion
            
            else:
                self.logger.warning(f"Unknown directory task type: {task_type}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.logger.error(f"Error processing directory task (job {job_id}, type {task_type}): {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def process_profile_task(self, ch, method, properties, body):
        """Process a profile task message."""
        task = json.loads(body)
        job_id = task.get('job_id', 'unknown_job')
        task_type = task.get('task_type', 'unknown_type')
        self.logger.info(f"Processing profile task: {task_type} for job {job_id}")
        
        try:
            if task_type == 'process_profile_page':
                contact_data = task.get('contact')
                # base_url = task.get('base_url') # Get base_url if needed
                if not contact_data:
                     raise ValueError("Missing 'contact' data in process_profile_page task")
                     
                # Recreate ContactInfo object from dict
                contact = ContactInfo(**contact_data) 
                
                if not contact.profile_url:
                     self.logger.warning(f"Skipping profile task for {contact.name} due to missing profile URL.")
                     ch.basic_ack(delivery_tag=method.delivery_tag)
                     return

                self.logger.info(f"Scraping profile page: {contact.profile_url}")
                updated_contact = self.scrape_profile_page(contact) # Scrape the page
                
                if updated_contact and updated_contact.email:
                    self.logger.info(f"Found email for {updated_contact.name}: {updated_contact.email}")
                    # Publish the result using the helper
                    self._publish_message('contact_results', updated_contact.__dict__)
                    self.send_status_update(job_id, "complete_record", updated_contact.department)
                else:
                    self.logger.warning(f"No email found for {contact.name} on profile page {contact.profile_url}")
                    self.send_status_update(job_id, "incomplete_record", contact.department)
                
                ch.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge task completion

            else:
                self.logger.warning(f"Unknown profile task type: {task_type}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.logger.error(f"Error processing profile task (job {job_id}, type {task_type}): {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


    # --- Scraping Logic Methods (Adapted from original, ensure they use self.logger) ---

    def get_college_program_urls(self, base_url, job_id):
        """Scrapes the main navigation to get college and program URLs and publishes tasks."""
        try:
            self.logger.info(f"Scraping college/program URLs from {base_url}")
            response = requests.get(base_url, timeout=15)
            response.raise_for_status() # Raise exception for bad status codes
            soup = BeautifulSoup(response.text, 'html.parser')
            
            main_menu = soup.find('ul', class_='nav navbar-nav menu-main-menu') 
            if not main_menu: raise ValueError("Main menu ('nav navbar-nav menu-main-menu') not found")
            
            # Navigation logic might be fragile - adjust indices/selectors if site structure changes
            academics_li_candidates = main_menu.find_all('li', recursive=False)
            if len(academics_li_candidates) < 3: raise ValueError("Not enough items in main menu for 'Academics'")
            academics_li = academics_li_candidates[2] 
            if not academics_li.find('a', string=re.compile(r'Academics', re.I)): # Case-insensitive search
                 raise ValueError("'Academics' link not found in expected position")

            academics_menu = academics_li.find('ul', recursive=False)
            if not academics_menu: raise ValueError("Academics submenu not found")
            
            colleges_li_candidates = academics_menu.find_all('li', recursive=False)
            if not colleges_li_candidates: raise ValueError("No items found in Academics submenu for 'Colleges'")
            colleges_li = colleges_li_candidates[0]
            if not colleges_li.find('a', string=re.compile(r'Colleges', re.I)):
                 raise ValueError("'Colleges' link not found in expected position")

            colleges_menu = colleges_li.find('ul', recursive=False)
            if not colleges_menu: raise ValueError("Colleges submenu not found")
            
            program_count = 0
            college_count = 0
            for college_li in colleges_menu.find_all('li', recursive=False):
                college_link = college_li.find('a', recursive=False)
                if not college_link: continue
                college_name = college_link.text.strip()
                self.logger.info(f"Processing College: {college_name}")
                college_count += 1
                self.send_status_update(job_id, "college")

                program_menu = college_li.find('ul', recursive=False)
                if program_menu:
                    for program_li in program_menu.find_all('li', recursive=False):
                        program_link = program_li.find('a')
                        if program_link and program_link.get('href'):
                            program_url_relative = program_link['href']
                            program_name = program_link.text.strip()
                            program_url_absolute = urllib.parse.urljoin(base_url, program_url_relative)
                            
                            self.logger.debug(f"Found program: {program_name} -> {program_url_absolute}")
                            # Publish task for this program page
                            self._publish_message('program_tasks', {
                                'job_id': job_id,
                                'task_type': 'process_program_page',
                                'college_name': college_name,
                                'program_name': program_name,
                                'program_url': program_url_absolute,
                                'base_url': base_url
                            })
                            program_count += 1

            self.logger.info(f"Found {college_count} colleges and initiated {program_count} program page tasks.")
            
        except requests.RequestException as e:
             self.logger.error(f"HTTP Error getting college/program URLs from {base_url}: {e}")
        except Exception as e:
            self.logger.error(f"Error parsing college/program URLs: {e}", exc_info=True)

    def get_faculty_page(self, program_url, program_name, base_url):
        """Scrapes a program page to find the faculty directory link."""
        try:
            self.logger.debug(f"Searching for faculty link on: {program_url}")
            response = requests.get(program_url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for links containing 'faculty' - make search more robust
            faculty_links = soup.find_all('a', href=True, string=re.compile(r'faculty|staff|directory|profile', re.I))
            
            potential_urls = set()
            for link in faculty_links:
                 link_text = link.text.lower()
                 href = link['href']
                 # Prioritize links with specific keywords
                 if 'faculty profile' in link_text or 'faculty directory' in link_text:
                      url = urllib.parse.urljoin(base_url, href)
                      potential_urls.add(url)
                 # Consider other links containing 'faculty' if specific ones aren't found
                 elif 'faculty' in link_text:
                      url = urllib.parse.urljoin(base_url, href)
                      potential_urls.add(url)

            if not potential_urls:
                self.logger.warning(f"No potential faculty links found for {program_name} on {program_url}")
                return None

            # Basic check against processed URLs (can be improved with distributed locking/cache if needed)
            for url in potential_urls:
                # Simple check: has this worker processed this URL *at all*?
                # A more robust check might involve checking if processed for *this specific program* if URLs overlap
                if url not in self.processed_faculty_urls:
                    self.processed_faculty_urls[url] = {program_name} # Mark as processed for this program
                    self.logger.info(f"Found faculty directory link for {program_name}: {url}")
                    return url
                elif program_name not in self.processed_faculty_urls.get(url, set()):
                     # URL seen before, but not for this program - process it
                     self.processed_faculty_urls.setdefault(url, set()).add(program_name)
                     self.logger.info(f"Found faculty directory link (previously seen for other program) for {program_name}: {url}")
                     return url
                 
            self.logger.warning(f"All potential faculty links for {program_name} seem to be processed already: {potential_urls}")
            return None # No suitable, unprocessed link found
            
        except requests.RequestException as e:
             self.logger.error(f"HTTP Error getting faculty page {program_url}: {e}")
             return None
        except Exception as e:
            self.logger.error(f"Error parsing faculty page link in {program_url}: {e}", exc_info=True)
            return None

    def scrape_directory_page(self, url, college_name, program_name, base_url):
        """Scrapes the faculty directory page and returns ContactInfo objects."""
        contacts = []
        try:
            self.logger.debug(f"Scraping directory: {url}")
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            processed_profile_urls_for_page = set() # Track profiles found on *this specific page load*

            # Adapt selectors based on observed site structure (these might need adjustment)
            # Try common patterns first
            # Pattern 1: Specific column class often used in WPBakery/Visual Composer
            faculty_elements = soup.find_all('div', class_=lambda c: c and 'vc_col-sm-4' in c and 'wpb_column' in c)

            # Pattern 2: Another common WPBakery structure
            if not faculty_elements:
                 faculty_elements = soup.find_all('div', class_=["wpb_text_column", "wpb_content_element"])

            # Pattern 3: General paragraph or div containing a link (less precise)
            if not faculty_elements:
                 # Look for paragraphs or divs that directly contain a link with text (likely name)
                 faculty_elements = soup.find_all(lambda tag: (tag.name == 'p' or tag.name == 'div') and tag.find('a', href=True, string=True))

            if not faculty_elements:
                self.logger.warning(f"No faculty member elements found using common patterns on {url} for {program_name}.")
                # Add more specific selectors if needed based on inspection
                # E.g., CCS specific layout if Pattern 1/2 didn't work:
                computer_programs_ids = {"CT", "IT", "ST", "CS"} # Add relevant IDs
                start_div = soup.find('div', id=lambda i: i in computer_programs_ids)
                if start_div:
                     self.logger.debug("Trying CCS-specific layout detection...")
                     # Complex logic from original attempt might go here if needed
                     # Be careful with find_next_sibling loops - ensure they terminate
                     pass # Add CCS logic if the general patterns fail


            if not faculty_elements:
                 self.logger.error(f"Could not find any faculty elements on directory page: {url}")
                 return []

            self.logger.debug(f"Found {len(faculty_elements)} potential faculty elements on {url}")

            for element in faculty_elements:
                profile_link = element.find('a', href=True, string=True) # Ensure link has text
                
                if profile_link:
                    full_name = profile_link.text.strip()
                    relative_profile_url = profile_link.get('href', '').strip()
                    
                    # Basic sanity checks
                    if not full_name or not relative_profile_url or relative_profile_url == '#':
                        continue
                    # Avoid generic links often present
                    if 'faculty profile' in full_name.lower() or 'directory' in full_name.lower():
                        continue

                    absolute_profile_url = urllib.parse.urljoin(base_url, relative_profile_url)

                    # Check if we already extracted this profile URL *from this page load*
                    if absolute_profile_url in processed_profile_urls_for_page:
                         continue
                    processed_profile_urls_for_page.add(absolute_profile_url)

                    # Check against worker's global processed list (simple check)
                    # This helps if the same person appears under multiple programs processed by the *same worker*
                    if absolute_profile_url in self.processed_faculty_urls and program_name in self.processed_faculty_urls[absolute_profile_url]:
                         self.logger.debug(f"Skipping already processed profile {absolute_profile_url} for program {program_name}")
                         continue
                    
                    # Mark as processed by this worker for this program
                    self.processed_faculty_urls.setdefault(absolute_profile_url, set()).add(program_name)

                    contact = ContactInfo(
                        name=full_name,
                        office=college_name,
                        department=program_name,
                        profile_url=absolute_profile_url
                    )
                    contacts.append(contact)
                    self.logger.debug(f"Extracted potential contact: {full_name} -> {absolute_profile_url}")

            if not contacts:
                self.logger.warning(f"Extracted 0 contacts from {len(faculty_elements)} elements on {url}")
            
            return contacts

        except requests.RequestException as e:
            self.logger.error(f"HTTP Error scraping directory page {url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing directory page {url} for {program_name}: {e}", exc_info=True)
            return []
            
    def scrape_profile_page(self, contact):
        """Scrapes an individual profile page to get the email (using Selenium)."""
        if not self.driver:
            self.logger.error("Selenium driver not available, cannot scrape profile page.")
            return contact # Return unmodified contact

        try:
            self.logger.debug(f"Navigating to profile page: {contact.profile_url}")
            self.driver.get(contact.profile_url)
            # Wait for dynamic content if necessary - adjust time as needed
            time.sleep(2) # Simple wait, consider explicit waits for elements if page load is slow/dynamic
            
            # Get page source or text
            # Using page text is often more robust against obfuscation than inspecting source
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Use regex to find emails (improved pattern)
            # Allows for domains with hyphens, common TLDs, and university .edu.xx extensions
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?'
            emails = re.findall(email_pattern, page_text)

            found_email = None
            if emails:
                 # Filter out common example/placeholder emails and potential image filenames
                 potential_emails = [e for e in emails if not e.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')) and '@example.' not in e.lower()]
                 
                 # Prioritize emails containing parts of the name or known domain (e.g., dlsu.edu.ph)
                 name_parts = contact.name.lower().split()
                 for email in potential_emails:
                      if 'dlsu.edu.ph' in email.lower(): # Prioritize official domain
                           found_email = email
                           break
                      # Simple check if parts of the name are in the email username
                      if any(part in email.lower().split('@')[0] for part in name_parts if len(part) > 2):
                          found_email = email
                          break
                 
                 # If no priority email found, take the first plausible one
                 if not found_email and potential_emails:
                      found_email = potential_emails[0]

            if found_email:
                contact.email = found_email.strip() # Store the found email
                self.logger.debug(f"Found email for {contact.name}: {contact.email}")
            else:
                 self.logger.warning(f"Could not find a plausible email for {contact.name} on {contact.profile_url}")
                 # Log the emails found for debugging if needed:
                 # self.logger.debug(f"Emails found by regex: {emails}")

            return contact

        except Exception as e:
            # Catch broader exceptions including Selenium errors (TimeoutException, WebDriverException, etc.)
            self.logger.error(f"Error scraping profile page {contact.profile_url}: {e}", exc_info=True)
            return contact # Return contact potentially without email


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description='Distributed Web Scraper Worker Node')
    parser.add_argument('--node-id', required=True, help='Unique identifier for this worker node')
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host IP or hostname')
    
    args = parser.parse_args()
    
    # Basic logging setup for the main script execution before worker starts
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MainThread] - %(message)s')
    
    worker = WorkerNode(args.node_id, args.rabbitmq_host)
    worker.start() # This now blocks until interrupted or threads finish

if __name__ == '__main__':
    main()