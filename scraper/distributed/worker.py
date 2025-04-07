#!/usr/bin/env python3
import json
import pika
import time
import urllib.parse
import requests
import re
import threading
import logging
from bs4 import BeautifulSoup
from datetime import datetime

from scraper.utils.data_models import ContactInfo
from scraper.utils.logging_config import ColoredLogger, ColoredFormatter
from scraper.utils.selenium_utils import init_selenium_driver
from selenium.webdriver.common.by import By

class WorkerNode:
    def __init__(self, node_id, rabbitmq_host='localhost'):
        self.node_id = node_id
        self.processed_faculty_urls = {}
        self.credentials = pika.PlainCredentials('rabbituser', 'rabbit1234') #should be in environment but whatever
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=5672, credentials=self.credentials))
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(ColoredFormatter())
            self.logger.addHandler(handler)
            
        # Initialize selenium driver
        self.driver = init_selenium_driver()
        
        # Set up channels and consumers
        self.setup_channels()
        
    def setup_channels(self):
        """Set up channels and queue consumers"""
        # Program tasks channel
        self.program_channel = self.connection.channel()
        self.program_channel.queue_declare(queue='program_tasks', durable=True)
        self.program_channel.basic_qos(prefetch_count=1)
        self.program_channel.basic_consume(
            queue='program_tasks',
            on_message_callback=self.process_program_task
        )
        
        # Directory tasks channel
        self.directory_channel = self.connection.channel()
        self.directory_channel.queue_declare(queue='directory_tasks', durable=True)
        self.directory_channel.basic_qos(prefetch_count=1)
        self.directory_channel.basic_consume(
            queue='directory_tasks',
            on_message_callback=self.process_directory_task
        )
        
        # Profile tasks channel
        self.profile_channel = self.connection.channel()
        self.profile_channel.queue_declare(queue='profile_tasks', durable=True)
        self.profile_channel.basic_qos(prefetch_count=1)
        self.profile_channel.basic_consume(
            queue='profile_tasks',
            on_message_callback=self.process_profile_task
        )
        
        # Results channel
        self.results_channel = self.connection.channel()
        self.results_channel.queue_declare(queue='contact_results', durable=True)
        
        # Status channel
        self.status_channel = self.connection.channel()
        self.status_channel.queue_declare(queue='status_updates', durable=True)
        
    def start(self):
        """Start the worker node"""
        self.logger.info(f"Worker node {self.node_id} starting up")
        
        # Create threads to consume from different queues
        program_thread = threading.Thread(target=self.program_channel.start_consuming)
        directory_thread = threading.Thread(target=self.directory_channel.start_consuming)
        profile_thread = threading.Thread(target=self.profile_channel.start_consuming)
        
        # Start threads
        program_thread.start()
        directory_thread.start()
        profile_thread.start()
        
        # Wait for threads to complete
        try:
            while any(t.is_alive() for t in [program_thread, directory_thread, profile_thread]):
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received interrupt, shutting down...")
            
        # Clean up
        if self.connection.is_open:
            self.connection.close()
        
        if self.driver:
            self.driver.quit()
            
    def send_status_update(self, job_id, stat_type, program_name=None):
        """Send status update to coordinator"""
        update = {
            'job_id': job_id,
            'node_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'stat_type': stat_type
        }
        
        if program_name:
            update['program_name'] = program_name
            
        self.status_channel.basic_publish(
            exchange='',
            routing_key='status_updates',
            body=json.dumps(update),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
    
    def process_program_task(self, ch, method, properties, body):
        """Process a program task"""
        try:
            task = json.loads(body)
            job_id = task['job_id']
            task_type = task['task_type']
            base_url = task['base_url']
            
            self.logger.info(f"Processing program task: {task_type}")
            
            if task_type == 'get_college_program_urls':
                college_programs = self.get_college_program_urls(base_url, job_id)
                
                # Task completed successfully
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            elif task_type == 'process_program_page':
                college_name = task['college_name']
                program_name = task['program_name']
                program_url = task['program_url']
                
                self.send_status_update(job_id, "program", program_name)
                
                # Process program page to find faculty directory
                faculty_url = self.get_faculty_page(program_url, program_name, base_url)
                if faculty_url:
                    self.send_status_update(job_id, "faculty_url_success", program_name)
                    
                    # Create directory task
                    self.directory_channel.basic_publish(
                        exchange='',
                        routing_key='directory_tasks',
                        body=json.dumps({
                            'job_id': job_id,
                            'task_type': 'process_directory_page',
                            'college_name': college_name,
                            'program_name': program_name,
                            'directory_url': faculty_url,
                            'base_url': base_url
                        }),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                        )
                    )
                else:
                    self.send_status_update(job_id, "faculty_url_failure", program_name)
                
                # Task completed
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            self.logger.error(f"Error processing program task: {str(e)}")
            # Requeue the task in case of failure
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def process_directory_task(self, ch, method, properties, body):
        """Process a directory task"""
        try:
            task = json.loads(body)
            job_id = task['job_id']
            task_type = task['task_type']
            
            if task_type == 'process_directory_page':
                college_name = task['college_name']
                program_name = task['program_name']
                directory_url = task['directory_url']
                base_url = task['base_url']
                
                self.logger.info(f"Processing directory page for {program_name}")
                
                # Scrape directory page
                contacts = self.scrape_directory_page(directory_url, college_name, program_name)
                
                if contacts:
                    self.logger.success(f"Found {len(contacts)} contacts for {program_name}")
                    
                    # Create profile tasks
                    for contact in contacts:
                        self.profile_channel.basic_publish(
                            exchange='',
                            routing_key='profile_tasks',
                            body=json.dumps({
                                'job_id': job_id,
                                'task_type': 'process_profile_page',
                                'contact': contact.__dict__,
                                'base_url': base_url
                            }),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # make message persistent
                            )
                        )
                        
                        self.send_status_update(job_id, "personnel_found", program_name)
                
                # Task completed
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            self.logger.error(f"Error processing directory task: {str(e)}")
            # Requeue the task in case of failure
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def process_profile_task(self, ch, method, properties, body):
        """Process a profile task"""
        try:
            task = json.loads(body)
            job_id = task['job_id']
            task_type = task['task_type']
            
            if task_type == 'process_profile_page':
                contact_data = task['contact']
                contact = ContactInfo(**contact_data)
                
                self.logger.info(f"Processing profile page: {contact.profile_url}")
                
                # Scrape profile page
                updated_contact = self.scrape_profile_page(contact)
                
                if updated_contact.email:
                    # Send result back to coordinator
                    self.results_channel.basic_publish(
                        exchange='',
                        routing_key='contact_results',
                        body=json.dumps(updated_contact.__dict__),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                        )
                    )
                    
                    self.send_status_update(job_id, "complete_record", contact.department)
                    self.logger.success(f"Found email: {updated_contact.email}")
                else:
                    self.send_status_update(job_id, "incomplete_record", contact.department)
                
                # Task completed
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            self.logger.error(f"Error processing profile task: {str(e)}")
            # Requeue the task in case of failure
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    # Worker methods (adapted from your original code)
    
    def get_college_program_urls(self, base_url, job_id):
        """
        Scrapes the main navigation to get college and program URLs
        """
        try:
            self.logger.info(f"Scraping college/program URLs from {base_url}")
            response = requests.get(base_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the colleges dropdown menu
            main_menu = soup.find('ul', class_='nav navbar-nav menu-main-menu') 
            if not main_menu:
                raise ValueError("Main menu not found")
            academics_li = main_menu.find_all('li', recursive=False)[2]  #Academics is the third option in the main menu
            if not academics_li or not academics_li.find('a', string='Academics'):
                raise ValueError("Academics menu not found")
            academics_menu = academics_li.find('ul', recursive=False)
            if not academics_menu:
                raise ValueError("Academics menu not found")
            colleges_li = academics_menu.find_all('li', recursive=False)[0]  #Colleges is the first option in the academics menu
            if not colleges_li or not colleges_li.find('a', string='Colleges'):
                raise ValueError("Colleges menu not found")
            colleges_menu = colleges_li.find('ul', recursive=False)
            if not colleges_menu:
                raise ValueError("Colleges menu not found")
            
            college_programs = {}
            for college_li in colleges_menu.find_all('li', recursive=False):
                program_menu = college_li.find('ul', recursive=False)
                college_name = college_li.find('a').text.strip()
                program_urls = []
                
                if program_menu:
                    for program_li in program_menu.find_all('li', recursive=False):
                        program_url = program_li.find('a')['href']
                        if program_url:
                            program_name = program_li.find('a').text.strip()
                            program_url = urllib.parse.urljoin(base_url, program_url)
                            
                            # Create program task
                            self.program_channel.basic_publish(
                                exchange='',
                                routing_key='program_tasks',
                                body=json.dumps({
                                    'job_id': job_id,
                                    'task_type': 'process_program_page',
                                    'college_name': college_name,
                                    'program_name': program_name,
                                    'program_url': program_url,
                                    'base_url': base_url
                                }),
                                properties=pika.BasicProperties(
                                    delivery_mode=2,  # make message persistent
                                )
                            )
                            
                            program_urls.append(program_url)
                    
                    college_programs[college_name] = program_urls
                    self.send_status_update(job_id, "college")

            total_urls = sum(len(programs) for programs in college_programs.values())
            self.logger.success(f"Found {total_urls} programs to process")
            return college_programs
            
        except Exception as e:
            self.logger.error(f"Error getting college/program URLs: {str(e)}")
            return {}
    
    def get_faculty_page(self, program_url, program_name, base_url):
        """Scrapes a program page to find the faculty directory link"""
        try:
            response = requests.get(program_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            faculty_links = soup.find_all('a', href=lambda href: href and 'faculty' in href.lower())
            if not faculty_links:
                raise ValueError("No faculty links found!")
            
            for link in faculty_links:
                if 'faculty profile' in link.text.lower():
                    url = urllib.parse.urljoin(base_url, link['href'])
                    
                    # In distributed system, we track processed URLs differently
                    # Each node maintains its own cache of processed URLs
                    if url in self.processed_faculty_urls:
                        if program_name not in self.processed_faculty_urls[url]:
                            self.processed_faculty_urls[url].add(program_name)
                            return url
                        else:
                            self.logger.info(f"Skipping duplicate faculty URL for {program_name}: {url}")
                            return None
                    else:
                        self.processed_faculty_urls[url] = {program_name}
                        return url
                    
            raise ValueError("No working faculty links found!")
            
        except Exception as e:
            self.logger.error(f"Error finding faculty page in {program_url}: {str(e)}")
            return None
    
    def scrape_directory_page(self, url, college_name, program_name):
        """Scrapes the directory/list page and returns profile URLs"""
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            contacts = []
            worker_elements = []
            
            # Layout 1: Computer Programs with specific div IDs
            computer_programs = {
                "Computer Technology": "CT",
                "Information Technology": "IT",
                "Software Technology": "ST"
            }
            
            if program_name in computer_programs:
                program_id = computer_programs[program_name]
                start_div = soup.find('div', id=program_id)

                if start_div:
                    all_faculty_elements = []
                    current = start_div.find_next_sibling()
                    
                    while current:
                        if current.get('id') in computer_programs.values():
                            break
                            
                        if (current.name == 'div' and 
                            isinstance(current.get('class'), list) and 
                            'vc_row' in current.get('class') and 
                            'wpb_row' in current.get('class') and 
                            'vc_row-fluid' in current.get('class')):
                            
                            faculty_in_container = current.find_all(
                                'div',
                                class_=['wpb_column', 'vc_column_container', 'vc_col-sm-4']
                            )
                            all_faculty_elements.extend(faculty_in_container)
                        
                        current = current.find_next_sibling()
                    
                    worker_elements = all_faculty_elements

            # Layout 2: Direct class-based layout
            if not worker_elements:
                worker_elements = soup.find_all('div', class_=['wpb_column', 'vc_column_container', 'vc_col-sm-4'])

            # Layout 3: Alternative layout with div main
            if not worker_elements:
                worker_elements = soup.find_all('div', class_=["wpb_text_column", "wpb_content_element"])
       
            if not worker_elements:
                raise ValueError(f"No faculty members found for {program_name} in any layout!")

            # Process found elements
            for element in worker_elements:
                profile_link = element.find('a')
                if profile_link and profile_link.text.strip() != 'Faculty Profiles' and profile_link.text.strip() is not None:
                    profile_url = profile_link.get('href', '').strip()
                    full_name = profile_link.text.strip()
                    
                    if not full_name or not profile_url:
                        continue

                    # Check if we should process this profile for this program
                    if profile_url in self.processed_faculty_urls:
                        if program_name in self.processed_faculty_urls[profile_url]:
                            continue  # Skip if already processed for this program
                        else:
                            self.processed_faculty_urls[profile_url].add(program_name)
                    else:
                        self.processed_faculty_urls[profile_url] = {program_name}

                    contact = ContactInfo(
                        name=full_name,
                        office=college_name,
                        department=program_name,
                        profile_url=profile_url
                    )
                    contacts.append(contact)

            if not contacts:
                raise ValueError(f"No contacts found for {program_name}!")
            
            self.logger.success(f"Found {len(contacts)} contacts for {program_name}")
            return contacts

        except Exception as e:
            self.logger.error(f"Error parsing directory page {url} for {program_name}: {str(e)}")
            return []
    
    def scrape_profile_page(self, contact):
        """Scrapes an individual profile page to get the email"""
        try:
            self.driver.get(contact.profile_url)
            time.sleep(2)
            
            try:
                # Regex solution to scrape the emails from the profile page
                page_text = self.driver.find_element(By.TAG_NAME, "body").text

                # Use regex to find emails
                email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
                emails = re.findall(email_pattern, page_text)
                if len(emails) > 0:
                    contact.email = emails[0]
                return contact
            except Exception as e:
                self.logger.error(f"Could not parse email for {contact.profile_url}: {str(e)}")
                return contact
            
        except Exception as e:
            self.logger.error(f"Error parsing profile page {contact.profile_url}: {str(e)}")
            return contact

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Distributed Web Scraper Worker Node')
    parser.add_argument('--node-id', required=True, help='Unique identifier for this worker node')
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host IP or hostname')
    
    args = parser.parse_args()
    
    worker = WorkerNode(args.node_id, args.rabbitmq_host)
    worker.start()

if __name__ == '__main__':
    main()