#!/usr/bin/env python3
import argparse
import json
import time
import uuid
import pika
from datetime import datetime, timedelta

from scraper.utils.data_models import ScraperStatistics

class DistributedWebScraper:
    def __init__(self, base_url, scrape_time_minutes, num_nodes, rabbitmq_host='localhost'):
        self.base_url = base_url
        self.scrape_time_minutes = scrape_time_minutes
        self.num_nodes = num_nodes
        self.job_id = str(uuid.uuid4())
        self.end_time = datetime.now() + timedelta(minutes=scrape_time_minutes)
        
        # Set up RabbitMQ connection
        self.credentials = pika.PlainCredentials('rabbituser1', 'rabbit1234')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=5672, credentials=self.credentials))
        self.channel = self.connection.channel()
        
        # Declare queues
        self.setup_queues()
        
        # Initialize stats
        self.stats = ScraperStatistics(base_url, num_nodes, scrape_time_minutes)
        
    def setup_queues(self):
        """Set up all required RabbitMQ queues"""
        # Task queues
        self.channel.queue_declare(queue='program_tasks', durable=False)
        self.channel.queue_declare(queue='directory_tasks', durable=False)
        self.channel.queue_declare(queue='profile_tasks', durable=False)
        
        # Result queues
        self.channel.queue_declare(queue='contact_results', durable=False)
        self.channel.queue_declare(queue='status_updates', durable=False) 
        
    def start(self):
        """Start the distributed scraping process"""
        print(f"Starting distributed scraping job {self.job_id}")
        print(f"Target URL: {self.base_url}")
        print(f"Number of nodes: {self.num_nodes}")
        print(f"Scrape time: {self.scrape_time_minutes} minutes")
        
        # Create initial task to fetch program URLs
        self.channel.basic_publish(
            exchange='',
            routing_key='program_tasks',
            body=json.dumps({
                'job_id': self.job_id,
                'task_type': 'get_college_program_urls',
                'base_url': self.base_url
            }),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
        # Monitor progress until completion or timeout
        self.monitor_progress()
        
        # Cleanup
        self.connection.close()
        
    def monitor_progress(self):
        """Monitor the progress of the scraping job"""
        start_time = datetime.now()
        
        # Set up consumer for status updates
        self.channel.basic_consume(
            queue='status_updates',
            on_message_callback=self.process_status_update,
            auto_ack=True
        )
        
        # Set up consumer for results
        self.channel.basic_consume(
            queue='contact_results',
            on_message_callback=self.process_result,
            auto_ack=True
        )
        
        # Start monitoring with timeout
        print("\nMonitoring job progress...")
        
        try:
            while datetime.now() < self.end_time:
                # Process messages for a short time, then check if we should continue
                self.connection.process_data_events(time_limit=1)
                
                # Print progress
                elapsed = (datetime.now() - start_time).seconds
                remaining = max(0, self.scrape_time_minutes * 60 - elapsed)
                print(f"\rTime remaining: {remaining//60}m {remaining%60}s | "
                      f"Contacts found: {self.stats.total_emails_recorded}", end="")
                
                # # Check queue stats to see if we're done
                # if self.are_queues_empty() and self.stats.total_emails_recorded > 0:
                #     print("\nAll queues empty and contacts found. Job complete!")
                #     break
                    
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nReceived interrupt signal. Shutting down...")
        
        # Save final statistics
        self.save_stats()
        
    def are_queues_empty(self):
        """Check if all task queues are empty"""
        program_queue = self.channel.queue_declare(queue='program_tasks', passive=True)
        directory_queue = self.channel.queue_declare(queue='directory_tasks', passive=True)
        profile_queue = self.channel.queue_declare(queue='profile_tasks', passive=True)
        
        return (program_queue.method.message_count == 0 and
                directory_queue.method.message_count == 0 and
                profile_queue.method.message_count == 0)
    
    def process_status_update(self, ch, method, properties, body):
        """Process status updates from worker nodes"""
        try:
            update = json.loads(body)
            if update['job_id'] != self.job_id:
                return  # Ignore updates from other jobs
                
            stat_type = update.get('stat_type')
            program_name = update.get('program_name')
            
            # Update statistics
            if stat_type == "page_visit":
                self.stats.total_pages_visited += 1
            elif stat_type == "college":
                self.stats.colleges_count += 1
            elif stat_type == "program":
                self.stats.programs_visited += 1
                self.stats.total_pages_visited += 1
            elif stat_type == "faculty_url_success" and program_name:
                self.stats.programs_with_faculty_url[program_name] = True
                self.stats.total_pages_visited += 1
            elif stat_type == "faculty_url_failure" and program_name:
                self.stats.programs_without_faculty_url[program_name] = True
            elif stat_type == "personnel_found" and program_name:
                self.stats.program_personnel_count[program_name] = \
                    self.stats.program_personnel_count.get(program_name, 0) + 1
            elif stat_type == "complete_record" and program_name:
                self.stats.program_complete_records[program_name] = \
                    self.stats.program_complete_records.get(program_name, 0) + 1
                self.stats.total_emails_recorded += 1
            elif stat_type == "incomplete_record" and program_name:
                self.stats.program_incomplete_records[program_name] = \
                    self.stats.program_incomplete_records.get(program_name, 0) + 1
                    
        except Exception as e:
            print(f"Error processing status update: {e}")
    
    def process_result(self, ch, method, properties, body):
        """Process contact results from worker nodes"""
        try:
            contact_data = json.loads(body)
            
            # Write to CSV
            with open('contacts.csv', 'w', newline='') as csvfile:
                if csvfile.tell() == 0:
                    # Write header if file is empty
                    csvfile.write('email,name,office,department,profile_url\n')
                
                # Write contact data
                csvfile.write(f"{contact_data['email']},{contact_data['name']},"
                              f"{contact_data['office']},{contact_data['department']},"
                              f"{contact_data['profile_url']}\n")
                
        except Exception as e:
            print(f"Error processing result: {e}")
    
    def save_stats(self):
        """Save final statistics to file"""
        try:
            stats_file = "scraping_stats.json"
            
            # Try loading existing data
            try:
                with open(stats_file, "r") as f:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data]
            except (FileNotFoundError, json.JSONDecodeError):
                existing_data = []
                
            # Append current stats
            existing_data.append(self.stats.to_dict())
            
            # Write updated data
            with open(stats_file, "w") as f:
                json.dump(existing_data, f, indent=4)
                
            print(f"Statistics saved to {stats_file}")
            
        except Exception as e:
            print(f"Error saving statistics: {e}")


def clear_queues(rabbitmq_host, credentials):
    queues_to_clear = ['program_tasks', 'directory_tasks', 'profile_tasks', 'contact_results', 'status_updates']
    conn = None
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=5672, credentials=credentials))
        ch = conn.channel()
        for q in queues_to_clear:
            try:
                ch.queue_delete(queue=q)
                print(f"Deleted queue: {q}")
            except Exception as e:
                # Might fail if queue doesn't exist, which is fine
                print(f"Could not delete queue {q} (may not exist): {e}")
    except Exception as e:
        print(f"Error connecting to RabbitMQ to clear queues: {e}")
    finally:
        if conn and conn.is_open:
            conn.close()
def main():
    parser = argparse.ArgumentParser(description='Distributed Web Scraper')
    parser.add_argument('url', help='Base URL to scrape')
    parser.add_argument('--time', type=int, default=1, help='Scrape time in minutes')
    parser.add_argument('--nodes', type=int, default=1, help='Number of worker nodes')
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host')
    
    args = parser.parse_args()
    credentials = pika.PlainCredentials('rabbituser1', 'rabbit1324')
    # clear_queues(args.rabbitmq_host, credentials)
    # Create and start scraper
    scraper = DistributedWebScraper(args.url, args.time, args.nodes, args.rabbitmq_host)
    scraper.start()

if __name__ == '__main__':
    main()