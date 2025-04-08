#!/usr/bin/env python3
import argparse
import json
import time
import subprocess
import os
from datetime import datetime

#should be in a .env but whatever
REMOTE_USER = "ralph"  # The username to SSH as on worker nodes
REMOTE_PROJECT_PATH = "/home/ralph/DISTRIBUTED_PROJECT" # Project path on workers
REMOTE_PYTHON_EXEC = f"{REMOTE_PROJECT_PATH}/venv/bin/python" # Path to python WITHIN the virtualenv on workers
# --- End Configuration ---

def start_coordinator(url, scrape_time, num_nodes, rabbitmq_host, server_info):
    """Start coordinator process on the remote server"""
    print(f"Starting coordinator on remote server for {url} with {num_nodes} nodes for {scrape_time} minutes (RabbitMQ: {rabbitmq_host})")
    
    # Parse the host info (host -p port format)
    host_parts = server_info.split()
    hostname = host_parts[0]
    ssh_options = host_parts[1:] if len(host_parts) > 1 else []
    
    # Build the SSH command
    ssh_cmd = ["ssh"]
    if ssh_options:
        ssh_cmd.extend(ssh_options)
    ssh_cmd.append(f"{REMOTE_USER}@{hostname}")
    
    # Build the remote command to run coordinator
    remote_cmd = (
        f"cd {REMOTE_PROJECT_PATH} && "
        f"{REMOTE_PYTHON_EXEC} -m scraper.distributed.coordinator "
        f"{url} "
        f"--time {scrape_time} "
        f"--nodes {num_nodes} "
        f"--rabbitmq-host {rabbitmq_host}"
    )
    
    ssh_cmd.append(remote_cmd)
    
    print(f"Executing: {' '.join(ssh_cmd)}")
    
    try:
        # Start coordinator on remote server
        coordinator_proc = subprocess.Popen(ssh_cmd)
        print(f"Coordinator started on {hostname} with local PID {coordinator_proc.pid}")
        return coordinator_proc
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        print(f"Error starting coordinator on {hostname}: {e}")
        return None

def deploy_workers(node_configs, rabbitmq_host, run_worker_on_coordinator=True):
    """
    Deploy and start worker processes on all nodes via SSH
    If run_worker_on_coordinator is True, also run a worker on the first server
    """
    worker_processes = {}  # Use a dictionary {node_id: (hostname, proc)}
    
    for node_id, node_info in node_configs:
        # Parse the node_info which may have hostname and port options
        host_parts = node_info.split()
        
        # First part is always the hostname
        hostname = host_parts[0]
        
        # Check if we have additional SSH options
        has_port = False
        port = ""
        ssh_options = []
        
        # Extract port information if it exists
        for i in range(1, len(host_parts), 2):
            if i+1 < len(host_parts):
                if host_parts[i] == "-p":
                    has_port = True
                    port = host_parts[i+1]
                    ssh_options.extend(["-p", port])
                else:
                    # Any other SSH options
                    ssh_options.extend([host_parts[i], host_parts[i+1]])
        
        # If this is the first node and we're running a worker on the coordinator server,
        # we need to track if we've already deployed to the coordinator server
        if node_id == 'node1' and not run_worker_on_coordinator:
            print(f"Skipping worker on coordinator server {hostname}")
            continue
            
        print(f"Attempting to start worker on {hostname}{' (port '+port+')' if has_port else ''} (Node ID: {node_id})")
        
        try:
            # Construct the remote command
            remote_cmd_str = (
                f"cd {REMOTE_PROJECT_PATH} && "
                f"{REMOTE_PYTHON_EXEC} -m scraper.distributed.worker "
                f"--node-id={node_id} "
                f"--rabbitmq-host={rabbitmq_host}"
            )
            
            # Construct the SSH command properly with options
            ssh_cmd = ["ssh"]
            
            # Add all SSH options before the hostname
            if ssh_options:
                ssh_cmd.extend(ssh_options)
            
            # Add the user@hostname
            ssh_cmd.append(f"{REMOTE_USER}@{hostname}")
            
            # Add the remote command as the last argument
            ssh_cmd.append(remote_cmd_str)
            
            print(f"Executing: {' '.join(ssh_cmd)}")
            
            # Start the SSH process
            ssh_proc = subprocess.Popen(ssh_cmd)
            worker_processes[node_id] = (hostname, ssh_proc)
            print(f"SSH command initiated for worker {node_id} on {hostname} (local PID: {ssh_proc.pid})")
            
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"Failed to start worker {node_id} on {hostname}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred deploying worker {node_id} on {hostname}: {e}")
    
    return worker_processes
    
def terminate_process(node_id, hostname, proc):
    """Helper to terminate a local process"""
    if isinstance(proc, subprocess.Popen) and proc.poll() is None:
        print(f"Terminating worker {node_id} on {hostname} (PID: {proc.pid})")
        try:
            proc.terminate() # Try graceful termination first
            proc.wait(timeout=5)
            print(f"Worker {node_id} terminated.")
        except subprocess.TimeoutExpired:
            print(f"Worker {node_id} did not terminate gracefully, killing...")
            proc.kill() # Force kill
        except Exception as e:
            print(f"Error terminating worker {node_id}: {e}")

def monitor_job(coordinator_proc, worker_processes):
    """
    Monitor the coordinator process. Assumes coordinator handles overall job completion/timeout.
    Optionally checks if worker SSH processes have exited, but this isn't a perfect indicator
    of the remote worker's status.
    """
    if not coordinator_proc:
        print("Coordinator process not started. Cannot monitor job.")
        return
    
    print("\nMonitoring Coordinator and Worker SSH processes. Press Ctrl+C to stop early.")
    try:
        # Wait for the coordinator SSH process to finish
        # This doesn't necessarily indicate the remote coordinator is done,
        # but it's our best indicator from the client
        coordinator_proc.wait()
        
        coord_exit_code = coordinator_proc.returncode
        print(f"\nCoordinator process finished with exit code: {coord_exit_code}")
        if coord_exit_code != 0:
            print("Coordinator exited with an error.")
        
    except KeyboardInterrupt:
        print("\nReceived interrupt. Stopping coordinator and workers...")
        # Stop coordinator first
        if coordinator_proc.poll() is None:
            print("Terminating coordinator SSH connection...")
            terminate_process("coordinator", "remote", coordinator_proc)
        
        # Stop workers (SSH processes)
        # Note: Terminating the SSH process *might* terminate the remote command
        # depending on SSH configuration and how the remote process handles signals.
        for node_id, (hostname, proc) in worker_processes.items():
             terminate_process(node_id, hostname, proc)
    
    finally:
        print("Attempting final cleanup of worker SSH processes...")
        # Ensure all worker processes initiated by the client are cleaned up
        for node_id, (hostname, proc) in worker_processes.items():
             terminate_process(node_id, hostname, proc)

def check_results(coordinator_server):
    """Check and display results of the scraping job from the remote server"""
    try:
        # Parse the host info (host -p port format)
        host_parts = coordinator_server.split()
        hostname = host_parts[0]
        ssh_options = host_parts[1:] if len(host_parts) > 1 else []
        
        # Build the SSH command to check for results
        ssh_cmd = ["ssh"]
        if ssh_options:
            ssh_cmd.extend(ssh_options)
        ssh_cmd.append(f"{REMOTE_USER}@{hostname}")
        
        # Check if contacts.csv exists on remote server
        contacts_cmd = ssh_cmd.copy()
        contacts_cmd.append(f"cat {REMOTE_PROJECT_PATH}/contacts.csv 2>/dev/null || echo 'No contacts file found'")
        contacts_result = subprocess.run(contacts_cmd, capture_output=True, text=True)
        
        if "No contacts file found" not in contacts_result.stdout:
            # Count lines in contacts.csv
            lines = contacts_result.stdout.strip().split('\n')
            print(f"\nScraping complete! Found {len(lines) - 1} contacts.")  # Subtract header row
        else:
            print("\nNo contacts file found on remote server. Scraping may have failed.")
        
        # Check if statistics file exists on remote server
        stats_cmd = ssh_cmd.copy()
        stats_cmd.append(f"cat {REMOTE_PROJECT_PATH}/scraping_stats.json 2>/dev/null || echo '{{}}'")
        stats_result = subprocess.run(stats_cmd, capture_output=True, text=True)
        
        if stats_result.stdout and stats_result.stdout.strip() != '{}':
            try:
                stats = json.loads(stats_result.stdout)
                if isinstance(stats, list):
                    stats = stats[-1]  # Get the latest stats
                
                print("\nScraping Statistics:")
                print(f"Total pages visited: {stats.get('total_pages_visited', 0)}")
                print(f"Total emails recorded: {stats.get('total_emails_recorded', 0)}")
                print(f"Programs visited: {stats.get('programs_visited', 0)}")
                print(f"Colleges scraped: {stats.get('colleges_count', 0)}")
                
                # Print node contributions if available
                node_contributions = stats.get('node_contributions', {})
                if node_contributions:
                    print("\nContributions by node:")
                    for node_id, count in node_contributions.items():
                        print(f"  {node_id}: {count} contacts")
            except json.JSONDecodeError:
                print("\nInvalid statistics file format.")
        else:
            print("\nNo statistics file found on remote server.")
    
    except Exception as e:
        print(f"Error checking results: {e}")

def main():
    global REMOTE_USER, REMOTE_PROJECT_PATH, REMOTE_PYTHON_EXEC
    """Main entry point for the distributed scraper client"""
    parser = argparse.ArgumentParser(description='Distributed Web Scraper Client')
    parser.add_argument('url', help='Base URL to scrape')
    parser.add_argument('--time', type=int, default=1, help='Scrape time in minutes')
    parser.add_argument('--nodes', type=int, default=1, help='Number of nodes to use (default: 1)')
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host IP or hostname accessible by all nodes')
    parser.add_argument('--remote-user', default=REMOTE_USER, help='SSH username for remote nodes')
    parser.add_argument('--remote-path', default=REMOTE_PROJECT_PATH, help='Project path on remote nodes')
    parser.add_argument('--no-worker-on-coordinator', action='store_true', 
                       help='Do not run a worker on the coordinator server')

    args = parser.parse_args()

    # Update global config if provided via args
    REMOTE_USER = args.remote_user
    REMOTE_PROJECT_PATH = args.remote_path
    REMOTE_PYTHON_EXEC = f"{REMOTE_PROJECT_PATH}/venv/bin/python"

    # Define worker nodes from argument
    node_configs = [
        ('node1', 'ccscloud.dlsu.edu.ph -p 31430'),
        ('node2', 'ccscloud.dlsu.edu.ph -p 31431'),  # For demo purposes, running multiple workers on localhost
        ('node3', 'ccscloud.dlsu.edu.ph -p 31432')   # In production, these would be different hostnames
    ][:args.nodes] 

    print(f"Starting distributed scraping with {len(node_configs)} nodes:")
    for i, (node_id, hostname) in enumerate(node_configs):
        print(f"  {i+1}. ID: {node_id}, Host: {hostname}")

    # Start coordinator (pass rabbitmq host)
    coordinator_process = start_coordinator(args.url, args.time, len(node_configs), args.rabbitmq_host, node_configs[0][1])

    if not coordinator_process:
        print("Failed to start coordinator. Exiting.")
        return
    
    # Deploy workers to all nodes
    worker_processes_dict = deploy_workers(
        node_configs,
        args.rabbitmq_host,
        not args.no_worker_on_coordinator  # Run worker on coordinator unless flag is set
    )
    
    if not worker_processes_dict:
        print("No workers were successfully started. Exiting.")
        if coordinator_process and coordinator_process.poll() is None:
            terminate_process("coordinator", "remote", coordinator_process)
        return

        #
    else:
        print("Coordinator failed to start. Stopping any running workers...")
        # Attempt to stop workers even if coordinator failed
        for node_id, (hostname, proc) in worker_processes_dict.items():
            terminate_process(node_id, hostname, proc)

    # Give workers time to initialize and connect to RabbitMQ
    print("Waiting 5 seconds for workers to initialize...")
    time.sleep(5)
     # Monitor the coordinator and worker processes
    monitor_job(coordinator_process, worker_processes_dict)
    
    # Check results from the remote coordinator server
    check_results(node_configs[0][1])   

if __name__ == '__main__':
    main()