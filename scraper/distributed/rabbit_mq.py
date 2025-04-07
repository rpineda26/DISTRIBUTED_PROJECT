import pika
import time
# No longer attempting to start service, so subprocess/os are removed
# import subprocess
# import os

# --- Updated setup_rabbitmq function ---
def setup_rabbitmq(host='localhost'):
    """
    Set up RabbitMQ with required queues on the specified host.
    Assumes RabbitMQ service is already running and accessible on that host.

    Args:
        host (str): The hostname or IP address of the RabbitMQ server.
                    Defaults to 'localhost'.

    Returns:
        bool: True if configuration was successful, False otherwise.
    """
    print(f"Attempting to configure RabbitMQ queues on host: {host}")

    try:
        # Create connection parameters using the provided host
        connection_params = pika.ConnectionParameters(host=host)

        # Try to connect to verify RabbitMQ is running and accessible
        connection = pika.BlockingConnection(connection_params)
        print(f"Successfully connected to RabbitMQ on {host}.")

        channel = connection.channel()

        # Declare task queues as durable (survive RabbitMQ restarts)
        print("Declaring durable queues: program_tasks, directory_tasks, profile_tasks...")
        channel.queue_declare(queue='program_tasks', durable=True)
        channel.queue_declare(queue='directory_tasks', durable=True)
        channel.queue_declare(queue='profile_tasks', durable=True)

        # Declare result/status queues as durable
        print("Declaring durable queues: contact_results, status_updates...")
        channel.queue_declare(queue='contact_results', durable=True)
        channel.queue_declare(queue='status_updates', durable=True)

        connection.close()
        print(f"RabbitMQ queues configured successfully on {host}")
        return True

    except pika.exceptions.AMQPConnectionError as e:
        print(f"\nError: Could not connect to RabbitMQ on host '{host}'.")
        print("Please ensure the RabbitMQ service is running on that host and accessible.")
        print(f"Connection Error Details: {e}\n")
        return False
    except Exception as e:
        print(f"\nError: Failed to configure RabbitMQ queues on host '{host}'.")
        print(f"Details: {str(e)}\n")
        return False

# --- setup_worker_node remains the same ---
def setup_worker_node(node_id, rabbitmq_host='localhost'):
    """Configure a worker node to connect to the specified RabbitMQ server"""
    try:
        # Create connection parameters for the worker
        connection_params = pika.ConnectionParameters(
            host=rabbitmq_host,
            heartbeat=600,  # Longer heartbeat for stability
            blocked_connection_timeout=300
        )

        # Test connection
        connection = pika.BlockingConnection(connection_params)
        connection.close()

        # Create or update configuration file for the worker
        config = {
            'rabbitmq_host': rabbitmq_host,
            'node_id': node_id
        }

        # In a real implementation, you might save this to a file
        # or use environment variables

        print(f"Worker node {node_id} configuration check successful for RabbitMQ host {rabbitmq_host}")
        return True
    except Exception as e:
        print(f"Failed to configure/connect worker node {node_id} to RabbitMQ host {rabbitmq_host}: {str(e)}")
        return False



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Setup RabbitMQ and worker nodes for distributed scraping')
    # --- Updated help text for setup-rabbitmq ---
    parser.add_argument('--setup-rabbitmq', action='store_true', help='Setup RabbitMQ queues on the specified host (assumes RabbitMQ is running)')
    parser.add_argument('--setup-worker', action='store_true', help='Setup/Test connection for a worker node')
    parser.add_argument('--node-id', help='Worker node ID (required for --setup-worker)')
    # --- rabbitmq-host applies to both setup-rabbitmq and setup-worker ---
    parser.add_argument('--rabbitmq-host', default='localhost', help='RabbitMQ host IP or hostname')

    args = parser.parse_args()

    if args.setup_rabbitmq:
        # --- Pass the host argument to the function ---
        setup_rabbitmq(host=args.rabbitmq_host)

    if args.setup_worker:
        if args.node_id:
            # Pass the host argument here too
            setup_worker_node(args.node_id, rabbitmq_host=args.rabbitmq_host)
        else:
            print("Error: --node-id is required when using --setup-worker")

