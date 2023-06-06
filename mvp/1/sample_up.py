import socket
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable
import aiokafka

def check_kafka_broker_connection(bootstrap_servers):
    # Check if the Kafka broker is reachable
    try:
        broker_host, broker_port = bootstrap_servers.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((broker_host, int(broker_port)))
        if result == 0:
            print(f"Kafka broker at {bootstrap_servers} is reachable.")
        else:
            print(f"Kafka broker at {bootstrap_servers} is unreachable.")
        sock.close()
    except ValueError:
        print("Invalid bootstrap servers format. Expected format: host:port")


def check_kafka_broker_configuration(bootstrap_servers):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        broker_info = admin_client.list_topics()
        print(f"Kafka broker configuration at {bootstrap_servers}:")
        print(broker_info)
    except (ValueError, KafkaTimeoutError, NoBrokersAvailable) as e:
        print(f"Failed to retrieve Kafka broker configuration: {e}")


def check_aiokafka_compatibility():
    print(f"aiokafka version: {aiokafka.__version__}")


def main():
    bootstrap_servers = '127.0.0.1:9092'
    print("Performing Kafka connection checks\n")

    print("Checking Kafka broker connectivity...")
    check_kafka_broker_connection(bootstrap_servers)

    print("✅✅\n\nChecking Kafka broker configuration...")
    check_kafka_broker_configuration(bootstrap_servers)

    print("✅✅\n\nChecking aiokafka library compatibility...")
    check_aiokafka_compatibility()

    print("✅✅")


if __name__ == '__main__':
    main()
