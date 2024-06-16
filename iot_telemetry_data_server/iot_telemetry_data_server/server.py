import socket
import time
import threading
import csv
import logging
import sys

# Global variable to keep track of the server socket
server_socket = None
shutdown_event = threading.Event()

BATCH_SIZE = 20
SEND_RATE = 1  # in seconds


def handle_client(client_socket: socket.socket, data_file_path: str):
    with open(data_file_path, mode="r") as file:
        csv_reader = csv.reader(file)

        try:
            i = 0
            stream_data = ""
            for row in csv_reader:
                row_as_str = ", ".join(row) + "\n"
                stream_data += row_as_str
                logging.info(row_as_str)

                i += 1
                # Send over TCP
                if i == BATCH_SIZE:
                    client_socket.send(stream_data.encode())
                    stream_data = ""
                    i = 0
                    time.sleep(SEND_RATE)

        except Exception as e:
            logging.error(f"Error: {str(e)}")
        finally:
            logging.warning("Closing client socket")
            client_socket.close()


def start_server(host: str, port: int, data_file_path: str):
    global server_socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()

    logging.info(f"Server listening on {host}:{port}")

    while not shutdown_event.is_set():
        try:
            server_socket.settimeout(
                1
            )  # Add a timeout to allow periodic shutdown checks
            client_socket, address = server_socket.accept()
            logging.info(f"Client connected from {address[0]}:{address[1]}")
            client_thread = threading.Thread(
                target=handle_client, args=(client_socket, data_file_path)
            )
            client_thread.start()
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"Error: {str(e)}")
            break

    logging.warning("Server shutting down...")
    server_socket.close()


def shutdown_server():
    logging.warning("Shutdown initiated...")
    shutdown_event.set()
    if server_socket:
        server_socket.close()
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    host = "localhost"
    port = 9999

    try:
        start_server(host, port, "iot_telemetry_data.csv")
    except KeyboardInterrupt:
        shutdown_server()
