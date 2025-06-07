#!/usr/bin/env python3
# Author: Loic Pottier  <pottier1@llnl.gov>

import argparse
import calendar
import csv
import json
import os
import time
import re
import socket
import ssl
import sys

from multiprocessing import Process, current_process, Manager

import numpy as np
import pika
import psutil

def format_bytes(size_in_bytes):
    """
    Convert a byte count into a human-readable string in B, KB, MB, or GB.
    """
    if size_in_bytes >= 1 << 30:
        return f"{size_in_bytes / (1 << 30):.2f} GB"
    elif size_in_bytes >= 1 << 20:
        return f"{size_in_bytes / (1 << 20):.2f} MB"
    elif size_in_bytes >= 1 << 10:
        return f"{size_in_bytes / (1 << 10):.2f} KB"
    else:
        return f"{size_in_bytes} B"

def parse_size_string(size_str):
    """
    Parses a size string like '100MB', '0.5 GB', '2048 KB' and returns the number of bytes.
    Supports B, KB, MB, GB (case-insensitive).
    """
    units = {
        'b': 1,
        'kb': 1 << 10,
        'mb': 1 << 20,
        'gb': 1 << 30,
    }

    size_str = size_str.strip().lower().replace(" ", "")
    match = re.fullmatch(r'(\d+(\.\d+)?)([a-z]+)', size_str)

    if not match:
        raise ValueError(f"Invalid size format: '{size_str}'")

    number, _, unit = match.groups()

    if unit not in units:
        raise ValueError(f"Unknown unit '{unit}' in size string '{size_str}'")

    return int(float(number) * units[unit])


def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Config file '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, 'r') as f:
        return json.load(f)

def create_connection(config: dict, cacert: str = None) -> pika.BlockingConnection:
    if cacert is None:
        ssl_options = None
    else:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = False
        context.load_verify_locations(cacert)
        ssl_options = pika.SSLOptions(context)

    credentials = pika.PlainCredentials(config["rabbitmq-user"], config["rabbitmq-password"])
    parameters = pika.ConnectionParameters(
        host=config["service-host"],
        port=config["service-port"],
        virtual_host=config["rabbitmq-vhost"],
        credentials=credentials,
        ssl_options=ssl_options
    )

    return pika.BlockingConnection(parameters)

def send_messages_process(config, cacert, exchange, exchange_type, routing_key, num_messages, message_size, verbose, sleep_time, process_index, stats_dict):
    process = psutil.Process(os.getpid())
    connection = create_connection(config, cacert)
    channel = connection.channel()
    if exchange == '':
        result = channel.queue_declare(queue = routing_key, exclusive = False, durable = False, auto_delete = False)
        if verbose:
            print(f"[Send {process_index}] Declared queue '{result.method.queue}'")
    else:
        channel.exchange_declare(exchange = exchange, exchange_type = exchange_type, auto_delete = False)
        if verbose:
            print(f"[Send {process_index}] Declared exchange '{exchange}'")

    total_size = 0
    start = time.perf_counter()
    cpu_percent = []
    mem_usage = []

    if exchange_type == "topic":
        rkey = f"{routing_key}.{process_index}"
    else:
        rkey = routing_key

    for i in range(num_messages):
        data = np.random.randint(0, 256, size=message_size, dtype=np.uint8)
        message = data.tobytes()

        channel.basic_publish(exchange=exchange, routing_key=rkey, body=message)

        total_size += len(message)
        if (i+1) % 1000 == 0:
            print(f"[Send {process_index}] Sent message {i+1}/{num_messages} ({format_bytes(len(message))})")

        # Monitor CPU and memory
        cpu_percent.append(process.cpu_percent(interval=None))  # non-blocking
        mem_usage.append(process.memory_info().rss)
        time.sleep(sleep_time)

    end = time.perf_counter() - num_messages*(sleep_time)
    duration = end - start
    bandwidth_gb = (total_size / 1e9) / duration if duration > 0 else 0

    start_closing = time.perf_counter()
    channel.close()
    connection.close()
    end_closing = time.perf_counter() - start_closing

    print(f"[Send {process_index}] Sent {num_messages} messages "
            f"({format_bytes(total_size)}) in {duration:.2f}s ({bandwidth_gb:.6f} GB/s) (closing connection in {end_closing:.2f} secs)")

    # Save monitoring stats to shared dictionary
    stats_dict[process_index] = {
        "messages_sent": num_messages,
        "total_bytes": total_size,
        "duration_secs": duration,
        "bandwidth_gbs": bandwidth_gb,
        "avg_cpu_percent": sum(cpu_percent)/len(cpu_percent) if cpu_percent else 0,
        "avg_memory_bytes": sum(mem_usage)/len(mem_usage) if mem_usage else 0,
    }

def main():
    parser = argparse.ArgumentParser(description='Send messages to RabbitMQ.')
    parser.add_argument('--config', required=True, help='Path to RabbitMQ config JSON file.')
    parser.add_argument('--nmsgs', '-n', type=int, required=True, help='Number of messages to send per process.')
    parser.add_argument('--size', '-s', type=str, required=True, help='Size of each message (e.g. 512B, 1KB, 2MB, 0.5GB)')
    parser.add_argument('--routing-key', '-r', required=True, help='Queue name to send messages to.')
    parser.add_argument('--exchange', '-e', required=False, default='', help='Exchange name to send messages to.')
    parser.add_argument('--verbose', '-v', action="store_true", help='Verbose output')
    parser.add_argument('--processes', '-p', type=int, default=1, help='Number of parallel processes to use')
    parser.add_argument('--monitoring', '-m', required=False, help='Output CSV file for performance monitoring')
    parser.add_argument('--sleep', required=False, type=float, default=0.0, help='Sleep time between each message')
    parser.add_argument('--strong-scaling', action="store_true", help='Number of messages is divided by number of processes')
    parser.add_argument('--unique-id', '-id', required=False, help='Unique ID for CSV')
    parser.add_argument('--use-topic', '-topic', action="store_true", help='Use topic exchange and send messages to routing_key.process_id')

    args = parser.parse_args()

    try:
        message_size = parse_size_string(args.size)
    except ValueError as e:
        print(f"Error parsing --size: {e}")
        sys.exit(1)
    
    if args.nmsgs <= 0:
        print(f"Error parsing --nmsgs: {args.nmsgs} cannot be <= 0")
        sys.exit(1)

    manager = Manager()
    stats_dict = manager.dict()

    config = load_config(args.config)
    cacert = config["rabbitmq-cert"]
 
    exchange_type = "topic" if args.use_topic else "direct"

    # Distribute messages evenly
    messages_per_proc = args.nmsgs // args.processes
    extra = args.nmsgs % args.processes
    processes = []
    for i in range(args.processes):
        if args.strong_scaling:
            count = messages_per_proc + (1 if i < extra else 0)
        else:
            count = args.nmsgs
            messages_per_proc = args.nmsgs
        proc = Process(
            target = send_messages_process,
            args = (
                config, cacert, args.exchange, exchange_type, args.routing_key,
                count, message_size, args.verbose, args.sleep, i, stats_dict)
            )
        processes.append(proc)
        proc.start()

    for proc in processes:
        proc.join()

    bandwidth_gb = sum(stat["bandwidth_gbs"] for stat in stats_dict.values())
    print(f"Sent {count} messages: aggregated bandwidth {bandwidth_gb:.6f} GB/s)")

    unique_id = args.unique_id or calendar.timegm(time.gmtime())

    if args.monitoring is not None:
        hostname = socket.gethostname()
        combined = {
            "exp_id": str(unique_id),
            "total_messages": sum(stat["messages_sent"] for stat in stats_dict.values()),
            "num_processes": args.processes,
            "hostname": hostname,
            "used_exchange": args.exchange !='',
            "sleep_time_secs": args.sleep,
            "messages_per_proc": messages_per_proc,
            "message_size": args.size,
            "message_count": args.nmsgs,
            "total_bytes": sum(stat["total_bytes"] for stat in stats_dict.values()),
            "avg_bandwidth_gbs": sum(stat["bandwidth_gbs"] for stat in stats_dict.values()) / len(stats_dict),
            "agg_bandwidth_gbs": sum(stat["bandwidth_gbs"] for stat in stats_dict.values()),
            "avg_cpu_percent": sum(stat["avg_cpu_percent"] for stat in stats_dict.values()) / len(stats_dict),
            "avg_memory_bytes": sum(stat["avg_memory_bytes"] for stat in stats_dict.values()) / len(stats_dict),
            # "process_stats": dict(stats_dict)
        }

        file_exists = os.path.isfile(args.monitoring)

        with open(args.monitoring, "a") as csv_file:
            writer = csv.writer(csv_file)
            if not file_exists:
                writer.writerow([key for key in combined])
            writer.writerow([val for key,val in combined.items()])

        if args.verbose:
            print(f"Monitoring data written to {args.monitoring}")

if __name__ == '__main__':
    main()
