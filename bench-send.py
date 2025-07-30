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
from pika import DeliveryMode
import pika.exceptions
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
        ssl_options=ssl_options,
        heartbeat=0
    )

    return pika.BlockingConnection(parameters)

def send_messages_process(config, cacert, exchange, exchange_type, routing_key, num_messages, message_size, verbose, sleep_time, process_index, stats_dict):
    process = psutil.Process(os.getpid())
    connection = create_connection(config, cacert)
    channel = connection.channel()

    if exchange == '':
        result = channel.queue_declare(queue = routing_key, exclusive = False, durable = False, auto_delete = False)
        if verbose:
            print(f"[Send {process_index}/{time.perf_counter()}] Declared queue '{result.method.queue}'")
    else:
        channel.exchange_declare(exchange = exchange, exchange_type = exchange_type, auto_delete = False)
        if verbose:
            print(f"[Send {process_index}/{time.perf_counter()}] Declared exchange '{exchange}'")

    total_size = 0
    start = time.perf_counter()
    cpu_percent = []
    mem_usage = []
    start_timing = 0
    end_timing = []

    if exchange_type == "topic":
        rkey = f"{routing_key}.{process_index}"
    else:
        rkey = routing_key

    confirm_delivery = False

    if confirm_delivery:

        # Turn on delivery confirmations
        channel.confirm_delivery()

        message_sent = 0
        for i in range(num_messages):
            start_timing = time.perf_counter()
            data = np.random.randint(0, 256, size=message_size, dtype=np.uint8)
            message = data.tobytes()
            
            try:
                channel.basic_publish(exchange=exchange, routing_key=rkey, body=message, properties=pika.BasicProperties(delivery_mode=DeliveryMode.Transient), mandatory=True)
                message_sent +=1
                total_size += len(message)
                if verbose and (message_sent % 100 == 0) and message_sent > 0:
                    print(f"[Send {process_index}/{time.perf_counter()}] Sent message to {exchange}/{rkey} => {i+1}/{num_messages} ({format_bytes(len(message))})")
            except pika.exceptions.AMQPChannelError as e:
                print(f"[Send {process_index}/{time.perf_counter()}] Message #{i} could not be confirmed {e}")

            # Monitor CPU and memory
            cpu_percent.append(process.cpu_percent(interval=None))  # non-blocking
            mem_usage.append(process.memory_info().rss)
            end_timing.append(time.perf_counter() - start_timing)
            time.sleep(sleep_time)
    else:
        message_sent = 0
        for i in range(num_messages):
            start_timing = time.perf_counter()
            data = np.random.randint(0, 256, size=message_size, dtype=np.uint8)
            message = data.tobytes()
            channel.basic_publish(exchange=exchange, routing_key=rkey, body=message, properties=pika.BasicProperties(delivery_mode=DeliveryMode.Transient))
            message_sent +=1
            total_size += len(message)
            if verbose and (message_sent % 2000 == 0) and message_sent > 0:
                print(f"[Send {process_index}/{time.perf_counter()}] Sent message to {exchange}/{rkey} => {i+1}/{num_messages} ({format_bytes(len(message))})")

            # Monitor CPU and memory
            cpu_percent.append(process.cpu_percent(interval=None))  # non-blocking
            mem_usage.append(process.memory_info().rss)
            end_timing.append(time.perf_counter() - start_timing)
            time.sleep(sleep_time)

    end = time.perf_counter() - num_messages*(sleep_time)
    duration_secs_sleep = sum(end_timing) + num_messages*(sleep_time)
    duration = end - start
    duration_loop = sum(end_timing)
    bandwidth_gb = (total_size / (1024**3)) / duration if duration > 0 else 0
    bandwidth_gb_loop = (total_size / (1024**3)) / duration_loop if duration_loop > 0 else 0
    bandwidth_gb_sleep= (total_size / (1024**3)) / duration_secs_sleep if duration_secs_sleep > 0 else 0

    start_closing = time.perf_counter()

    channel.close()
    connection.close()
    end_closing = time.perf_counter() - start_closing

    print(f"[Send {process_index}/{time.perf_counter()}] Sent {num_messages} messages to {exchange}/{rkey} "
            f"({format_bytes(total_size)}) in {duration:.4f}s Pure ({bandwidth_gb_loop:.6f} GB/s | With sleep {bandwidth_gb_sleep:.6f} GB/s ) (closing connection in {end_closing:.2f} secs)")

    # Save monitoring stats to shared dictionary
    stats_dict[process_index] = {
        "messages_sent": num_messages,
        "total_bytes": total_size,
        "duration_secs": duration_loop,
        "duration_secs_sleep": duration_secs_sleep,
        "bandwidth_gbs": bandwidth_gb_loop,
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
    parser.add_argument('--routing-per-rank', action="store_true", help='Generate a unique routing key per process (cannot be used with -topic)')

    args = parser.parse_args()

    try:
        message_size = parse_size_string(args.size)
    except ValueError as e:
        print(f"Error parsing --size: {e}")
        sys.exit(1)
    
    if args.nmsgs <= 0:
        print(f"Error parsing --nmsgs: {args.nmsgs} cannot be <= 0")
        sys.exit(1)

    if args.routing_per_rank and args.use_topic:
        print(f"Error parsing --routing-per-rank cannot be used with --use-topic/-topic")
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
        
        routing_key = args.routing_key
        if args.routing_per_rank:
            routing_key = f"{args.routing_key}.{i}"

        # time.sleep(random)

        proc = Process(
            target = send_messages_process,
            args = (
                config, cacert, args.exchange, exchange_type, routing_key,
                count, message_size, args.verbose, args.sleep, i, stats_dict)
            )
        processes.append(proc)
        proc.start()

    for proc in processes:
        proc.join()

    bandwidth_gb = (sum(stat["total_bytes"] for stat in stats_dict.values())/(1024**3)) / max(stat["duration_secs"] for stat in stats_dict.values())
    bandwidth_gb_sleep = (sum(stat["total_bytes"] for stat in stats_dict.values())/(1024**3)) / max(stat["duration_secs_sleep"] for stat in stats_dict.values())

    unique_id = args.unique_id or calendar.timegm(time.gmtime())

    queue_opts = "queue_multi" if args.routing_per_rank else "queue_one"
    used_exchange = f"exchange_{exchange_type}" if args.exchange !='' else queue_opts

    msgs_sent = list(stat["messages_sent"] for stat in stats_dict.values())
    total_message_sent = sum(msgs_sent)
    agg_bandwidth_msgs = total_message_sent / max(stat["duration_secs"] for stat in stats_dict.values())
    agg_bandwidth_msgs_sleep = total_message_sent / max(stat["duration_secs_sleep"] for stat in stats_dict.values())
    
    print(f"[Send/{time.perf_counter()}] Sent {total_message_sent} messages with {args.processes} processes: agg. bandwidth {bandwidth_gb:.6f} GB/s | Agg. with sleep {bandwidth_gb_sleep:.6f} GB/s")

    print(f"[Send/{time.perf_counter()}] Total number of messages sent: {total_message_sent} | {agg_bandwidth_msgs} msg/s | With sleep {agg_bandwidth_msgs_sleep} msg/s | min={min(msgs_sent)} max={max(msgs_sent)}")

    if args.monitoring is not None:
        hostname = socket.gethostname()
        combined = {
            "exp_id": str(unique_id),
            "total_messages": total_message_sent,
            "num_processes": args.processes,
            "hostname": hostname,
            "used_exchange": used_exchange,
            "sleep_time_secs": args.sleep,
            "messages_per_proc": messages_per_proc,
            "message_size": args.size,
            "message_count": args.nmsgs,
            "total_bytes": sum(stat["total_bytes"] for stat in stats_dict.values()),
            "avg_bandwidth_gbs": np.mean(list(stat["bandwidth_gbs"] for stat in stats_dict.values())),
            "agg_bandwidth_gbs": bandwidth_gb,
            "agg_bandwidth_gbs_sleep": bandwidth_gb_sleep,
            "agg_bandwidth_msgs": agg_bandwidth_msgs,
            "agg_bandwidth_msgs_sleep": agg_bandwidth_msgs_sleep,
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
            print(f"[Send/{time.perf_counter()}] Monitoring data written to {args.monitoring}")

if __name__ == '__main__':
    main()
