#!/usr/bin/env python3
# Author: Loic Pottier  <pottier1@llnl.gov>

import argparse
import calendar
import csv
import json
import os
import time
import socket
import ssl
import sys

from multiprocessing import Process, Manager

import numpy as np
import pika
import psutil

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Config file '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, 'r') as f:
        return json.load(f)

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

def consume_worker(config, cacert, exchange, exchange_type, queue, binding_keys, process_index, stats_dict, max_messages, timeout, qos, verbose):
    proc = psutil.Process(os.getpid())
    connection = create_connection(config, cacert)
    channel = connection.channel()

    if queue == '' and exchange != '':
        channel.exchange_declare(exchange = exchange, exchange_type = exchange_type, auto_delete = False)
        if verbose:
            print(f"[Recv {process_index}] Declared exchange '{exchange}' with type {exchange_type}")

    result = channel.queue_declare(queue = queue, exclusive = False, durable = False, auto_delete = True)
    queue_name = result.method.queue
    if verbose:
        print(f"[Recv {process_index}] Declared queue '{queue_name}'")

    binding_keys = list(binding_keys)

    if exchange != '':
        for binding_key in binding_keys:
            channel.queue_bind(exchange = exchange, queue = queue_name, routing_key = binding_key)
            if verbose:
                print(f"[Recv {process_index}] Bound queue '{queue_name}' to exchange '{exchange}' with routing key '{binding_key}'")

    stats = {
        "messages_received": 0,
        "start_time": time.perf_counter(),
        "qos": qos,
        "end_time": None
    }

    channel.basic_qos(prefetch_count = qos)

    cpu_percent = []
    mem_usage = []
    peak_rss = 0

    start_general = None

    print_nmsgs = f"{max_messages}" if max_messages else ""
    nmsgs = int(max_messages) if max_messages else None

    if verbose:
        if timeout:
            print(f"[Recv {process_index}] Waiting for {print_nmsgs} messages on queue '{queue_name}' with {timeout} seconds timout. Press Ctrl+C to exit.")
        else:
            print(f"[Recv {process_index}] Waiting for {print_nmsgs} messages on queue '{queue_name}', no timeout. Press Ctrl+C to exit.")

    def callback(ch, method, properties, body):
        nonlocal peak_rss, start_general
        if start_general is None:
            start_general = time.perf_counter()
        stats["messages_received"] += 1
        start = time.perf_counter()
        
        #NOTE: This line drops the bandwidth by ~30x
        #ch.basic_ack(delivery_tag = method.delivery_tag)

        data = np.frombuffer(body, dtype=np.uint8)
        end = time.perf_counter()

        # Monitoring
        cpu_percent.append(proc.cpu_percent(interval=None))
        current_rss = proc.memory_info().rss
        mem_usage.append(current_rss)
        if current_rss > peak_rss:
            peak_rss = current_rss

        return end - start, data.nbytes

    end = None
    if verbose and max_messages:
        print(f"[Recv {process_index}] Waiting for {nmsgs} messages")
    elif verbose:
        print(f"[Recv {process_index}] Waiting for unlimited messages")
    try:
        total_duration = 0
        total_size = 0
        message_consumed = 0

        # # Get X messages and break out
        for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout = timeout):
            if (method_frame, properties, body) == (None, None, None):
                end = time.perf_counter()
                closing_time = time.perf_counter()
                print(f"[Recv {process_index}] Timed out after {timeout} seconds (messages received: {message_consumed})")
                requeued_messages = channel.cancel()
                if requeued_messages > 0 and verbose:
                    print(f"[Recv {process_index}] requeued {requeued_messages} messages")
                # Close the channel and the connection
                channel.stop_consuming()
                channel.close()

                closing_time = time.perf_counter() - closing_time
                print(f"[Recv {process_index}] Closing channel time {closing_time} secs")
                break

            duration, dsize = callback(channel, method_frame, properties, body)
            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

            if verbose and (message_consumed % 500 == 0) and message_consumed > 0:
                print(f"[Recv {process_index}] Received {message_consumed} messages")
            total_duration += duration
            total_size += dsize

            message_consumed += 1
            # if n_msgs is None, consume for ever
            if message_consumed == nmsgs:
                end = time.perf_counter()
                print(f"[Recv {process_index}] Consumed {message_consumed} messages. Stopping")
                closing_time = time.perf_counter()
                # Cancel the consumer and return any pending messages
                requeued_messages = channel.cancel()
                if requeued_messages > 0 and verbose:
                    print(f"[Recv {process_index}] requeued {requeued_messages} messages")
                # Close the channel and the connection
                channel.stop_consuming()
                channel.close()
                closing_time = time.perf_counter() - closing_time
                if verbose:
                    print(f"[Recv {process_index}] Closing channel time {closing_time} secs")
                break

    except KeyboardInterrupt:
        print(f"[Recv {process_index}] Interrupted. Closing connection...")
        print("")
        end = time.perf_counter()
        closing_time = time.perf_counter()
        requeued_messages = channel.cancel()
        if requeued_messages > 0 and verbose:
            print(f"[Recv {process_index}] requeued {requeued_messages} messages")
        # Close the channel and the connection
        channel.close()
        closing_time = time.perf_counter() - closing_time
        print(f"[Recv {process_index}] Closing channel time {closing_time} secs")
    finally:
        start_closing_conn = time.perf_counter()
        connection.close()

        end = end or time.perf_counter()
        stats["end_time"] = end
        stats["avg_cpu_percent"] = sum(cpu_percent) / len(cpu_percent) if cpu_percent else 0
        stats["avg_memory_bytes"] = sum(mem_usage) / len(mem_usage) if mem_usage else 0
        stats["peak_memory_bytes"] = peak_rss

        cpu_times = proc.cpu_times()
        stats["cpu_time_user_secs"] = cpu_times.user
        stats["cpu_time_system_secs"] = cpu_times.system
        
        bandwidth_gb = (total_size/1e9) / total_duration if total_duration > 0 else 0

        if start_general is None:
            real_duration = -1
        else:
            real_duration = end - start_general

        stats["total_size_gb"] = total_size / 1e9
        stats["real_duration_secs"] = real_duration
        stats["bandwidth_gbs"] = bandwidth_gb
        stats["bandwidth_gbs_wait"] = (total_size/1e9) / real_duration if real_duration > 0 else 0
        stats["wait_time_percent"] = 100 * (real_duration - total_duration) / real_duration

        print(stats)

        stats_dict[process_index] = stats
        end_closing_conn = time.perf_counter() - start_closing_conn
        if verbose:
            print(f"[Recv {process_index}] Done (Closing done in {end_closing_conn} sec)")

def main():
    parser = argparse.ArgumentParser(description='Multiprocess RabbitMQ consumer that uses topic exchange.')
    parser.add_argument('--config', required=True, help='RabbitMQ config file')
    parser.add_argument('--queue', '-q', required=False, default='', help='Queue name to consume from')
    parser.add_argument('--processes', '-p', type=int, default=1, help='Number of consumer processes')
    parser.add_argument('--timeout', '-t', required=False, default=None, type=int, help='Timeout in seconds (default: None)')
    parser.add_argument('--nmsgs', '-n', required=False, type=int, default=None, help='Number of messages to expect per process')
    parser.add_argument('--exchange', '-e', required=False, default='', help='Exchange name to send messages to.')
    parser.add_argument('--routing-key', '-r', required=False, help='Routing key to receive messages from.')
    parser.add_argument('--verbose', '-v', action="store_true", help='Verbose output')
    parser.add_argument('--monitoring', '-m', required=False, help='Write monitoring output CSV')
    parser.add_argument('--qos', default=0, help='QOS level')
    parser.add_argument('--unique-id', '-id', required=False, help='Unique ID for CSV')
    parser.add_argument('--strong-scaling', action="store_true", help='Number of messages is divided by number of processes')
    parser.add_argument('--sender-process', '-sp', type=int, required=True, help='The number of processes used by the sender')

    args = parser.parse_args()

    config = load_config(args.config)
    cacert = config.get("rabbitmq-cert")
    manager = Manager()
    stats_dict = manager.dict()

    if args.queue == '' and args.exchange == '':
        print(f"Error: either queue or exchange must be provided")
        return

    if args.queue != '' and args.exchange != '':
        print(f"Error: queue and exchange connot be both provided")
        return

    if args.exchange != '' and args.routing_key == '':
        print(f"Error: if exchange is provided then routing key cannot be null")
        return

    # Distribute messages evenly
    messages_per_proc = None
    if args.nmsgs:
        messages_per_proc = args.nmsgs // args.processes
        extra = args.nmsgs % args.processes
        processes = []
        for i in range(args.processes):
            if args.strong_scaling:
                count = messages_per_proc + (1 if i < extra else 0)
            else:
                count = args.nmsgs
                messages_per_proc = args.nmsgs

    exchange_type = "topic"

    binding_keys = [f"{args.routing_key}.{i}" for i in range(args.sender_process)]

    if args.sender_process % args.processes != 0:
        print(f"Warning: --sender-process must divide --processes")
        sys.exit(-1)

    keys_per_process = args.sender_process // args.processes
    if keys_per_process == 0:
        binding_keys = binding_keys * args.processes
        binding_keys = [[x] for x in binding_keys]
    else:
        binding_keys = [binding_keys[i:i + keys_per_process] for i in range(0, len(binding_keys), keys_per_process)]

    processes = []
    for i in range(args.processes):
        proc = Process(target=consume_worker,
                       args=(config, cacert, args.exchange, exchange_type, args.queue, binding_keys[i], i, stats_dict, messages_per_proc, args.timeout, args.qos, args.verbose))
        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()
    
    min_wait_time = max_wait_time = 0
    if len(stats_dict) == 0:
        print(f"Warning: No stats collected")
        return
    
    min_wait_time = min(stat["wait_time_percent"] for stat in stats_dict.values())
    max_wait_time = max(stat["wait_time_percent"] for stat in stats_dict.values())

    wait_time = [min_wait_time, max_wait_time]
    bandwidth_gbs = sum(stat["bandwidth_gbs"] for stat in stats_dict.values())
    bandwidth_gbs_wait = sum(stat["bandwidth_gbs_wait"] for stat in stats_dict.values())

    msgs_received = list(stat["messages_received"] for stat in stats_dict.values())
    total_messages = sum(msgs_received)
    print(f"Total number of messages received: {total_messages} | min={min(msgs_received)} max={max(msgs_received)}")
    print(f"No-wait agg. bandwith {bandwidth_gbs} GB/s | Wait agg. bandwidth {bandwidth_gbs_wait} GB/s ({max_wait_time} %)")

    unique_id = args.unique_id or calendar.timegm(time.gmtime())
    used_exchange = f"exchange_{exchange_type}"

    if args.monitoring is not None:
        hostname = socket.gethostname()
        total = {
            "exp_id": str(unique_id),
            "total_messages": total_messages,
            "nmsgs": args.nmsgs,
            "num_processes": args.processes,
            "qos": args.qos,
            "hostname": hostname,
            "used_exchange": used_exchange,
            "total_runtime_secs": max(stat["end_time"] for stat in stats_dict.values()) - 
                                min(stat["start_time"] for stat in stats_dict.values()),
            "avg_cpu_percent": sum(stat["avg_cpu_percent"] for stat in stats_dict.values()) / len(stats_dict),
            "avg_memory_bytes": sum(stat["avg_memory_bytes"] for stat in stats_dict.values()) / len(stats_dict),
            "peak_memory_bytes": max(stat["peak_memory_bytes"] for stat in stats_dict.values()),
            "total_cpu_time_user_secs": sum(stat["cpu_time_user_secs"] for stat in stats_dict.values()),
            "real_duration_secs": max(stat["real_duration_secs"] for stat in stats_dict.values()),
            "bandwidth_gbs": sum(stat["bandwidth_gbs"] for stat in stats_dict.values()),
            "bandwidth_gbs_wait": sum(stat["bandwidth_gbs_wait"] for stat in stats_dict.values()),
            "total_size_gb": sum(stat["total_size_gb"] for stat in stats_dict.values()),
            "wait_time_percent": max_wait_time,
            # "process_stats": dict(stats_dict)
        }
        # with open(args.monitoring, "w") as f:
        #     json.dump(total, f, indent = 2)

        file_exists = os.path.isfile(args.monitoring)

        with open(args.monitoring, "a") as csv_file:
            writer = csv.writer(csv_file)
            if not file_exists:
                writer.writerow([key for key in total])
            writer.writerow([val for key,val in total.items()])

        if args.verbose:
            print(f"Monitoring data written to {args.monitoring}")

if __name__ == '__main__':
    main()
