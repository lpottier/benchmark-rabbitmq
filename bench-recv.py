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
        ssl_options=ssl_options,
        heartbeat=0
    )

    return pika.BlockingConnection(parameters)

def consume_worker(config, cacert, exchange, queue, routing_key, process_index, stats_dict, max_messages, timeout, qos, epsilon_percent, sleep_sender, verbose):
    proc = psutil.Process(os.getpid())
    
    start_init = time.perf_counter()
    connection = create_connection(config, cacert)
    channel = connection.channel()

    if queue == '' and exchange != '':
        channel.exchange_declare(exchange = exchange, exchange_type = 'direct', auto_delete = False)
        if verbose:
            print(f"[Recv {process_index}/{time.perf_counter()}] Declared exchange '{exchange}'")

    result = channel.queue_declare(queue = queue, exclusive = False, durable = False, auto_delete = False)
    queue_name = result.method.queue
    if verbose:
        print(f"[Recv {process_index}/{time.perf_counter()}] Declared queue '{queue_name}'")

    if exchange != '':
        if not isinstance(routing_key, list):
            routing_key = [routing_key]

        for key in routing_key:
            channel.queue_bind(exchange = exchange, queue = queue_name, routing_key = key)
            if verbose:
                print(f"[Recv {process_index}/{time.perf_counter()}] Bound queue '{queue_name}' to exchange '{exchange}' with routing key '{key}'")

    start_ts = time.perf_counter()
    stats = {
        "messages_received": 0,
        "start_time": start_ts,
        "last_received_msg": start_ts,
        "qos": qos,
        "end_time": None
    }

    if verbose:
        print(f"[Recv {process_index}/{time.perf_counter()}] Setting qos = {qos}")
    channel.basic_qos(prefetch_count = qos)

    cpu_percent = []
    mem_usage = []
    peak_rss = 0

    start_general = None

    print_nmsgs = f"{max_messages}" if max_messages else ""
    nmsgs = int(max_messages) if max_messages else None

    if verbose:
        if timeout:
            print(f"[Recv {process_index}/{time.perf_counter()}] Waiting for {print_nmsgs} messages on queue '{queue_name}' with {timeout} seconds timout. Press Ctrl+C to exit.")
        else:
            print(f"[Recv {process_index}/{time.perf_counter()}] Waiting for {print_nmsgs} messages on queue '{queue_name}', no timeout. Press Ctrl+C to exit.")

    def callback(ch, method, properties, body):
        nonlocal peak_rss, start_general
        if start_general is None:
            start_general = time.perf_counter()
        stats["messages_received"] += 1
        start_cb = time.perf_counter()

        #NOTE: This line drops the bandwidth by ~30x
        # ch.basic_ack(delivery_tag = method.delivery_tag)

        data = np.frombuffer(body, dtype=np.uint8)
        end_cb = time.perf_counter()

        # Monitoring
        cpu_percent.append(proc.cpu_percent(interval=None))
        current_rss = proc.memory_info().rss
        mem_usage.append(current_rss)
        if current_rss > peak_rss:
            peak_rss = current_rss
        return end_cb - start_cb, data.nbytes

    relaxed_bounds = round(nmsgs * (1-(epsilon_percent/100.0)))
    interval_msgs = [max(0, nmsgs-relaxed_bounds), nmsgs+relaxed_bounds]

    end = None
    end_init = time.perf_counter() - start_init

    print(f"[Recv {process_index}/{time.perf_counter()}] Waiting for {nmsgs} messages (initialization done in {end_init})", flush=True)

    try:
        callback_duration = 0
        total_size = 0
        message_consumed = 0
        # Get X messages and break out
        for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout = timeout):
            if (method_frame, properties, body) == (None, None, None):
                end = time.perf_counter()
                closing_time = time.perf_counter()
                print(f"[Recv {process_index}/{time.perf_counter()}] Timed out after {timeout} seconds (messages received: {message_consumed})")
                try:
                    requeued_messages = channel.cancel()
                except Exception as e:
                    if verbose:
                        print(f"[Recv {process_index}/{time.perf_counter()}] {e}")
                    requeued_messages = 0
                if requeued_messages > 0 and verbose:
                    print(f"[Recv {process_index}/{time.perf_counter()}] requeued {requeued_messages} messages")
                # Close the channel and the connection
                channel.stop_consuming()
                try:
                    channel.close()
                except Exception as e:
                    if verbose:
                        print(f"[Recv {process_index}/{time.perf_counter()}] {e}")
                    pass
                closing_time = time.perf_counter() - closing_time
                print(f"[Recv {process_index}/{time.perf_counter()}] Closing channel time {closing_time} secs")
                break

            duration, dsize = callback(channel, method_frame, properties, body)

            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

            if verbose and (message_consumed % 500 == 0) and message_consumed > 0:
                print(f"[Recv {process_index}/{time.perf_counter()}] Received {message_consumed} messages")
            callback_duration += duration
            total_size += dsize

            message_consumed += 1
            stats["last_received_msg"] = time.perf_counter()
            # if n_msgs is None, consume for ever
            if message_consumed >= interval_msgs[0] and message_consumed <= interval_msgs[1]:
                end = time.perf_counter()
                print(f"[Recv {process_index}/{time.perf_counter()}] Consumed {message_consumed} messages. Stopping")
                closing_time = time.perf_counter()
                # Cancel the consumer and return any pending messages
                try:
                    requeued_messages = channel.cancel()
                except Exception as e:
                    print(f"[Recv {process_index}/{time.perf_counter()}] {e}")
                    requeued_messages = 0
                if requeued_messages > 0 and verbose:
                    print(f"[Recv {process_index}/{time.perf_counter()}] requeued {requeued_messages} messages")
                # Close the channel and the connection
                channel.stop_consuming()
                try:
                    channel.close()
                except Exception as e:
                    if verbose:
                        print(f"[Recv {process_index}/{time.perf_counter()}] {e}")
                    pass
                closing_time = time.perf_counter() - closing_time
                if verbose:
                    print(f"[Recv {process_index}/{time.perf_counter()}] Closing channel time {closing_time} secs")
                break
    except KeyboardInterrupt:
        print(f"[Recv {process_index}/{time.perf_counter()}] Interrupted. Closing connection...")
        print("")
        end = time.perf_counter()
        closing_time = time.perf_counter()
        try:
            requeued_messages = channel.cancel()
        except Exception as e:
            print(f"[Recv {process_index}] {e}")
            requeued_messages = 0
        if requeued_messages > 0 and verbose:
            print(f"[Recv {process_index}/{time.perf_counter()}] requeued {requeued_messages} messages")
        # Close the channel and the connection
        channel.stop_consuming()
        try:
            channel.close()
        except Exception as e:
            if verbose:
                print(f"[Recv {process_index}/{time.perf_counter()}] {e}")
            pass
        closing_time = time.perf_counter() - closing_time
        print(f"[Recv {process_index}/{time.perf_counter()}] Closing channel time {closing_time} secs")
    finally:
        start_closing_conn = time.perf_counter()
        try:
            connection.close()
        except pika.exceptions.ConnectionWrongStateError as e:
            pass

        if end is None:
            end = time.perf_counter()
        stats["end_time"] = end
        stats["avg_cpu_percent"] = sum(cpu_percent) / len(cpu_percent) if cpu_percent else 0
        stats["avg_memory_bytes"] = sum(mem_usage) / len(mem_usage) if mem_usage else 0
        stats["peak_memory_bytes"] = peak_rss

        cpu_times = proc.cpu_times()
        stats["cpu_time_user_secs"] = cpu_times.user
        stats["cpu_time_system_secs"] = cpu_times.system

        if start_general is None:
            real_duration = -1
        else:
            real_duration = stats["last_received_msg"] - start_general - sleep_sender

        total_size_gb = total_size / (1024**3)
        bandwidth_gb_cb = (total_size_gb / callback_duration) if callback_duration > 0 else 0
        bandwidth_gb_wait = total_size_gb / real_duration if real_duration > 0 else 0
        bandwidth_gb_sleep = total_size_gb / (real_duration+sleep_sender) if real_duration > 0 else 0

        # bandwidth_gb2 = total_size_gb / (stats["last_received_msg"] - start_general) if (stats["last_received_msg"] - start_general) > 0 else 0
        # a = stats["last_received_msg"] - start_general
        # print(f"Difference between: end and last_received_msg = {bandwidth_gb} {bandwidth_gb2} | {total_size_gb} | {real_duration} {a}")

        stats["total_size_gb"] = total_size_gb
        stats["real_duration_secs"] = real_duration
        stats["real_duration_secs_sleep"] = real_duration+sleep_sender
        stats["callback_duration_secs"] = callback_duration
        stats["bandwidth_gbs"] = bandwidth_gb_cb
        stats["bandwidth_gbs_sleep"] = bandwidth_gb_sleep
        stats["bandwidth_gbs_wait"] = bandwidth_gb_wait
        stats["wait_time_percent"] = 100 * (real_duration - callback_duration) / real_duration
        stats["wait_time_percent_sleep"] = 100 * ((real_duration+sleep_sender) - callback_duration) / (real_duration+sleep_sender)

        stats_dict[process_index] = stats
        end_closing_conn = time.perf_counter() - start_closing_conn
        print(f"[Recv {process_index}/{time.perf_counter()}] Done (Closing done in {end_closing_conn:.2f} sec / {message_consumed} messages consumed)")

def slice_list(array, n, prefix=''):
    new_data = [x.tolist() for x in np.array_split(array, n)]
    if prefix == '':
        return new_data

    new_data_prefix = []
    for sub_list in new_data:
        new_data_prefix.append([f"{prefix}.{x}" for x in sub_list])
    return new_data_prefix

def main():
    parser = argparse.ArgumentParser(description='Multiprocess RabbitMQ consumer.')
    parser.add_argument('--config', required=True, help='RabbitMQ config file')
    parser.add_argument('--queue', '-q', required=False, default='', help='Queue name to consume from')
    parser.add_argument('--processes', '-p', type=int, default=1, help='Number of consumer processes')
    parser.add_argument('--timeout', '-t', required=False, default=None, type=int, help='Timeout in seconds (default: None)')
    parser.add_argument('--nmsgs', '-n', required=False, type=int, default=None, help='Number of messages to expect per process')
    parser.add_argument('--exchange', '-e', required=False, default='', help='Exchange name to send messages to.')
    parser.add_argument('--routing-key', '-r', required=False, help='Routing key to receive messages from.')
    parser.add_argument('--verbose', '-v', action="store_true", help='Verbose output')
    parser.add_argument('--monitoring', '-m', required=False, help='Write monitoring output CSV')
    parser.add_argument('--qos', default=0, type=int, help='QOS level')
    parser.add_argument('--unique-id', '-id', required=False, help='Unique ID for CSV')
    parser.add_argument('--strong-scaling', action="store_true", help='Number of messages is divided by number of processes')
    parser.add_argument('--routing-per-rank', action="store_true", help='Expect a unique routing key per process (cannot be used with -topic)')
    parser.add_argument('--yappi', action="store_true", help='use yappi to profile the code')
    parser.add_argument('--epsilon-percent', default=100, type=float, help='Consumer will stop when the number of messages "nmsgs * epsilon-percent" is received')
    parser.add_argument('--sender-process', default=None, type=int, required=False, help='Number of senders processes')
    parser.add_argument('--sleep-sender', default=0, type=float, required=False, help='Time in seconds each sender wait (wait_time x nmsgs_sent). This time will be subtracted from the duration used to compute various metrics')

    args = parser.parse_args()

    start_init = time.perf_counter()

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

    if args.routing_per_rank and args.routing_key == '':
        print(f"Error: if --routing-per-rank is provided then --routing-key must be provided")
        return

    if args.epsilon_percent > 100 or args.epsilon_percent <= 0:
        print(f"Error: --epsilon-percent must be between 0 and 100")
        return
    
    if args.sender_process is None and not args.routing_per_rank:
        print(f"Error: --sender-process must be used with --routing-per-rank")
        return

    if args.yappi:
        import yappi
        yappi.start()

    end_init = time.perf_counter() - start_init
    print(f"[Recv/{time.perf_counter()}] Initialization: {end_init} sec")

    # If the number of senders is != of the number of consumers we must group bindings
    # For example: with 5 producers and 2 consumers:
    #    First consumer will have routing key [key.0, key.1, key.2]
    #    Second consumer will have routing key [key.3, key.4]
    if args.sender_process:
        binding_keys = slice_list(list(range(args.sender_process)), args.processes, args.routing_key)

    # Distribute messages evenly
    messages_per_proc = None
    messages_per_procs = []

    sleep_sender = args.sleep_sender * args.nmsgs
    if sleep_sender > 0:
        print(f"Sleep time per receiver = {sleep_sender} secs")

    if args.sender_process and args.nmsgs:
        # Weak scaling case
        args.nmsgs = args.nmsgs * args.sender_process
        print(f"Weak scaling case: total nmsgs: {args.nmsgs}")

    if args.nmsgs:
        messages_per_proc = args.nmsgs // args.processes
        extra = args.nmsgs % args.processes
        processes = []
        for i in range(args.processes):
            if args.strong_scaling or args.sender_process:
                messages_per_procs.append(messages_per_proc + (1 if i < extra else 0))
            else:
                messages_per_procs.append(args.nmsgs)

    # print(f"messages_per_procs = {messages_per_procs}")
    # if args.sender_process:
    #     if sum(messages_per_procs) != args.nmsgs:
    #         print(f"Weird = {sum(messages_per_procs)} {args.nmsgs}")

    processes = []
    for i in range(args.processes):
        routing_key = args.routing_key
        queue = args.queue
        if args.routing_per_rank and args.sender_process is None:
            routing_key = f"{args.routing_key}.{i}"
        elif args.routing_per_rank and args.sender_process:
            routing_key = binding_keys[i]
        
        if args.queue and args.routing_per_rank:
            queue = f"{args.queue}.{i}"

        proc = Process(target=consume_worker,
                       args=(config, cacert, args.exchange, queue, routing_key, i, stats_dict, messages_per_procs[i], args.timeout, args.qos, args.epsilon_percent, sleep_sender, args.verbose))
        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()
    
    if args.yappi:
        yappi.stop()
        yappi.get_func_stats().print_all()
        print("========")
        yappi.get_thread_stats().print_all()
    
    min_wait_time = max_wait_time = 0
    if len(stats_dict) == 0:
        print(f"[Recv/{time.perf_counter()}] Warning: No stats collected")
        return

    min_wait_time = min(stat["wait_time_percent"] for stat in stats_dict.values())
    max_wait_time = max(stat["wait_time_percent"] for stat in stats_dict.values())

    wait_time = [min_wait_time, max_wait_time]


    bandwidth_gbs = sum(stat["total_size_gb"] for stat in stats_dict.values()) / max(stat["callback_duration_secs"] for stat in stats_dict.values())
    bandwidth_gbs_wait = sum(stat["total_size_gb"] for stat in stats_dict.values()) / max(stat["real_duration_secs"] for stat in stats_dict.values())
    bandwidth_gbs_sleep = sum(stat["total_size_gb"] for stat in stats_dict.values()) / max(stat["real_duration_secs_sleep"] for stat in stats_dict.values())

    msgs_received = list(stat["messages_received"] for stat in stats_dict.values())
    total_messages = sum(msgs_received)

    print(f"[Recv/{time.perf_counter()}] Total number of messages received: {total_messages} | min={min(msgs_received)} max={max(msgs_received)}")
    print(f"[Recv/{time.perf_counter()}] CB agg. bandwith {bandwidth_gbs:.6f} GB/s | Wait agg. bandwidth {bandwidth_gbs_wait:.6f} GB/s | With Sleep agg. bandwidth {bandwidth_gbs_sleep:.6f} GB/s ([{min_wait_time:.6f}, {max_wait_time:.6f}] %)")

    unique_id = args.unique_id or calendar.timegm(time.gmtime())

    queue_opts = "queue_multi" if args.routing_per_rank else "queue_one"
    used_exchange = "exchange_direct" if args.exchange != '' else queue_opts

    if args.monitoring is not None:
        hostname = socket.gethostname()
        total = {
            "exp_id": str(unique_id),
            "total_messages": sum(stat["messages_received"] for stat in stats_dict.values()),
            "nmsgs": args.nmsgs,
            "num_processes": args.processes,
            "sender_process": args.sender_process or -1,
            "qos": args.qos,
            "hostname": hostname,
            "used_exchange": used_exchange,
            "total_runtime_secs": max(stat["end_time"] for stat in stats_dict.values()) - 
                                min(stat["start_time"] for stat in stats_dict.values()),
            "avg_cpu_percent": sum(stat["avg_cpu_percent"] for stat in stats_dict.values()) / len(stats_dict),
            "avg_memory_bytes": sum(stat["avg_memory_bytes"] for stat in stats_dict.values()) / len(stats_dict),
            "peak_memory_bytes": max(stat["peak_memory_bytes"] for stat in stats_dict.values()),
            "total_cpu_time_user_secs": sum(stat["cpu_time_user_secs"] for stat in stats_dict.values()),
            "sleep_sender": args.sleep_sender,
            "real_duration_secs_max": max(stat["real_duration_secs"] for stat in stats_dict.values()),
            "real_duration_secs_min": min(stat["real_duration_secs"] for stat in stats_dict.values()),
            "bandwidth_gbs": bandwidth_gbs,
            "bandwidth_gbs_wait": bandwidth_gbs_wait,
            "bandwidth_gbs_sleep": bandwidth_gbs_sleep,
            "bandwidth_msgs": total_messages / max(stat["real_duration_secs"] for stat in stats_dict.values()),
            "bandwidth_msgs_sleep": total_messages / max(stat["real_duration_secs_sleep"] for stat in stats_dict.values()),
            "total_size_gb": sum(stat["total_size_gb"] for stat in stats_dict.values()),
            "wait_time_percent_min": min_wait_time,
            "wait_time_percent_max": max_wait_time,
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
            print(f"[Recv/{time.perf_counter()}] Monitoring data written to {args.monitoring}")

if __name__ == '__main__':
    main()
