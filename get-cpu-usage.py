#!/usr/bin/env python3
# Copyright 2021-2023 Lawrence Livermore National Security, LLC and other
# AMSLib Project Developers
#
# SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

import csv
import os
import socket
import time
import argparse
import subprocess
import sys
from datetime import datetime

import psutil

def run_command(command):
    try:
        # Run the command and capture the output
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.stderr != '':
            print(f"Error: {command} => {result.stderr}", file=sys.stderr)
        return result.stdout.rstrip()

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        return None

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


def get_process_by_name(name):
    for proc in psutil.process_iter(['name', 'pid']):
        if proc.info['name'] == name:
            return proc
    return None

def monitor_cpu_usage(process_name, interval, duration, output_file, podman_id):
    proc = get_process_by_name(process_name)
    if not proc:
        print(f"No process found with name '{process_name}'")
        return

    file_exists = os.path.isfile(output_file)

    if file_exists:
        print(f"Warning: '{output_file}' already exists. Data will be appended.")
    else:
        print(f"Creating new file '{output_file}' and writing header.")

    # Prime CPU usage reading
    proc.cpu_percent(interval = None)
    proc.memory_full_info()
    hostname = socket.gethostname()

    with open(output_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['Timestamp', 'Hostname', 'CPU Usage (%)', 'VSS (byte)', 'Virtual Memory (byte)', 'Number of queues', 'Total msg in queues', 'Number of channels', 'Total unacknowledged msg in channels'])

        try:
            start_time = time.time()
            while time.time() - start_time < duration:
                cpu = proc.cpu_percent(interval = interval)
                mem = proc.memory_full_info()
                rss = format_bytes(mem.rss)
                vms = format_bytes(mem.vms)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S-%f')

                message_in_queues = 0
                number_of_queues = 0
                message_in_ch = 0
                number_of_ch = 0
                if podman_id:
                    # get queue messages unack
                    command = f"podman exec {podman_id} bash -c \"rabbitmq-diagnostics list_queues --online --no-table-headers --quiet --vhost rabbitmq\""

                    podman_request = run_command(command)
                    if podman_request != '' and podman_request is not None:
                        for line in podman_request.split('\n'):
                            elements = line.split('\t')
                            if elements[0] == "name" or elements[1] == "messages":
                                continue
                            number_of_queues += 1
                            queue_name = elements[0]
                            message_in_queues += int(elements[1])
                            
                    command = f"podman exec {podman_id} bash -c \"rabbitmq-diagnostics list_channels --no-table-headers --quiet consumer_count,messages_unacknowledged\""
                    podman_request = run_command(command)
                    if podman_request != '' and podman_request is not None:
                        for line in podman_request.split('\n'):
                            elements = line.split('\t')
                            number_of_ch += 1
                            message_in_ch += int(elements[1])

                writer.writerow([timestamp, hostname, cpu, mem.rss, mem.vms, number_of_queues, message_in_queues])
                print(f"[{timestamp}] CPU Usage: {cpu:8.2f}% | RSS {rss} | VIRT MEM {vms} | #Queues {number_of_queues} | Messages in queues {message_in_queues} | #Channels {number_of_ch} | Unack Messages in channel {message_in_ch}")
            
        except KeyboardInterrupt as e:
            print("")
            return

def main():
    parser = argparse.ArgumentParser(description="Monitor CPU usage / RSS / VMS of a process and log to CSV.")
    parser.add_argument('--process', '-p', type=str, required=True, help='Name of the process to monitor (e.g. python.exe)')
    parser.add_argument('--interval', '-i', type=float, required=False, default=1.0, help='Interval in seconds between CPU usage checks')
    parser.add_argument('--duration', '-d', type=float, required=True, help='Total duration in seconds to monitor')
    parser.add_argument('--output', '-o', type=str, required=True, help='CSV file to write CPU usage data')
    parser.add_argument('--podman-id', '-cid', type=str, required=False, help='ID of the container running RabbitMQ')

    args = parser.parse_args()
    monitor_cpu_usage(args.process, args.interval, args.duration, args.output, args.podman_id)

if __name__ == "__main__":
    main()