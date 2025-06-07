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
from datetime import datetime

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


def get_process_by_name(name):
    for proc in psutil.process_iter(['name', 'pid']):
        if proc.info['name'] == name:
            return proc
    return None

def monitor_cpu_usage(process_name, interval, duration, output_file):
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
            writer.writerow(['Timestamp', 'Hostname', 'CPU Usage (%)', 'VSS (byte)', 'Virtual Memory (byte)'])

        try:

            start_time = time.time()
            while time.time() - start_time < duration:
                cpu = proc.cpu_percent(interval = interval)
                mem = proc.memory_full_info()
                rss = format_bytes(mem.rss)
                vms = format_bytes(mem.vms)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S-%f')

                writer.writerow([timestamp, hostname, cpu, mem.rss, mem.vms])
                print(f"[{timestamp}] CPU Usage: {cpu:8.2f}% | RSS {rss} | VIRT MEM {vms}")
            
        except KeyboardInterrupt as e:
            print("")
            return

def main():
    parser = argparse.ArgumentParser(description="Monitor CPU usage / RSS / VMS of a process and log to CSV.")
    parser.add_argument('--process', '-p', type=str, required=True, help='Name of the process to monitor (e.g. python.exe)')
    parser.add_argument('--interval', '-i', type=float, required=False, default=1.0, help='Interval in seconds between CPU usage checks')
    parser.add_argument('--duration', '-d', type=float, required=True, help='Total duration in seconds to monitor')
    parser.add_argument('--output', '-o', type=str, required=True, help='CSV file to write CPU usage data')

    args = parser.parse_args()
    monitor_cpu_usage(args.process, args.interval, args.duration, args.output)

if __name__ == "__main__":
    main()