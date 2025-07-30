#!/usr/bin/env python3
# Author: Loic Pottier  <pottier1@llnl.gov>

import argparse
import pandas as pd
import matplotlib.pyplot as plt

def parse_arguments():
    parser = argparse.ArgumentParser(description="Plot CPU usage and queue metrics from a CSV file.")
    parser.add_argument("csv_file", help="Path to the CSV file.")
    parser.add_argument("--output", "-o", help="Path to save the plot as PNG (optional).")
    return parser.parse_args()

def load_and_clean_data(csv_file):
    df = pd.read_csv(csv_file)

    df['Timestamp'] = df['Timestamp'].str.replace(r'-(\d{6})$', r'.\1', regex=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format="%Y-%m-%d %H:%M:%S.%f", errors='coerce')

    df.set_index("Timestamp", inplace=True)
    return df

def plot_data(df, metric_left = "CPU Usage (%)", metric_right = "Number of queues", output_path=None):
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Left y-axis
    ax1.plot(df.index, df[metric_left], color="blue", label=metric_left)
    ax1.set_xlabel("Time")
    ax1.set_ylabel(metric_left, color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")

    # Right y-axis
    ax2 = ax1.twinx()
    ax2.plot(df.index, df[metric_right], color="red", label=metric_right)
    ax2.set_ylabel(metric_right, color="red")
    ax2.tick_params(axis="y", labelcolor="red")

    plt.title(f"CPU Usage and {metric_right} over time")
    ax1.grid(True)
    fig.tight_layout()
#     plt.xticks(rotation=45)

    if output_path:
        plt.savefig(output_path)
        print(f"Plot saved to {output_path}")
    else:
        plt.show()

def main():
    args = parse_arguments()
    df = load_and_clean_data(args.csv_file)
    plot_data(
        df = df,
        metric_left = "CPU Usage (%)",
        metric_right = "Number of queues",
        # metric_right = "Total msg in queues"
        output_path = args.output
    )

if __name__ == "__main__":
    main()