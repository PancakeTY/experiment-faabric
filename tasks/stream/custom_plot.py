from invoke import task
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from tasks.util.stats import extract_data, extract_avg_tuple_duration_prefix

def shorten_number(x, pos):
    if x == "inf":
        return x
    if x >= 1000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'
    
@task
def state_access(ctx):
    wc_data = extract_data('tasks/stream/logs/exp_wc_latency.txt')
    sd_data = extract_data('tasks/stream/logs/exp_sd_latency.txt')
    mo_data = extract_data('tasks/stream/logs/exp_mo_latency.txt', "mo_alert")

    wc_df = pd.DataFrame(wc_data)
    wc_df['application'] = 'wc'

    sd_df = pd.DataFrame(sd_data)
    sd_df['application'] = 'sd'

    mo_df = pd.DataFrame(mo_data)
    mo_df['application'] = 'mo'

    # Concatenate all the DataFrames into one
    df = pd.concat([wc_df, sd_df, mo_df], ignore_index=True)

    # Create filtered DataFrame and add relevant columns
    df['Scale'] = df['Scale'].astype(int)  # Ensure 'Scale' column is integer


   # Process each application's data to calculate 'Average Tuple Duration (µs)'
    wc_filtered = df[df['application'] == 'wc'][['Functions','Scale']].copy()
    wc_filtered['Average Tuple Duration (µs)'] = wc_filtered['Functions'].apply(
        lambda x: extract_avg_tuple_duration_prefix(x, function_name_prefix="stream_wordcountindiv_count")
    )
    wc_filtered['application'] = 'wc'

    sd_filtered = df[df['application'] == 'sd'][['Functions','Scale']].copy()
    sd_filtered['Average Tuple Duration (µs)'] = sd_filtered['Functions'].apply(
        lambda x: extract_avg_tuple_duration_prefix(x, function_name_prefix="stream_sd_moving_avg")
    )
    sd_filtered['application'] = 'sd'

    mo_filtered = df[df['application'] == 'mo'][['Functions','Scale']].copy()
    mo_filtered['Average Tuple Duration (µs)'] = mo_filtered['Functions'].apply(
        lambda x: extract_avg_tuple_duration_prefix(x, function_name_prefix="stream_mo_anomaly")
    )
    mo_filtered['application'] = 'mo'

    # Concatenate all filtered DataFrames
    df_combined = pd.concat([wc_filtered, sd_filtered, mo_filtered], ignore_index=True)

    print(df_combined)
    # faasm_wc_data = extract_data('tasks/stream/logs/native_mo_latency_1node.txt', native = True)
    # faasm_wc_data = pd.DataFrame(faasm_wc_data)
    # faasm_wc_data['application'] = ['FAASM-wc-1']
    

@task
def latency_plot(ctx):
    # Updated Data Preparation
    data = {
        "Platform": [
            "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3",
            "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3",
            "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-1", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3", "FAASM-3",
            "ScalaSSC-1", "ScalaSSC-3", "ScalaSSC-1", "ScalaSSC-3", "ScalaSSC-1", "ScalaSSC-3"
        ],
        "Application": [
            "MO", "MO", "MO", "MO", "MO", "MO", "MO", "MO", "MO", "MO",
            "SD", "SD", "SD", "SD", "SD", "SD", "SD", "SD", "SD", "SD",
            "WC", "WC", "WC", "WC", "WC", "WC", "WC", "WC", "WC", "WC",
            "WC", "WC", "SD", "SD", "MO", "MO"
        ],
        "Latency": [
            2750, 2748, 2730, 3153, 2807, 240610, 243841, 225804, 242847, 234264,
            2357, 2088, 3743, 2557, 2075, 228199, 226368, 226382, 226140, 226644,
            842, 860, 841, 861, 844, 203086, 202183, 202167, 202202, 202282,
            20, 34, 43, 52, 51, 65
        ]
    }

    df = pd.DataFrame(data)

    # Aggregating data to average latencies for each platform-application pair
    avg_latency = df.groupby(["Application", "Platform"])["Latency"].mean().reset_index()

    # Plotting
    plt.figure(figsize=(10, 6))

    # Creating the bar plot
    applications = avg_latency["Application"].unique()
    platforms = avg_latency["Platform"].unique()
    width = 0.2  # Bar width

    # Generating bar positions for each platform within each application
    x = range(len(applications))  # Base x positions for applications

    for i, platform in enumerate(platforms):
        platform_data = avg_latency[avg_latency["Platform"] == platform]
        y_values = [platform_data[platform_data["Application"] == app]["Latency"].mean() if app in platform_data["Application"].values else 0 for app in applications]
        plt.bar(
            [pos + i * width for pos in x],  # Adjust x positions for each platform
            y_values,
            width=width,
            label=platform
        )

    # Setting up the plot
    plt.yscale('log')  # Use log scale for the y-axis
    plt.xlabel("Application")
    plt.ylabel("Average Tuple Processing Latency (µs) [Log Scale]")
    plt.xticks([pos + width for pos in x], applications)  # Adjust x-ticks to center
    plt.legend(title="Platform")
    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/latency_con.png")
    plt.savefig(f"tasks/stream/pdf/latency_con.pdf")

@task
def wc_overall(ctx):
    # Create the data as a pandas DataFrame
    data = {
        "Approach": [
            "FAASM", "FAASM", "FAASM", "FAASM", "FAASM", "FAASM",
            "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC",
            "Lambda", "Lambda", "Lambda", "Lambda", "Lambda", "Lambda"
        ],
        "Input rate": [
            1, 1000, 2000, 3500, 5000, 6500,
            1, 1000, 2000, 3500, 5000, 6500,
            1, 1000, 2000, 3500, 5000, 6500,
        ],
        "Throughput": [
            10, 100, 120, 100, 110, 100,
            10, 10000, 20000, 34909, 46390, 46000,
            11, 4860, 5245, 3310, 3081, 3106,
        ],
        "Latency": [
            28, 1600, 2060, 1963, 1920, 1901,
            149, 140, 101, 404, 2500, 3937,
            1967, 52316, 164784, 113536, 58098, 75365,
        ]
    }
    df = pd.DataFrame(data)

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Latency"], marker='o', label=approach)

    # Configure plot
    plt.yscale('log')
    plt.xlabel('', fontsize=14)
    plt.ylabel('End-to-end Latency (ms) [Log Scale]', fontsize=14)
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())

    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(f"tasks/stream/figure/wc_overall_latency.png")
    plt.savefig(f"tasks/stream/pdf/wc_overall_latency.pdf")

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Throughput"], marker='o', label=approach)

    # Configure plot
    plt.xlabel('', fontsize=14)
    plt.ylabel('Throughput (tuple/s)', fontsize=14)
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())

    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))

    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(f"tasks/stream/figure/wc_overall_throughput.png")
    plt.savefig(f"tasks/stream/pdf/wc_overall_throughput.pdf")

@task
def sd_overall(ctx):
    # Create the data as a pandas DataFrame
    data = {
    "Approach": [
        "FAASM", "FAASM", "FAASM", "FAASM", "FAASM", "FAASM",
        "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC",
        "Lambda", "Lambda", "Lambda", "Lambda", "Lambda", "Lambda"
    ],
    "Input rate": [
        1, 1000, 3000, 10000, 18000, 26000,
        1, 1000, 3000, 10000, 18000, 26000,
        1, 1000, 3000, 10000, 18000, 26000,
    ],
    "Throughput": [
        1, 20, 19, 20, 21, 19,
        1, 1000, 3000, 10000, 16061, 16060,
        1, 1100, 1945, 1895, 1853, 1892,
    ],
    "Latency": [
        10, 404, 411, 415, 401, 413,
        153, 50, 49, 57, 100, 101,
        1956, 2016, 2161, 1761, 1745, 1926,
    ]
    }
    df = pd.DataFrame(data)

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Latency"], marker='o', label=approach)

    # Configure plot
    plt.yscale('log')
    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())

    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(f"tasks/stream/figure/sd_overall_latency.png")
    plt.savefig(f"tasks/stream/pdf/sd_overall_latencyerall.pdf")

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Throughput"], marker='o', label=approach)

    # Configure plot
    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/sd_overall_throughput.png")
    plt.savefig(f"tasks/stream/pdf/sd_overall_throughput.pdf")

@task
def mo_overall(ctx):
    data = {
    "Approach": [
        "FAASM", "FAASM", "FAASM", "FAASM", "FAASM", "FAASM",
        "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC", "ScalaSSC",
        "Lambda", "Lambda", "Lambda", "Lambda", "Lambda", "Lambda"
    ],
    "Input rate": [
        1, 1000, 3000, 6000, 12000, 18000,
        1, 1000, 3000, 6000, 12000, 18000,
        1, 1000, 3000, 6000, 12000, 18000,
    ],
    "Throughput": [
        1, 17, 18, 17, 17, 18,
        1, 1000, 3000, 6000, 12000, 10484,
        1, 1061, 2620, 2571, 2681, 2616,
    ],
    "Latency": [
        124, 4470, 4291, 4377, 4080, 4084,
        523, 175, 178, 143, 190, 498,
        4110, 13210, 55217, 59906, 50686, 48531,
    ]
    }
    df = pd.DataFrame(data)

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Latency"], marker='o', label=approach)

    # Configure plot
    plt.yscale('log')
    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())

    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/mo_overall_latency.png")
    plt.savefig(f"tasks/stream/pdf/mo_overall_latencyerall.pdf")

    # Plot the data
    plt.figure(figsize=(10, 6))
    for approach in df["Approach"].unique():
        subset = df[df["Approach"] == approach]
        plt.plot(subset["Input rate"], subset["Throughput"], marker='o', label=approach)

    # Configure plot
    plt.xlabel('')
    plt.ylabel('')
    plt.xticks(df["Input rate"].unique(), df["Input rate"].unique())

    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.legend(title="Approach")
    # plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/mo_overall_throughput.png")
    plt.savefig(f"tasks/stream/pdf/mo_overall_throughput.pdf")
