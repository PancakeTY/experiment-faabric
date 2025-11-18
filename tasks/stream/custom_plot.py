from invoke import task
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.ticker import FuncFormatter
import numpy as np
import json
import statistics

from tasks.util.stats import (
    extract_data,
    extract_avg_tuple_duration_prefix,
    parse_log,
)


def shorten_number(x, pos):
    if x == "inf":
        return x
    if x >= 1000:
        return f"{x/1000:.0f}k"
    return f"{x:.0f}"


@task
def trans_plot(ctx):
    log_files = {
        "WC": "tasks/stream/logs_hs/trans_wc.txt",
        "SD": "tasks/stream/logs_hs/trans_sd.txt",
        "AA": "tasks/stream/logs_hs/trans_aa.txt",
        "PL": "tasks/stream/logs_hs/trans_pl.txt",
        "NWM": "tasks/stream/logs_hs/trans_nwm.txt",
        "ETL": "tasks/stream/logs_hs/trans_etl.txt",
    }

    method_mapping = {7: "Centralized Scheduling", 5: "LcSched", 3: "FaaSFlow"}
    plot_data = []

    for app_name, file_path in log_files.items():
        df_from_log = parse_log(file_path)

        if df_from_log.empty:
            print(f"Warning: Could not parse or found no data in {file_path}")
            continue

        # Aggregate: add std for both median and p95 latency
        agg_data = (
            df_from_log.groupby("schedulemode")
            .agg(
                median_latency=("medianTotalLatency", "median"),
                median_std=("medianTotalLatency", "std"),  # 👈 added
                p95_latency=("p95TotalLatency", "median"),
                p95_std=("p95TotalLatency", "std"),
            )
            .reset_index()
        )

        for _, row in agg_data.iterrows():
            plot_data.append(
                {
                    "Application": app_name,
                    "Method": method_mapping.get(
                        row["schedulemode"], "Unknown"
                    ),
                    "Median Latency": row["median_latency"],
                    "Median Std": row["median_std"],  # 👈 added
                    "p95 Latency": row["p95_latency"],
                    "p95 Std": row["p95_std"],
                }
            )

    df_plot = pd.DataFrame(plot_data)

    fig, ax = plt.subplots(figsize=(10, 6))

    app_names = df_plot["Application"].unique()
    methods = ["LcSched", "FaaSFlow", "Centralized Scheduling"]
    x_positions = np.arange(len(app_names))
    bar_width = 0.25
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    for i, method in enumerate(methods):
        subset = df_plot[df_plot["Method"] == method]
        subset = (
            subset.set_index("Application").reindex(app_names).reset_index()
        )
        positions = [pos + i * bar_width for pos in x_positions]

        # p95 bars with error bars
        ax.bar(
            positions,
            subset["p95 Latency"],
            yerr=subset["p95 Std"],
            width=bar_width,
            label=f"{method} p95",
            color=colors[i],
            capsize=4,
        )

        # Median latency with error bars
        ax.errorbar(
            positions,
            subset["Median Latency"],
            yerr=subset["Median Std"],
            fmt="o",
            color="black",
            markersize=5,
            capsize=4,
            label=("Median Latency" if i == 0 else ""),
        )

    # Formatting
    ax.set_xticks([pos + bar_width for pos in x_positions])
    ax.set_xticklabels(app_names, fontsize=15)
    ax.set_ylabel("End-to-End Latency (µs)", fontsize=15)

    handles, labels = plt.gca().get_legend_handles_labels()
    legend_order = [
        "Median Latency",
        "LcSched p95",
        "FaaSFlow p95",
        "Centralized Scheduling p95",
    ]

    ordered_handles = [
        handles[labels.index(lbl)] for lbl in legend_order if lbl in labels
    ]
    ordered_labels = [lbl for lbl in legend_order if lbl in labels]

    ax.legend(ordered_handles, ordered_labels, loc="upper left", fontsize=14)

    plt.tight_layout()
    plt.savefig("tasks/stream/pdf_hs/4.1.trans_comparision.pdf")
    plt.show()


# Example of calling the function
# trans_plot(None)


@task
def test_plot(ctx):
    log_files = {
        "WC": "tasks/stream/logs_hs/test_wc.txt",
        "SD": "tasks/stream/logs_hs/test_sd.txt",
        "AA": "tasks/stream/logs_hs/test_aa.txt",
        "PL": "tasks/stream/logs_hs/test_pl.txt",
        "NWM": "tasks/stream/logs_hs/test_nwm.txt",
        "ETL": "tasks/stream/logs_hs/test_etl.txt",
    }

    method_mapping = {7: "Centralized Scheduling", 5: "LcSched", 3: "FaaSFlow"}
    plot_data = []

    for app_name, file_path in log_files.items():
        df_from_log = parse_log(file_path)

        if df_from_log.empty:
            print(f"Warning: Could not parse or found no data in {file_path}")
            continue

        # Aggregate: add std for both median and p95 latency
        agg_data = (
            df_from_log.groupby("schedulemode")
            .agg(
                throughput=("throughput", "median"),
                throughput_std=("throughput", "std"),
                median_latency=("medianTotalLatency", "median"),
                median_std=("medianTotalLatency", "std"),
                p95_latency=("p95TotalLatency", "median"),
                p95_std=("p95TotalLatency", "std"),
            )
            .reset_index()
        )

        print(agg_data)

        for _, row in agg_data.iterrows():
            plot_data.append(
                {
                    "Application": app_name,
                    "Method": method_mapping.get(
                        row["schedulemode"], "Unknown"
                    ),
                    "Throughput": row["throughput"],
                    "Throughput Std": row["throughput_std"],
                    "Median Latency": row["median_latency"] / 1e6,
                    "Median Std": row["median_std"] / 1e6,
                    "p95 Latency": row["p95_latency"] / 1e6,
                    "p95 Std": row["p95_std"] / 1e6,
                }
            )

    comparisons = []

    for app_name, file_path in log_files.items():
        df_from_log = parse_log(file_path)
        if df_from_log.empty:
            continue

        agg_data = (
            df_from_log.groupby("schedulemode")
            .agg(
                throughput=("throughput", "median"),
                median_latency=("medianTotalLatency", "median"),
            )
            .reset_index()
        )

        # Make lookup by schedulemode
        modes = agg_data.set_index("schedulemode").to_dict(orient="index")

        if 3 in modes and 5 in modes:
            thr_5_vs_3 = modes[5]["throughput"] / modes[3]["throughput"]
            lat_5_vs_3 = (
                modes[3]["median_latency"] - modes[5]["median_latency"]
            ) / modes[3]["median_latency"]
        else:
            thr_5_vs_3 = lat_5_vs_3 = None

        if 7 in modes and 5 in modes:
            thr_5_vs_7 = modes[5]["throughput"] / modes[7]["throughput"]
            lat_5_vs_7 = (
                modes[7]["median_latency"] - modes[5]["median_latency"]
            ) / modes[7]["median_latency"]
        else:
            thr_5_vs_7 = lat_5_vs_7 = None

        comparisons.append(
            {
                "Application": app_name,
                "Throughput 5 vs 3 (×)": thr_5_vs_3,
                "Throughput 5 vs 7 (×)": thr_5_vs_7,
                "Latency reduction 5 vs 3 (s)": (
                    lat_5_vs_3 if lat_5_vs_3 else None
                ),
                "Latency reduction 5 vs 7 (s)": (
                    lat_5_vs_7 if lat_5_vs_7 else None
                ),
            }
        )

    df_comparisons = pd.DataFrame(comparisons)
    print(df_comparisons)

    df_plot = pd.DataFrame(plot_data)

    fig, ax = plt.subplots(figsize=(10, 6))

    app_names = df_plot["Application"].unique()
    methods = ["LcSched", "FaaSFlow", "Centralized Scheduling"]
    x_positions = np.arange(len(app_names))
    bar_width = 0.25
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    for i, method in enumerate(methods):
        subset = df_plot[df_plot["Method"] == method]
        subset = (
            subset.set_index("Application").reindex(app_names).reset_index()
        )
        positions = [pos + i * bar_width for pos in x_positions]

        # p95 bars with error bars
        ax.bar(
            positions,
            subset["p95 Latency"],
            yerr=subset["p95 Std"],
            width=bar_width,
            label=f"{method} p95",
            color=colors[i],
            capsize=4,
        )

        # Median latency with error bars
        ax.errorbar(
            positions,
            subset["Median Latency"],
            yerr=subset["Median Std"],
            fmt="o",
            color="black",
            markersize=5,
            capsize=4,
            label=("Median Latency" if i == 0 else ""),
        )

    # Formatting
    ax.set_xticks([pos + bar_width for pos in x_positions])
    ax.set_xticklabels(app_names, fontsize=15)
    ax.set_ylabel("End-to-End Latency (s)", fontsize=15)

    handles, labels = plt.gca().get_legend_handles_labels()
    legend_order = [
        "Median Latency",
        "LcSched p95",
        "FaaSFlow p95",
        "Centralized Scheduling p95",
    ]

    ordered_handles = [
        handles[labels.index(lbl)] for lbl in legend_order if lbl in labels
    ]
    ordered_labels = [lbl for lbl in legend_order if lbl in labels]

    ax.legend(ordered_handles, ordered_labels, loc="upper left", fontsize=14)

    plt.tight_layout()
    plt.savefig("tasks/stream/pdf_hs/4.3.overall_comparision_latency.pdf")
    plt.show()

    fig, ax1 = plt.subplots(figsize=(10, 6))

    app_names = df_plot["Application"].unique()
    methods = ["LcSched", "FaaSFlow", "Centralized Scheduling"]
    x_positions = np.arange(len(app_names))

    for i, method in enumerate(methods):
        subset = df_plot[df_plot["Method"] == method]
        # Ensure data aligns with the applications on the x-axis
        subset = (
            subset.set_index("Application").reindex(app_names).reset_index()
        )
        positions = [pos + i * bar_width for pos in x_positions]

        ax1.bar(
            positions,
            subset["Throughput"],
            yerr=subset["Throughput Std"],
            width=bar_width,
            label=method,
            color=colors[i],
            capsize=4,
        )

    # --- Formatting and Labels ---
    ax1.set_xticks([pos + bar_width for pos in x_positions])
    ax1.set_xticklabels(app_names, fontsize=15)
    ax1.set_ylabel("Throughput (requests/sec)", fontsize=15)
    ax1.set_title("")
    ax1.legend(loc="upper right", fontsize=14)
    # ax1.grid(axis="y", linestyle="--", alpha=0.7)

    def thousands_formatter(x, pos):
        "The two args are the value and tick position"
        return f"{int(x/1000)}k"

    # Apply the formatter to the y-axis
    ax1.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    ax1.set_ylim(0, 105000)

    plt.tight_layout()
    plt.savefig("tasks/stream/pdf_hs/4.3.overall_comparision_throughput.pdf")
    plt.show()


@task
def test3_plot(ctx):
    log_files = {
        "WC": "tasks/stream/logs_hs/test_wc_3.txt",
        "SD": "tasks/stream/logs_hs/test_sd_3.txt",
        "AA": "tasks/stream/logs_hs/test_aa_3.txt",
        "PL": "tasks/stream/logs_hs/test_pl_3.txt",
        "NWM": "tasks/stream/logs_hs/test_nwm_3.txt",
        "ETL": "tasks/stream/logs_hs/test_etl_3.txt",
    }

    method_mapping = {7: "Centralized Scheduling", 5: "LcSched", 3: "FaaSFlow"}
    plot_data = []

    for app_name, file_path in log_files.items():
        df_from_log = parse_log(file_path)

        if df_from_log.empty:
            print(f"Warning: Could not parse or found no data in {file_path}")
            continue

        # Aggregate: add std for both median and p95 latency
        agg_data = (
            df_from_log.groupby("schedulemode")
            .agg(
                throughput=("throughput", "median"),
                throughput_std=("throughput", "std"),
                median_latency=("medianTotalLatency", "median"),
                median_std=("medianTotalLatency", "std"),
                p95_latency=("p95TotalLatency", "median"),
                p95_std=("p95TotalLatency", "std"),
            )
            .reset_index()
        )

        for _, row in agg_data.iterrows():
            plot_data.append(
                {
                    "Application": app_name,
                    "Method": method_mapping.get(
                        row["schedulemode"], "Unknown"
                    ),
                    "Throughput": row["throughput"],
                    "Throughput Std": row["throughput_std"],
                    "Median Latency": row["median_latency"] / 1e6,
                    "Median Std": row["median_std"] / 1e6,
                    "p95 Latency": row["p95_latency"] / 1e6,
                    "p95 Std": row["p95_std"] / 1e6,
                }
            )

    df_plot = pd.DataFrame(plot_data)

    print(df_plot)

    app_names = df_plot["Application"].unique()
    methods = ["LcSched", "FaaSFlow", "Centralized Scheduling"]
    x_positions = np.arange(len(app_names))
    bar_width = 0.25
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    fig, ax1 = plt.subplots(figsize=(10, 6))

    app_names = df_plot["Application"].unique()
    methods = ["LcSched", "FaaSFlow", "Centralized Scheduling"]
    x_positions = np.arange(len(app_names))

    for i, method in enumerate(methods):
        subset = df_plot[df_plot["Method"] == method]
        # Ensure data aligns with the applications on the x-axis
        subset = (
            subset.set_index("Application").reindex(app_names).reset_index()
        )
        positions = [pos + i * bar_width for pos in x_positions]

        ax1.bar(
            positions,
            subset["Throughput"],
            yerr=subset["Throughput Std"],
            width=bar_width,
            label=method,
            color=colors[i],
            capsize=4,
        )

    # --- Formatting and Labels ---
    ax1.set_xticks([pos + bar_width for pos in x_positions])
    ax1.set_xticklabels(app_names, fontsize=15)
    ax1.set_ylabel("Throughput (requests/sec)", fontsize=15)
    ax1.set_title("")
    ax1.legend(loc="upper right", fontsize=14)
    # ax1.grid(axis="y", linestyle="--", alpha=0.7)

    ax1.set_ylim(0, 105000)

    def thousands_formatter(x, pos):
        "The two args are the value and tick position"
        return f"{int(x/1000)}k"

    # Apply the formatter to the y-axis
    ax1.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

    plt.tight_layout()
    plt.savefig("tasks/stream/pdf_hs/4.4.three_worker_comparision.pdf")
    plt.show()


@task
# inv stream.custom-plot.state-access
def state_access(ctx):
    data = np.array(
        [
            [397, 1473, 1532, 371, 376],
            [420, 1097, 1554, 370, 378],
            [377, 1191, 1767, 387, 379],
        ]
    )
    categories = ["S", "RO", "RT", "LO", "LT"]

    # Calculate averages per category
    averages = data.mean(axis=0)

    # Plot bar chart of averages with dark blue color
    x = np.arange(len(categories))

    plt.rcParams.update({"font.size": 15})
    fig, ax = plt.subplots()
    # #1f77b4 is blue
    ax.bar(x, averages, width=0.6, color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_xlabel("")
    ax.set_ylabel("Avg Executor Exec. Time (ns)")
    ax.set_title("")
    ax.grid(False)

    plt.tight_layout()
    plt.show()
    plt.savefig(f"tasks/stream/pdf_hs/2.bg_state.pdf")


@task
# inv stream.custom-plot.data-locality
def data_locality(ctx):
    data = np.array([[100, 100, 100, 77, 80]])
    categories = ["WC", "SD", "AA", "NWM", "ETL"]

    # Calculate averages per category
    averages = data.mean(axis=0)

    # Plot bar chart of averages with dark blue color
    x = np.arange(len(categories))

    plt.rcParams.update({"font.size": 15})
    fig, ax = plt.subplots()
    # #2ca02c is green
    ax.bar(x, averages, width=0.6, color="#2CA02C")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_xlabel("")
    ax.set_ylabel("Dispachted Remotely Proportion (%)")
    ax.set_title("")
    ax.grid(False)

    plt.tight_layout()
    plt.show()
    plt.savefig(f"tasks/stream/pdf_hs/2.bg_locality.pdf")


@task
# inv stream.custom-plot.partitioned-state
def partitioned_state(ctx):
    data = np.array(
        [
            [553034, 296078, 710888],
            [942976, 1125040, 1367798],
        ]
    )
    categories = ["WC", "SD"]
    partitions = ["Partition 1", "Partition 2", "Partition 3"]

    # Calculate percentage distribution per category
    row_sums = data.sum(axis=1, keepdims=True)
    percentages = data / row_sums * 100

    # Plot grouped by category
    x = np.arange(len(categories))
    width = 0.2
    plt.rcParams.update({"font.size": 15})

    fig, ax = plt.subplots()
    for i, part in enumerate(partitions):
        ax.bar(x + (i - 1) * width, percentages[:, i], width, label=part)

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Percentage of Request Distribution (%)")
    ax.set_xlabel("")
    ax.set_title("")
    # ax.legend(title="Partitions")
    plt.tight_layout()
    plt.show()
    plt.savefig(f"tasks/stream/pdf_hs/2.partition_state.pdf")


@task
def cpuload(ctx, app):
    # Ensure the file path is correct for your environment
    RESULT_FILE = f"tasks/stream/logs_hs/cpu_{app}.txt"
    try:
        with open(RESULT_FILE, "r") as file:
            log_data = file.read()
    except FileNotFoundError:
        print(f"Error: The file {RESULT_FILE} was not found.")
        return

    # 1. Isolate the relevant JSON data for schedulemode=5
    runs = re.split(
        r"\d{2}--\w{3}--\d{4} \d{2}:\d{2}:\d{2} Running with", log_data
    )
    target_json_str = ""
    for run in runs:
        if "schedulemode=5" in run:
            start_index = run.find("{")
            end_index = run.rfind("}")
            if start_index != -1 and end_index != -1:
                target_json_str = run[start_index : end_index + 1]
                break

    # 2. Parse the JSON and perform calculations
    if target_json_str:
        try:
            data = json.loads(target_json_str)
            worker_stats = data.get("workerStats", {})

            all_proportions = []
            all_exec_pcts = []  # To store exec percentages for avg/median
            all_sched_pcts = []  # To store sched percentages for avg/median
            record_count = 0

            # Iterate through each IP address and its records
            for ip, records in worker_stats.items():
                for record in records:
                    exec_pct = record.get("cpuExecutePct", 0)
                    sched_pct = record.get("cpuSchedulePct", 0)

                    all_exec_pcts.append(exec_pct)
                    all_sched_pcts.append(sched_pct)
                    record_count += 1

            # 3. Calculate the final averages and medians, then display the results
            if record_count > 0:
                # Calculate all metrics
                # average_proportion = sum(all_proportions) / record_count
                average_exec_pct = sum(all_exec_pcts) / record_count
                average_sched_pct = sum(all_sched_pcts) / record_count
                median_exec_pct = statistics.median(all_exec_pcts)
                median_sched_pct = statistics.median(all_sched_pcts)

                sum_exec_pct = sum(all_exec_pcts)
                sum_sched_pct = sum(all_sched_pcts)
                exec_proportion = (
                    100 * sum_exec_pct / (sum_exec_pct + sum_sched_pct)
                )

                # Print a clear summary
                print(
                    f"Found {record_count} valid records for the run with schedulemode=5 (where cpuExecutePct >= 20)."
                )
                print("------------------------------------------")
                print(f"Average cpuExecutePct : {average_exec_pct:.2f}%")
                print(f"Median cpuExecutePct  : {median_exec_pct:.2f}%")
                print(f"Average cpuSchedulePct: {average_sched_pct:.2f}%")
                print(f"Median cpuSchedulePct : {median_sched_pct:.2f}%")

                print(f"sum cpuExecutePct : {sum_exec_pct:.2f}%")
                print(f"sum cpuSchedulePct : {sum_sched_pct:.2f}%")
                print(f"sum exec_proportion : {exec_proportion:.2f}%")

                print("------------------------------------------")
                # print(
                #     f"Average Proportion (Execute/Schedule): {average_proportion:.2f} to 1"
                # )
            else:
                print(
                    "No valid worker stat records were found after applying filters."
                )

        except json.JSONDecodeError:
            print("Error: Could not parse the JSON data.")
    else:
        print("Error: Could not find the data block for schedulemode=5.")


@task
def notused_reconfig(ctx):
    # Raw data provided by the user
    data_string = """
        """

    # 1. --- Data Preparation ---
    # Split the main string into two parts based on the "data 2" separator
    parts = re.split(r"data 2", data_string, flags=re.IGNORECASE)
    data1_str = parts[0]
    data2_str = parts[1]

    # Use regular expressions to find all numbers in each part
    # and convert them to integers
    data1 = [int(num) for num in re.findall(r"\d+", data1_str)]
    data2 = [int(num) for num in re.findall(r"\d+", data2_str)]

    # 2. --- Data Smoothing ---
    # Convert lists to pandas Series to easily calculate the moving average
    s1 = pd.Series(data1)
    s2 = pd.Series(data2)

    # Define the window size for the moving average
    window_size = 20
    data1_smooth = s1.rolling(window=window_size).mean()
    data2_smooth = s2.rolling(window=window_size).mean()

    # 3. --- Plotting ---
    plt.style.use("seaborn-v0_8-whitegrid")  # Use a nice style for the plot
    fig, ax = plt.subplots(
        figsize=(12, 7)
    )  # Create a figure and axes for the plot

    # Plot the smoothed data
    ax.plot(
        data1_smooth,
        label=f"Data 1 (Smoothed, Window={window_size})",
        color="royalblue",
        linewidth=2,
    )
    ax.plot(
        data2_smooth,
        label=f"Data 2 (Smoothed, Window={window_size})",
        color="darkorange",
        linewidth=2,
    )

    # Add titles and labels for clarity
    ax.set_title(
        "Smoothed Comparison of ETL reconfig", fontsize=16, fontweight="bold"
    )
    ax.set_xlabel("Data Point Index", fontsize=12)
    ax.set_ylabel("Value", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)

    # Improve layout and display the plot
    plt.tight_layout()
    plt.show()
    plt.savefig(f"tasks/stream/pdf_hs/reconfig_etl.pdf")


def _process_log_content(content: str):
    """
    Parses log content from a string, extracts all JSON objects, and calculates
    the average of the 'runtimeCountHistory' lists.
    """
    try:
        with open(content, "r") as file:
            log_data = file.read()
    except FileNotFoundError:
        print(f"Error: The file {content} was not found.")
        return

    runs = re.split(
        r"\d{2}--\w{3}--\d{4} \d{2}:\d{2}:\d{2} Running with", log_data
    )

    json_strings = []
    for run in runs:
        start_index = run.find("{")
        end_index = run.rfind("}")
        if start_index != -1 and end_index != -1:
            target_json_str = run[start_index : end_index + 1]
            json_strings.append(target_json_str)

    all_histories = []

    for js_string in json_strings:
        try:
            data = json.loads(js_string)
            if "runtimeCountHistory" in data and data["runtimeCountHistory"]:
                all_histories.append(data["runtimeCountHistory"])
        except json.JSONDecodeError:
            continue  # Ignore non-JSON blocks

    if not all_histories:
        return []

    print(f"Histories size is {len(all_histories)}")

    # Find the length of the longest list to define the array size
    max_len = max(len(h) for h in all_histories)

    sums = np.zeros(max_len)
    counts = np.zeros(max_len, dtype=int)

    # Add values from each history list to the sums and counts arrays
    for history in all_histories:
        sums[: len(history)] += history
        counts[: len(history)] += 1

    # Calculate the average, handling potential division by zero
    averages = np.divide(
        sums, counts, out=np.zeros_like(sums), where=counts != 0
    )

    return averages.tolist()


def moving_average(data: list, window_size: int):
    """Calculates the simple moving average of a list of numbers."""
    if not data or len(data) < window_size:
        return []
    return np.convolve(data, np.ones(window_size) / window_size, mode="valid")


@task
def reconfig(ctx, app):
    """
    Analyzes reconfiguration log data, smooths the results, and generates
    a comparison plot.
    """
    print("🚀 Starting reconfiguration analysis task...")

    LOG_DATA_CONFIG = f"tasks/stream/logs_hs/reconfig_{app}_config.txt"
    LOG_DATA_NOCONFIG = f"tasks/stream/logs_hs/reconfig_{app}_noconfig.txt"

    avg_config = _process_log_content(LOG_DATA_CONFIG)
    avg_noconfig = _process_log_content(LOG_DATA_NOCONFIG)

    avg_config = avg_config[0:600]
    avg_noconfig = avg_noconfig[0:600]
    window_size = 20
    print(
        f"🔬 Smoothing data with a pandas rolling window of {window_size}..."
    )

    smoothed_config = pd.Series(avg_config).rolling(window=window_size).mean()
    smoothed_noconfig = (
        pd.Series(avg_noconfig).rolling(window=window_size).mean()
    )

    # --- Plotting ---
    print("📊 Generating plot...")
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(12, 7))
    # Plot the smoothed pandas Series
    ax.plot(
        smoothed_config,
        color="blue",
        linestyle="-",
        label=f"With Reconfiguration (Smoothed, window={window_size})",
    )
    ax.plot(
        smoothed_noconfig,
        color="red",
        linestyle="--",
        label=f"Without Reconfiguration (Smoothed, window={window_size})",
    )

    ax.set_title("")

    if app == "wc":
        ax.set_ylim(10000, 26000)

    if app == "aa":
        ax.set_ylim(22000, 36000)

    if app == "nwm":
        ax.set_ylim(20000, 23000)

    if app == "pl":
        ax.set_ylim(20000, 28000)

    ax.set_xlabel("", fontsize=12)
    ax.set_ylabel("Average Count", fontsize=15)
    ax.legend()
    ax.grid(False)
    plt.tight_layout()

    plot_filename = f"tasks/stream/pdf_hs/4.2.config_{app}.pdf"
    plt.savefig(plot_filename)
    print(f"✅ Plot saved as '{plot_filename}'")
    plt.show()

    print("✨ Task finished successfully.")
