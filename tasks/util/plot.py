from matplotlib.patches import Polygon
from os import makedirs
from os.path import join
from subprocess import run
from tasks.util.env import PLOTS_ROOT
from tasks.util.faasm import get_faasm_version
from tasks.util.stats import extract_avg_tuple_duration

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd

_PLOT_COLORS = {
    "granny": (1, 0.4, 0.4),
    "granny-no-migrate": (0.29, 0.63, 0.45),
    "granny-no-elastic": (0.29, 0.63, 0.45),
    "batch": (0.2, 0.6, 1.0),
    "slurm": (0.3, 0.3, 0.3),
}

UBENCH_PLOT_COLORS = [
    (1, 0.4, 0.4),
    (0.29, 0.63, 0.45),
    (0.2, 0.6, 1.0),
    (0.3, 0.3, 0.3),
    (0.6, 0.4, 1.0),
    (1.0, 0.8, 0.4),
]

PLOT_PATTERNS = ["//", "\\\\", "||", "-", "*-", "o-"]
SINGLE_COL_FIGSIZE = (6, 3)
SINGLE_COL_FIGSIZE_HALF = (3, 3)
DOUBLE_COL_FIGSIZE_HALF = SINGLE_COL_FIGSIZE
DOUBLE_COL_FIGSIZE_THIRD = (4, 4)


def fix_hist_step_vertical_line_at_end(ax):
    axpolygons = [
        poly for poly in ax.get_children() if isinstance(poly, Polygon)
    ]
    for poly in axpolygons:
        poly.set_xy(poly.get_xy()[:-1])


def _do_get_for_baseline(workload, baseline, color=False, label=False):
    if workload == "omp-elastic":
        if baseline == "granny":
            this_label = "granny-no-elastic"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "granny-elastic":
            this_label = "granny"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "batch" or baseline == "slurm":
            this_label = baseline
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]

        raise RuntimeError(
            "Unrecognised baseline ({}) for workload: {}".format(
                baseline, workload
            )
        )

    if workload == "mpi-migrate":
        if baseline == "granny":
            this_label = "granny-no-migrate"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "granny-migrate":
            this_label = "granny"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline in ["slurm", "batch"]:
            this_label = baseline
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]

        raise RuntimeError(
            "Unrecognised baseline ({}) for workload: {}".format(
                baseline, workload
            )
        )

    if workload == "mpi-locality":
        if baseline == "granny":
            this_label = "slurm"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "granny-migrate":
            this_label = "granny"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "granny-batch":
            this_label = "batch"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]

        raise RuntimeError(
            "Unrecognised baseline ({}) for workload: {}".format(
                baseline, workload
            )
        )

    if workload == "mpi-spot":
        if baseline == "granny":
            this_label = "granny"
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]
        if baseline == "batch" or baseline == "slurm":
            this_label = baseline
            if label:
                return this_label
            if color:
                return _PLOT_COLORS[this_label]

        raise RuntimeError(
            "Unrecognised baseline ({}) for workload: {}".format(
                baseline, workload
            )
        )


def get_color_for_baseline(workload, baseline):
    return _do_get_for_baseline(workload, baseline, color=True)


def get_label_for_baseline(workload, baseline):
    return _do_get_for_baseline(workload, baseline, label=True)


def save_plot(fig, plot_dir, plot_name):
    fig.tight_layout()
    versioned_dir = join(PLOTS_ROOT, get_faasm_version())
    makedirs(versioned_dir, exist_ok=True)
    for plot_format in ["png", "pdf"]:
        this_plot_name = "{}.{}".format(plot_name, plot_format)
        plot_file = join(plot_dir, this_plot_name)
        fig.savefig(plot_file, format=plot_format, bbox_inches="tight")
        print("Plot saved to: {}".format(plot_file))

        # Also make a copy in the tag directory
        versioned_file = join(versioned_dir, this_plot_name)
        run(
            "cp {} {}".format(plot_file, versioned_file),
            shell=True,
            check=True,
        )

    hostname = (
        run("hostname", shell=True, check=True, capture_output=True)
        .stdout.decode("utf-8")
        .strip()
    )
    tmp_file = "/tmp/{}".format(this_plot_name)
    print(
        "scp {}:{} {} && evince {} &".format(
            hostname, plot_file, tmp_file, tmp_file
        )
    )

def shorten_number(x, pos):
    if x == "inf":
        return x
    if x >= 1000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'

def varied_para_plot_util(df, application, workers = 3, function_duration = None):
    """
    Plot the 'varied parallelism' experiment with bar figure
    """

    sns.set(style="ticks")

    custom_colors =['#c6e9b4', '#40b5c4', '#225da8']  # Example colors for scales 1, 2, 3

    # Histogram 1: Distribution of Input Rate
    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    sns.barplot(
    data=df,
    x="Input Rate",
    y="99th Percentile Actual Time (ms)",
    hue="Scale",
    errorbar="ci",  # or "ci" for confidence intervals
    capsize=0.3,    # Controls the "T" cap size
    err_kws={"color": "black", "linewidth": 1.5},  # Updated approach
    palette=custom_colors,
    )
    if (application == "wc"):
        plt.ylabel('End-to-end Latency (ms)', fontsize=14)
    else:
        plt.ylabel('', fontsize=14)
    plt.xlabel('', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))

    plt.tight_layout()
    if workers == 3:
        plt.savefig(f"tasks/stream/figure/{application}_para_latency.png")
        plt.savefig(f"tasks/stream/pdf/{application}_para_latency.pdf")
    else:
        plt.savefig(f"tasks/stream/figure/{application}_para_latency_{workers}-worker.png")       
        plt.savefig(f"tasks/stream/pdf/{application}_para_latency_{workers}-worker.pdf")
    plt.close()

    # Histogram 3: Distribution of Throughput
    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    sns.barplot(
    data=df,
    x="Input Rate",
    y="Throughput (msg/sec)",
    hue="Scale",
    errorbar="ci",  # or "ci" for confidence intervals
    capsize=0.3,    # Controls the "T" cap size
    err_kws={"color": "black", "linewidth": 1.5},  # Updated approach
    palette=custom_colors
    )
    if (application == "wc"):
        plt.ylabel('Throughput (tuple/s)', fontsize=14)
    else:
        plt.ylabel('', fontsize=14)
    plt.xlabel('', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))

    plt.tight_layout()  # Adjust layout to reduce whitespace
    if workers == 3:
        plt.savefig(f"tasks/stream/figure/{application}_para_throughput.png")
        plt.savefig(f"tasks/stream/pdf/{application}_para_throughput.pdf")
    else:
        plt.savefig(f"tasks/stream/figure/{application}_para_throughput_{workers}-worker.png")       
        plt.savefig(f"tasks/stream/pdf/{application}_para_throughput_{workers}-worker.pdf")       
    plt.close()

    if function_duration is None:
        return
    
    # Plot the duration of function_duration
    df_filter = pd.DataFrame()
    df_filter['Input Rate'] = df['Input Rate']
    df_filter['Scale'] = df['Scale'].astype(int)
    df_filter['Average Tuple Duration (µs)'] = df['Functions'].apply(lambda x: extract_avg_tuple_duration(x, function_name=function_duration))
    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    sns.barplot(
        data=df_filter,
        x="Input Rate",
        y="Average Tuple Duration (µs)",
        hue="Scale",
        errorbar="ci",  # or "ci" for confidence intervals
        capsize=0.3,    # Controls the "T" cap size
        err_kws={"color": "black", "linewidth": 1.5},  # Updated approach
        palette=custom_colors,
    )
    if (application == "wc"):
        plt.ylabel('Average Tuple Processing Latency (µs)', fontsize=14)
    else:
        plt.ylabel('', fontsize=14)
    plt.xlabel('', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    plt.tight_layout()
    if workers == 3:
        plt.savefig(f"tasks/stream/figure/{application}_para_duration.png")
        plt.savefig(f"tasks/stream/pdf/{application}_para_duration.pdf")
    else:
        plt.savefig(f"tasks/stream/figure/{application}_para_duration_{workers}-worker.png")
        plt.savefig(f"tasks/stream/pdf/{application}_para_duration_{workers}-worker.pdf")
    plt.close()

    # df_grouped = df.groupby(['Input Rate', 'Scale'])['Throughput (msg/sec)'].mean().reset_index()

    # # Print the average throughput for each combination of Input Rate and Scale
    # print("Average Throughput (msg/sec) for each combination of Input Rate and Scale:")
    # for index, row in df_grouped.iterrows():
    #     print(f"Input Rate: {row['Input Rate']}, Scale: {row['Scale']}, Average Throughput: {row['Throughput (msg/sec)']} msg/sec")

    df_grouped = df.groupby(['Input Rate', 'Scale'])['99th Percentile Actual Time (ms)'].mean().reset_index()

    # Print the average latency for each combination of Input Rate and Scale
    print("Average 99th Percentile Latency (ms) for each combination of Input Rate and Scale:")
    for index, row in df_grouped.iterrows():
        print(f"Input Rate: {row['Input Rate']}, Scale: {row['Scale']}, Average Latency: {row['99th Percentile Actual Time (ms)']} ms")

def varied_batch_plot_util(df, application):
    """
    Plot the 'varied batch size' experiment with T-shaped error bars
    using the updated Seaborn API (>= 0.15) and concise axis labels.
    """

    sns.set(style="whitegrid")

    # Plot 1: Input Rate vs 99th Percentile Actual Time
    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    sns.barplot(
        data=df,
        x="Input Rate",
        y="99th Percentile Actual Time (ms)",
        hue="Batch Size",
        errorbar="ci",  # or "ci" for confidence intervals
        capsize=0.3,    # Controls the "T" cap size
        err_kws={"color": "black", "linewidth": 1.5},  # Updated approach
        palette=sns.color_palette("YlGnBu", n_colors=df['Batch Size'].nunique()),
        ax=ax
    )
    ax.yaxis.grid(False)
    
    plt.yscale('log')
    if (application == "wc"):
        plt.ylabel('End-to-end Latency (ms)', fontsize=14)
    else:
        plt.ylabel('', fontsize=14)
    plt.xlabel('', fontsize=14)
    # Set x-axis tick labels to be concise (e.g., 20000 -> 20k)
    # ax.xaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/{application}_batch_latency.png")
    plt.savefig(f"tasks/stream/pdf/{application}_batch_latency.pdf")
    plt.close()

    # Plot 2: Input Rate vs Throughput
    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    sns.barplot(
        data=df,
        x="Input Rate",
        y="Throughput (msg/sec)",
        hue="Batch Size",
        errorbar="ci",
        capsize=0.3,
        err_kws={"color": "black", "linewidth": 1.5},  # Updated approach
        palette=sns.color_palette("YlGnBu", n_colors=df['Batch Size'].nunique()),
        ax=ax
    )
    ax.yaxis.grid(False)

    if (application == "wc"):
        plt.ylabel('Throughput (msg/sec)', fontsize=14)
    else:
        plt.ylabel('', fontsize=14)
    plt.xlabel('', fontsize=14)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/{application}_batch_throughput.png")
    plt.savefig(f"tasks/stream/pdf/{application}_batch_throughput.pdf")
    plt.close()

def varied_con_plot_util(df, application):
    import matplotlib.ticker as mticker

    sns.set(style="whitegrid")

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    sns.lineplot(
        data=df,
        x="Concurrency",
        y="Average Tuple Duration (µs)",
        marker="o",
        errorbar=None,
        ax=ax
    )
    ax.yaxis.grid(False)
    ax.xaxis.grid(False)
    # plt.xlabel('Concurrency', fontsize=14)
    if application == 'wc':
        plt.ylabel('Average Tuple Processing Latency (µs)', fontsize=14)
    else:
        plt.ylabel('')       
    plt.xlabel('')       

    plt.xticks(ticks=range(df['Concurrency'].min(), df['Concurrency'].max() + 1), fontsize=12)
    plt.yticks(fontsize=12)

    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/{application}_con_duration.png")
    plt.savefig(f"tasks/stream/pdf/{application}_con_duration.pdf")
    plt.close()

    df_avg = df.groupby('Concurrency', as_index=False)['Average Tuple Duration (µs)'].mean()
    print("Average Tuple Duration (µs) for each Concurrency level:")
    for index, row in df_avg.iterrows():
        print(f"Concurrency: {row['Concurrency']}, Average Latency: {row['Average Tuple Duration (µs)']:.2f} µs")


def overall_plot_util(df, application):
    sns.set(style="whitegrid")

    # Plot 1: Input Rate vs 99th Percentile Actual Time
    plt.figure(figsize=(6.4, 4.8))
    # ax = plt.gca()
    sns.barplot(
        data=df,
        x="Input Rate",
        y="99th Percentile Actual Time (ms)",
        errorbar="sd",
        capsize=0.2,
        err_kws={"color": "black", "linewidth": 1.5},
        # ax=ax
    )

    plt.yscale('log')
    plt.xlabel('Input Rate (tuples/s)', fontsize=12)
    plt.ylabel('End-to-end Latency (ms)', fontsize=12)
    # ax.xaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.xticks(fontsize=11)
    plt.yticks(fontsize=11)

    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/{application}_overall_latency.png")
    plt.close()

    # Plot 2: Input Rate vs Throughput
    plt.figure(figsize=(6.4, 4.8))
    ax = plt.gca()
    sns.barplot(
        data=df,
        x="Input Rate",
        y="Throughput (msg/sec)",
        errorbar="sd",
        capsize=0.2,
        err_kws={"color": "black", "linewidth": 1.5},
        ax=ax
    )

    plt.xlabel('Input Rate (tuples/s)', fontsize=12)
    plt.ylabel('Throughput (tuples/s)', fontsize=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(shorten_number))
    plt.xticks(fontsize=11)
    plt.yticks(fontsize=11)

    plt.tight_layout()
    plt.savefig(f"tasks/stream/figure/{application}_overall_throughput.png")
    plt.close()
