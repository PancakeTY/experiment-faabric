from invoke import task
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from tasks.util.stats import extract_data, extract_avg_tuple_duration_prefix


def shorten_number(x, pos):
    if x == "inf":
        return x
    if x >= 1000:
        return f"{x/1000:.0f}k"
    return f"{x:.0f}"


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
    ax.set_ylabel("Avg Executor Exec. Time (ms)")
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
