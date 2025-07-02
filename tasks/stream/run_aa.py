import time
import threading
from datetime import datetime
from invoke import task
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.planner import run_application_with_input
from tasks.util.faasm import write_string_to_log
from tasks.util.stats import extract_data
from tasks.stream.data_generator.aa_data import get_persistent_state
from tasks.util.file import read_data_from_txt_file

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 15
INPUT_BATCHSIZE = 20
NUM_INPUT_THREADS = 10
APPLICATION_NAME = "aa_application"
INPUT_FILE = '/pvol/runtime/experiment-faabric/tasks/stream/data/aa_dataset.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "aa_deserialize",
}
RESULT_FILE = 'tasks/stream/logs/exp_aa_results.txt'
INPUT_MAP = {"json": 0}

def enwrap_json(record):
    """
    Enwrap the record into a JSON object.
    """
    ad_id, campaign_id, event_time, event_type = record
    json_str = json.dumps({
        "user_id" : "stream",
        "page_id" : "page",
        "ad_id": ad_id,
        "ad_type": "ad",
        "event_type" : event_type,
        "event_time": event_time,
        "ip_address": "1.1.1.1",
    })
    return [json_str]

@task
def run(ctx, scale, batchsize, concurrency, inputbatch, input_rate, duration):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    # Prepare the input data
    raw_records = read_data_from_txt_file(INPUT_FILE)
    # records
    records = [enwrap_json(record) for record in raw_records]

    # # Prepare the persistent state
    persistent_state = get_persistent_state()

    node1 = {"name": "stream_aa_deserialize",
             "node_type": "STATELESS",
             "input": True,
             "inputFields": ["json"],
             "successorNode": ["stream_aa_filter"],
            }
    node2 = {"name": "stream_aa_filter",
             "node_type": "STATELESS",
             "input": None,
             "inputFields": ["ad_id"],
             "successorNode": ["stream_aa_projection"],
            }
    node3 = {"name": "stream_aa_projection",
             "node_type": "STATELESS",
             "input": None,
             "inputFields": ["ad_id"],
             "successorNode": ["stream_aa_join"],
            }
    node4 = {"name": "stream_aa_join",
             "node_type": "PARTITIONED_STATEFUL",
             "partitionBy": "ad_id",
             "input": None,
             "parallelism": scale,
             "inputFields": ["ad_id"],
             "successorNode": ["stream_aa_campaign"],
            }
    node5 = {"name": "stream_aa_campaign",
             "node_type": "PARTITIONED_STATEFUL",
             "partitionBy": "campaign_id",
             "input": None,
             "parallelism": scale,
             "inputFields": ["stream_campaign_id"],
             "successorNode": [],
            }

    nodes = [node1, node2, node3, node4, node5]

    run_application_with_input(
        application_name = APPLICATION_NAME,
        nodes = nodes,
        records=records,
        result_file=RESULT_FILE,
        input_map=INPUT_MAP,
        input_msg=INPUT_MSG,
        num_input_threads=NUM_INPUT_THREADS,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=inputbatch,
        input_rate=input_rate,
        duration=duration,
        persistent_state = persistent_state,
    )

@task
def test(ctx, scale=3):
    global DURATION
    global RESULT_FILE
    
    DURATION = 60
    RESULT_FILE = 'tasks/stream/logs/aa_temp_test.txt'

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    inputbatch = 20
    concurrency = 10
    batchsize = 20

    # rates = [2500, 5000, 7500, 10000]
    rates = [2500]
    schedule_modes = [0]
    
    for schedule_mode in schedule_modes:
        reset_stream_parameter("schedule_mode", schedule_mode)
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")

# Experiment for overall performance
@task
def overall_exp(ctx, scale=3):
    """
    Run the 'overall performance' experiments of wordcount application.
    Basic setup: 
    scale 3; batchsize 20; concurrency 10; inputbatch 20; 
    input rates: 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400, 2500
    runtime: 10 minutes or all the data are processed
    """
    global DURATION

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    inputbatch = 20
    concurrency = 10
    batchsize = 20
    rates = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400, 2500]

    for rate in rates:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
        write_string_to_log(RESULT_FILE, start_message)
        # Call the test_contention task with the current batchsize
        run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
        print(f"Completed test_contention with con: {concurrency}")

@task
def overall_plot(ctx):
    data = extract_data("tasks/stream/logs/temp.txt")
    df = pd.DataFrame(data)
    print(df)

    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Scale'] = df['Scale'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)
    df['Throughput (msg/sec)'] = df['Throughput (msg/sec)'].astype(float)
    sns.set(style="whitegrid")

    # Plot 1: Input Rate vs 99th Percentile Actual Time
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="Input Rate",
        y="99th Percentile Actual Time (ms)",
        hue="Scale",
        marker="o",
    )
    plt.title("Input Rate vs 99th Percentile Actual Time")
    plt.xlabel("Input Rate")
    plt.ylabel("99th Percentile Actual Time (ms)")
    plt.legend(title="Scale")
    plt.savefig("tasks/stream/figure/wc_para_latency.png")
    plt.close()  # Close the figure to prevent overlap

    # Plot 2: Input Rate vs Throughput
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="Input Rate",
        y="Throughput (msg/sec)",
        hue="Scale",
        marker="o",
    )
    plt.title("Input Rate vs Throughput (msg/sec)")
    plt.xlabel("Input Rate")
    plt.ylabel("Throughput (msg/sec)")
    plt.legend(title="Scale")
    plt.savefig("tasks/stream/figure/wc_para_throughput.png")
    plt.close()  # Close the figure to prevent overlap

# Experiment for different batch size performance
@task
def varied_batch_exp(ctx, scale=3):
    """
    Run the 'varied batch size' experiment with different batchsize and different input rates
    Basic setup:
    scale 3; concurrency 10; inputbatch 20;
    batchsize: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30
    input rates: 1000, 1200, 1400, 1600, 1800, 2000
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE

    DURATION = 60
    RESULT_FILE = 'tasks/stream/logs/exp_wc_batch.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied batch size")
    inputbatch = 300
    concurrency = 10
    # batchsize_list = [1, 10, 20, 30, 40]
    batchsize_list = [50, 60, 100]
    rates = [30000]

    for batchsize in batchsize_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")

@task
def varied_batch_plot(ctx):
    """
    Plot the 'varied batch size' experiment
    """

    #unfinished
    data = extract_data("tasks/stream/logs/exp_wc_batch.txt")

    df = pd.DataFrame(data)
    # Ensure that the data types are correct
    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Batch Size'] = df['Batch Size'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)

    sns.set(style="whitegrid")

    # Plot 1: Input Rate vs 99th Percentile Actual Time
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="Input Rate",
        y="99th Percentile Actual Time (ms)",
        hue="Batch Size",
        marker="o",
    )
    plt.title("Input Rate vs 99th Percentile Actual Time")
    plt.xlabel("Input Rate")
    plt.ylabel("99th Percentile Actual Time (ms)")
    plt.legend(title="Batch Size")
    plt.savefig("tasks/stream/figure/wc_batch_latency.png")
    plt.close()  # Close the figure to prevent overlap

    # Plot 2: Input Rate vs Throughput
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="Input Rate",
        y="Throughput (msg/sec)",
        hue="Batch Size",
        marker="o",
    )
    plt.title("Input Rate vs Throughput")
    plt.xlabel("Input Rate")
    plt.ylabel("Throughput (msg/sec)")
    plt.legend(title="Batch Size")
    plt.savefig("tasks/stream/figure/wc_batch_throughput.png")
    plt.close()


# Experiment for different scale performance
@task
def varied_para_exp(ctx, scale=3):
    """
    Run the 'varied parallelism' experiment with different parallelism and different input rates
    Basic setup:
    batchsize 20; concurrency 10; inputbatch 20;
    scale 1, 2, 3;
    input rates: 1000, 1100, 1200, 1300, 1400, 1500
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE
    
    DURATION = 60
    RESULT_FILE = 'tasks/stream/logs/exp_wc_para.txt'

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied parallelism")

    inputbatch = 200
    concurrency = 10
    batchsize = 20
    rates = [6000]
    # rates = [6000, 8000, 10000]
    scale_list = [1, 2, 3]

    for scale in scale_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")

@task
def varied_para_plot(ctx):
    """
    Plot the 'varied parallelism' experiment with line plots and histograms
    """
    data = extract_data("tasks/stream/logs/exp_wc_para.txt")
    df = pd.DataFrame(data)
    
    pd.set_option('display.max_rows', None)  # Show all rows


    # Convert columns to appropriate data types
    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Scale'] = df['Scale'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)
    df['Throughput (msg/sec)'] = df['Throughput (msg/sec)'].astype(float)
    df = df[df['Input Rate'] != 8000]

    df['Input Rate'] = df['Input Rate'].replace(9223372036854775807, float('inf'))

    print(df)

    sns.set(style="whitegrid")

    custom_colors =['#c6e9b4', '#40b5c4', '#225da8']  # Example colors for scales 1, 2, 3

    # Histogram 1: Distribution of Input Rate
    plt.figure(figsize=(10, 6))
    sns.barplot(
    data=df,
    x="Input Rate",
    y="99th Percentile Actual Time (ms)",
    hue="Scale",
    errorbar=None,
    palette=custom_colors
    )
    plt.xlabel('')  # Hide x-axis label
    plt.ylabel('')  # Hide y-axis label
    plt.tight_layout()  # Adjust layout to reduce whitespace
    application = "wc"
    plt.savefig(f"tasks/stream/figure/{application}_para_latency.pdf")
    plt.close()

    # Histogram 3: Distribution of Throughput
    plt.figure(figsize=(10, 6))
    sns.barplot(
    data=df,
    x="Input Rate",
    y="Throughput (msg/sec)",
    hue="Scale",
    errorbar=None,
    palette=custom_colors
    )
    plt.xlabel('')  # Hide x-axis label
    plt.ylabel('')  # Hide y-axis label
    plt.tight_layout()  # Adjust layout to reduce whitespace
    plt.savefig("tasks/stream/figure/wc_para_throughput_hist.pdf")
    plt.close()


# Experiment for latency performance
@task
def latency_exp(ctx, scale=3):
    """
    Run the 'latency' experiments of wordcount application.
    Basic setup: 
    batchsize 1; concurrency 10; inputbatch 1; 
    input rates: 1
    scale: 1, 3
    runtime: 10 minutes or all the data are processed
    """
    global DURATION

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: latency_exp")

    inputbatch = 1
    concurrency = 10
    batchsize = 1
    rates = [1]
    scale_list = [1, 3]

    for scale in scale_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")


# Experiment for different concurrency
@task
def varied_con_exp(ctx, scale=1):
    """
    Run the 'varied concurrency' experiments of wordcount application.
    Basic setup: 
    scale 1; batchsize 30; inputbatch 20; 
    input rates: 1200
    concurrency: 1, 2, 3, 4, 5
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE

    RESULT_FILE = 'tasks/stream/logs/exp_wc_cons.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied_con_exp")

    inputbatch = 300
    concurrency_list = [1, 2, 3, 4, 5]
    batchsize = 30
    rate = 1200
    for concurrency in concurrency_list:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
        write_string_to_log(RESULT_FILE, start_message)
        # Call the test_contention task with the current batchsize
        run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
        print(f"Completed test_contention with con: {concurrency}")

@task
def varied_con_plot(ctx):
    """
    Plot the 'varied concurrency' experiment
    """
    data = extract_data("tasks/stream/logs/exp_wc_cons.txt")
    df = pd.DataFrame(data)

    df_filter = pd.DataFrame()
    df_filter['Concurrency'] = df['Concurrency'].astype(int)
    df_filter['Average Tuple Duration (µs)'] = df['Average Tuple Duration (µs)'].astype(float)

    df_avg = df_filter.groupby('Concurrency', as_index=False)['Average Tuple Duration (µs)'].mean()

    plt.figure(figsize=(10, 6))
    sns.set(style="whitegrid")
    sns.lineplot(
        data=df_avg,
        x="Concurrency",
        y="Average Tuple Duration (µs)",
        marker="o",
    )
    plt.title("Concurrency vs Duration per Request (µs)")
    plt.xlabel("Concurrency")
    plt.ylabel("Duration per Request (µs)")
    plt.xticks(ticks=range(df_avg['Concurrency'].min(), df_avg['Concurrency'].max() + 1))

    plt.savefig("tasks/stream/figure/wc_con_duration.png")
    plt.close()  # Close the figure to prevent overlap

# Experiment for different scale performance
@task
def decentra_run(ctx, scale=2):
    global DURATION
    global RESULT_FILE
    
    DURATION = 30
    RESULT_FILE = 'tasks/stream/logs/decentra_wc.txt'

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied parallelism")
    inputbatch = 200
    concurrency = 10
    batchsize = 20
    rates = [10000]
    scale_list = [3]

    for scale in scale_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")
