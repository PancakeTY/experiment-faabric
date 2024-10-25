import time
import threading
from datetime import datetime
from invoke import task
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Utility imports for Faasm and task management
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import (
    reset_batch_size, scale_function_parallelism, 
    register_function_state, reset_max_replicas, output_result
)
from faasmctl.util.invoke import query_result
from tasks.util.thread import (
    AtomicInteger,
    batch_producer,
    batch_consumer,
)
from tasks.util.file import copy_outout, load_app_results, read_data_from_txt_file

# Custom utility functions
from tasks.util.faasm import (
    post_async_batch_msg,
    write_metrics_to_log,
    write_string_to_log,
    generate_input_data,
    statistics_result,
)

from tasks.util.stats import extract_data

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
NUM_INPUT_THREADS = 10
INPUT_FILE = 'tasks/stream/data/data_sensor_sorted.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "sd_moving_avg",
}
RESULT_FILE = 'tasks/stream/logs/exp_sd_results_cons_new.txt'
INPUT_MAP = {"partitionedAttribute": 3, "temperature": 4}

@task
def run(ctx, scale, batchsize, concurrency, inputbatch, input_rate, duration):
    """
    Test the 'wordcount' function with resource contention.
    Input rate unit: data/ second
    """
    global INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP
    global NUM_INPUT_THREADS
    write_string_to_log(RESULT_FILE, f"Input Rates:{input_rate}, Batchsize: {batchsize}, Concurrency: {concurrency}, InputBatch:{inputbatch}, Scale: {scale}\n")
    
    # Get records
    records = read_data_from_txt_file(INPUT_FILE)
    flush_workers()
    flush_scheduler()
    
    register_function_state("stream_sd_moving_avg", "partitionedAttribute", "partitionStateKey")
    
    # Run one request at begining
    input_data = generate_input_data(records, 0, 1, INPUT_MAP)
    appid = post_async_batch_msg(100000, INPUT_MSG, batch_size = 1, input_list = input_data, chained_id_list = [1])
    query_result(appid)

    # Adjust the parameters
    if scale > 1:
        scale_function_parallelism("stream", "sd_moving_avg" ,scale)

    if batchsize > 0:
        reset_batch_size(batchsize)
    
    if concurrency > 0:
        reset_max_replicas(concurrency)

    atomic_count = AtomicInteger(1)
    appid_list = []
    appid_list_lock = threading.Lock()
    input_threads = []
    
    batch_queue = Queue()

    # Launch multiple threads
    start_time = time.time()
    end_time = start_time + duration
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    # Start the ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # Submit the batch_producer function to the executor
        future = executor.submit(
            batch_producer,
            records,
            atomic_count,
            inputbatch,
            INPUT_MAP,
            batch_queue,
            end_time,
            input_rate,
            NUM_INPUT_THREADS
        )

        # Start consumer threads
        for _ in range(NUM_INPUT_THREADS):
            thread = threading.Thread(
                target=batch_consumer,
                args=(
                    batch_queue,
                    appid_list,
                    appid_list_lock,
                    INPUT_MSG,
                    inputbatch,
                    end_time,
                )
            )
            input_threads.append(thread)
            thread.start()

        # Wait for the producer to finish and get the result
        total_items_produced = future.result()
        produce_messenger = f"Total items produced: {total_items_produced}"
        print(produce_messenger)
        write_string_to_log(RESULT_FILE, produce_messenger)

        # Wait for consumer threads to finish
        for thread in input_threads:
            thread.join()

    # Get results from 
    get_result_start_time = None
    result_output = False
    while not result_output:
        get_result_start_time = time.time()
        result_output = output_result()
        time.sleep(5)

    # Copy the output file from container
    copy_outout()
    batches_result = load_app_results()

    get_result_end_time = time.time()
    fetch_result_duration = get_result_end_time - get_result_start_time
    print(f"Duration to get result: {fetch_result_duration}")
    np_result_message, function_metrics = statistics_result(batches_result, duration)
    print(np_result_message)
    write_string_to_log(RESULT_FILE, np_result_message)

    for func_name, metrics in function_metrics.items():
        print(f"Metrics for {func_name}:")
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            print(f"  Average {metric_name}: {int(average_metric_time)} μs")
    write_metrics_to_log(RESULT_FILE, function_metrics)


# Experiment for overall performance
@task
def overall_exp(ctx, scale=3):
    """
    Run the 'overall performance' experiments of spike detection application.
    Basic setup: 
    scale 3; batchsize 20; concurrency 10; inputbatch 20; 
    input rates: 1000, 2000, 3000, 4000, 6000, 8000, 10000, 12000
    runtime: 10 minutes or all the data are processed
    """
    global DURATION

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    inputbatch = 20
    concurrency = 10
    batchsize = 20
    rates = [1000, 2000, 3000, 4000, 6000, 8000, 10000, 12000]

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
    Run the 'varied concurrency' experiments of spike detection application.
    Basic setup: 
    scale 1; batchsize 30; inputbatch 20; 
    input rates: 3000
    concurrency: 1, 2, 3, 4, 5
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE

    RESULT_FILE = 'tasks/stream/logs/exp_sd_cons.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied_con_exp")

    inputbatch = 300
    concurrency_list = [1, 2, 3, 4, 5]
    batchsize = 30
    rate = 3000
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
    data = extract_data("tasks/stream/logs/exp_sd_cons.txt")
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

    plt.savefig("tasks/stream/figure/sd_con_duration.png")
    plt.close()  # Close the figure to prevent overlap

# Experiment for different batch size performance
@task
def varied_batch_exp(ctx, scale=3):
    """
    Run the 'varied batch size' experiment with different batchsize and different input rates
    Basic setup:
    scale 3; concurrency 10; inputbatch 20;
    batchsize: 1, 5, 10, 15, 20, 30
    input rates: 2000, 4000, 6000, 8000 (maximum throughput is about 4000)
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE

    RESULT_FILE = 'tasks/stream/logs/exp_sd_batch.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied_batch_exp")
    inputbatch = 20
    concurrency = 10
    batchsize_list = [40, 50]
    # batchsize_list = [1, 5, 10, 15, 20, 30]
    rates = [1000, 1500, 2000, 2500, 3000, 3500]

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
    data = extract_data("tasks/stream/logs/exp_sd_batch.txt")

    df = pd.DataFrame(data)
    # Ensure that the data types are correct
    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Batch Size'] = df['Batch Size'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)

    sns.set(style="whitegrid")

    # Plot 1: Input Rate vs 99th Percentile Actual Time with a focused y-axis limit
    plt.figure(figsize=(10, 6))
    sns.lineplot(
        data=df,
        x="Input Rate",
        y="99th Percentile Actual Time (ms)",
        hue="Batch Size",
        marker="o",
    )
    plt.ylim(0, 100)  # Adjust this limit as needed to show differences between other batch sizes
    plt.title("Input Rate vs 99th Percentile Actual Time (Focused View)")
    plt.xlabel("Input Rate")
    plt.ylabel("99th Percentile Actual Time (ms)")
    plt.legend(title="Batch Size")
    plt.savefig("tasks/stream/figure/sd_batch_latency_focused.png")
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
    plt.savefig("tasks/stream/figure/sd_batch_throughput.png")
    plt.close()


# Experiment for different scale performance
@task
def varied_para_exp(ctx, scale=3):
    """
    Run the 'varied parallelism' experiment with different parallelism and different input rates
    Basic setup:
    batchsize 20; concurrency 10; inputbatch 20;
    scale 1, 2, 3;
    input rates: 1000, 2000, 3000, 4000, 5000
    runtime: 10 minutes or all the data are processed
    """
    global DURATION
    global RESULT_FILE

    DURATION = 60
    # RESULT_FILE = 'tasks/stream/logs/exp_sd_para.txt'
    RESULT_FILE = 'tasks/stream/logs/temp.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied parallelism")

    inputbatch = 300
    concurrency = 10
    batchsize = 20
    # rates = [1000, 2000, 3000, 4000, 5000, 10000, 12000, 14000]
    rates = [6000, 8000]
    # scale_list = [1, 2, 3]
    scale_list = [2]

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
    Plot the 'varied parallelism' experiment
    """
    data = extract_data("tasks/stream/logs/exp_sd_para.txt")
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
    plt.savefig("tasks/stream/figure/sd_para_latency.png")
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
    plt.savefig("tasks/stream/figure/sd_para_throughput.png")
    plt.close()  # Close the figure to prevent overlap
