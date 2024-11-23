import time
import threading
from datetime import datetime
from invoke import task
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from datetime import datetime, timedelta
import time

# Utility imports for Faasm and task management
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import (
    reset_batch_size, scale_function_parallelism, reset_stream_parameter,
    register_function_state, reset_max_replicas, output_result
)
from faasmctl.util.invoke import query_result
from tasks.util.thread import (
    AtomicInteger,
    batch_producer,
    batch_consumer,
)
from tasks.util.file import copy_outout, load_app_results, read_data_from_txt_file
from tasks.util.k8s import flush_redis
# Custom utility functions
from tasks.util.faasm import (
    post_async_batch_msg,
    write_metrics_to_log,
    write_string_to_log,
    generate_input_data,
    statistics_result,
)

from tasks.util.stats import extract_data, extract_avg_tuple_duration
from tasks.util.plot import varied_para_plot_util, varied_batch_plot_util, varied_con_plot_util

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
INPUT_BATCHSIZE = 300
NUM_INPUT_THREADS = 10
INPUT_FILE = 'tasks/stream/data/data_sensor_sorted.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "sd_moving_avg",
}
RESULT_FILE = 'tasks/stream/logs/exp_sd_results_cons_new.txt'
INPUT_MAP = {"partitionedAttribute": 3, "temperature": 4}

@task
def run(ctx, scale, batchsize, concurrency, inputbatch, input_rate, duration, max_inflight_reqs = 15000):
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
    flush_redis()
    
    register_function_state("stream_sd_moving_avg", "partitionedAttribute", "partitionStateKey")
    reset_stream_parameter("is_outputting", 0)
    reset_stream_parameter("max_inflight_reqs", max_inflight_reqs)

    # Run one request at begining
    input_data = generate_input_data(records, 0, 1, INPUT_MAP)
    chained_id_return = post_async_batch_msg(100000, INPUT_MSG, batch_size = 1, input_list = input_data, chained_id_list = [1])
    query_result(chained_id_return[0])

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

    print("All threads finished")
    time.sleep(10)

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
    inputbatch = 500
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

    inputbatch = 500
    concurrency_list = [1, 2, 3, 4, 5]
    batchsize = 30
    rate = 12000
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

    # Process DataFrame
    function_name = "stream_sd_moving_avg_0"
    print(pd)
    df_filter = pd.DataFrame()
    df_filter['Concurrency'] = df['Concurrency'].astype(int)
    df_filter['Average Tuple Duration (µs)'] = df['Functions'].apply(lambda x: extract_avg_tuple_duration(x, function_name=function_name))

    print(df_filter)
    varied_con_plot_util(df_filter, "sd")

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
    inputbatch = 500
    concurrency = 10
    # batchsize_list = [1, 10, 20, 40, 80, 200, 500]
    batchsize_list = [80, 200, 500]
    rates = [12000, 15000, 18000, 21000, sys.maxsize]
    # rates = [12000, 15000, 18000]

    for batchsize in batchsize_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            while True:
                try:
                    # Call the test_contention task with the current batchsize
                    run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION)
                    print(f"Completed test_contention with con: {concurrency}")
                    break  # Break the loop if the function completes successfully
                except Exception as e:
                    # Log the error message
                    error_timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
                    error_message = f"{error_timestamp} Error occurred: {e}. Retrying in 1 minute..."
                    write_string_to_log(RESULT_FILE, error_message)
                    print(error_message)
                    
                    # Wait for 1 minute before retrying
                    time.sleep(60)

    
@task
def varied_batch_plot(ctx):
    """
    Plot the 'varied batch size' experiment
    """

    data = extract_data("tasks/stream/logs/exp_sd_batch.txt")

    df = pd.DataFrame(data)
    pd.set_option('display.max_rows', None)  # Show all rows

    df = df[(df['Batch Size'] != 5) & (df['Batch Size'] != 60)]
    # Ensure that the data types are correct
    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Batch Size'] = df['Batch Size'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)
    df['Input Rate'] = df['Input Rate'].replace(9223372036854775807, float('inf'))

    pd.set_option('display.max_rows', None)
    sorted_df = df.sort_values(by=['Batch Size', 'Input Rate'])
    print(sorted_df)

    duplicates = df[df.duplicated(['Throughput (msg/sec)'], keep=False)]
    # print(duplicates)

    grouped_counts = df.groupby(['Batch Size', 'Input Rate']).size().reset_index(name='counts')
    print(grouped_counts)

    varied_batch_plot_util(df, "sd")


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

    RESULT_FILE = 'tasks/stream/logs/exp_sd_para.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: varied parallelism")
    max_inflight_reqs = 15000

    inputbatch = 500
    concurrency = 10
    batchsize = 20
    rates = [5000, 10000, 15000, 20000, sys.maxsize]
    scale_list = [1, 2, 3]

    for scale in scale_list:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, input_rate=rate, duration=DURATION, max_inflight_reqs = max_inflight_reqs)
            print(f"Completed test_contention with con: {concurrency}")
    
@task
def varied_para_plot(ctx):
    """
    Plot the 'varied parallelism' experiment
    """
    data = extract_data("tasks/stream/logs/exp_sd_para.txt")
    df = pd.DataFrame(data)

    df['Input Rate'] = df['Input Rate'].astype(int)
    df['Scale'] = df['Scale'].astype(int)
    df['99th Percentile Actual Time (ms)'] = df['99th Percentile Actual Time (ms)'].astype(float)
    df['Throughput (msg/sec)'] = df['Throughput (msg/sec)'].astype(float)
    df['Input Rate'] = df['Input Rate'].replace(9223372036854775807, float('inf'))
    
    pd.set_option('display.max_rows', None)
    # print(df)

    # duplicates = df[df.duplicated(['Throughput (msg/sec)'], keep=False)]
    # print(duplicates)

    sorted_df = df.sort_values(by=['Scale', 'Input Rate'])
    print(sorted_df)

    varied_para_plot_util(df, "sd", 3, 'stream_sd_moving_avg_0')