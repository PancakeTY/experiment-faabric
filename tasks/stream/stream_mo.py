import time
import threading
from datetime import datetime
from collections import defaultdict
from invoke import task
import concurrent.futures
import json
import csv

# Utility imports for Faasm and task management
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import (
    reset_batch_size, scale_function_parallelism, 
    register_function_state, reset_max_replicas, output_result
)
from faasmctl.util.invoke import query_result
from tasks.util.thread import AtomicInteger
from tasks.util.file import copy_outout, load_app_results, read_data_from_txt_file

# Custom utility functions
from tasks.util.faasm import (
    get_faasm_exec_chained_milli_time_from_json,
    get_faasm_metrics_from_json,
    post_async_batch_msg,
    write_metrics_to_log,
    write_string_to_log,
    async_invoke_thread,
    generate_input_data,
    statistics_result,
)

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
INPUT_BATCHSIZE = 300
NUM_INPUT_THREADS = 10
INPUT_FILE = 'tasks/stream/data/machine_usage.csv'
INPUT_MSG = {
    "user": "stream",
    "function": "mo_score",
}
RESULT_FILE = 'tasks/stream/logs/my_mo_results-2.txt'
INPUT_MAP = {"machineId": 0, "cpu": 2, "mem": 3, "timestamp": 1}

def read_data_from_csv(file_path):
    records = []
    with open(file_path, 'r') as file:  # Open the file
        reader = csv.reader(file)
        for data in reader:
            # Modify the first column to keep only the numeric part after 'm_'
            data[0] = data[0].split('_')[1]
            records.append(data)
    return records

@task
def run(ctx, scale=0, batchsize=0, concurrency=10):
    """
    Test the 'machine outlier' function with resource contention.
    """
    global INPUT_BATCHSIZE, INPUT_FILE, INPUT_MSG, DURATION, RESULT_FILE, INPUT_MAP
    global NUM_INPUT_THREADS
    write_string_to_log(RESULT_FILE, f"Batchsize: {batchsize}, Concurrency: {concurrency}, Scale: {scale}\n")

    # Get records
    records = read_data_from_csv(INPUT_FILE)
    flush_workers()
    flush_scheduler()
    
    register_function_state("stream_mo_score", "None", "None")
    register_function_state("stream_mo_anomaly", "partitionedAttribute", "partitionStateKey")
    register_function_state("stream_mo_alert", "None", "None")
    
    # Run one request at begining
    input_data = generate_input_data(records, 0, 1, INPUT_MAP)
    appid = post_async_batch_msg(100000, INPUT_MSG, batch_size = 1, input_list = input_data, chained_id_list = [1])
    query_result(appid)

    # Adjust the parameters
    if scale > 1:
        scale_function_parallelism("stream", "stream_mo_anomaly" ,scale)

    if batchsize > 0:
        reset_batch_size(batchsize)
    
    if concurrency > 0:
        reset_max_replicas(concurrency)

    atomic_count = AtomicInteger(1)

    appid_list = []
    
    appid_list_lock = threading.Lock()

    input_threads = []
    # Launch multiple threads
    start_time = time.time()
    end_time = start_time + DURATION
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    for _ in range(NUM_INPUT_THREADS):
        print(f"Launching input thread {_}")
        thread = threading.Thread(target=async_invoke_thread, args=(records, atomic_count, INPUT_BATCHSIZE, INPUT_MSG, INPUT_MAP, appid_list, appid_list_lock, end_time))
        input_threads.append(thread)
        thread.start()
    
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
    duration = get_result_end_time - get_result_start_time
    print(f"Duration to get result: {duration}")
    np_result_message, function_metrics = statistics_result(batches_result, DURATION)
    print(np_result_message)
    write_string_to_log(RESULT_FILE, np_result_message)

    for func_name, metrics in function_metrics.items():
        print(f"Metrics for {func_name}:")
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            print(f"  Average {metric_name}: {int(average_metric_time)} μs")
    write_metrics_to_log(RESULT_FILE, function_metrics)


@task
def run_multiple_batches(ctx, scale=0):
    """
    Run the 'test_contention' task with different batch sizes: 1, 5, 10, 15, 20, 30, 50, 75, 100.
    """
    write_string_to_log(RESULT_FILE, CUTTING_LINE)

    inputbatch = 300
    concurrency = 10
    batch_sizes = [1, 5, 10, 15, 20, 30, 50, 75, 100]
    # batch_sizes = [30]
    global DURATION

    for batchsize in batch_sizes:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
        write_string_to_log(RESULT_FILE, start_message)
        # Call the test_contention task with the current batchsize
        run(ctx, scale=scale, batchsize=batchsize)
        print(f"Completed test_contention with batchsize: {batchsize}")

    
@task
def run_multiple_cons(ctx, scale=0):
    """
    Run the 'test_contention' task with different cons: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 50
    """
    write_string_to_log(RESULT_FILE, CUTTING_LINE)

    inputbatch = 300
    concurrencies = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 50]
    batchsize = 30
    global DURATION

    for concurrency in concurrencies:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
        write_string_to_log(RESULT_FILE, start_message)
        # Call the test_contention task with the current batchsize
        run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency)
        print(f"Completed test_contention with con: {concurrency}")

    