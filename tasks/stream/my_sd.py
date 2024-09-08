from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism, register_function_state, reset_max_replicas
from tasks.util.faasm import (
    get_faasm_metrics_from_json,
    post_async_msg_and_get_result_json,
    write_metrics_to_log,
    write_string_to_log,
) 
from tasks.util.thread import AtomicInteger

from datetime import datetime
import time
from invoke import task
import concurrent.futures
import threading
import re
from collections import defaultdict
import json

msg = {}
result_file = 'tasks/stream/logs/my_sd_results.txt'
output_file = 'tasks/stream/results/'

def read_data_from_file(file_path):
    data_vectors = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors

# Helper function to generate input data ranges
def generate_input_data(records, start, size):
    return [{"partitionedAttribute": record[3], 
             "temperature": record[4]} for record in records[start:start + size]]

def send_message_and_get_result(input_data, chainedId_list):
    global msg
    result_json = post_async_msg_and_get_result_json(msg, num_message=len(input_data) , input_list=input_data, chainedId_list=chainedId_list)
    return result_json

def worker_thread(end_time, sentences, atomic_count, inputbatch):
    count = 0
    total_time = 0
    results = []
    while time.time() < end_time:
        try:
            input_id = atomic_count.get_and_increment(inputbatch)
            chainedId_list = []
            chainedId_list = [input_id + i for i in range(inputbatch)]  # Generate the list
            input_data = generate_input_data(sentences, input_id, inputbatch)
            result = send_message_and_get_result(input_data, chainedId_list)
            results.append(result)
        except Exception as exc:
            print(f"Generated an exception: {exc}")

    return results

@task(default=True)
def run(ctx, batchsize=0, concurrency = 0, inputbatch = 1, scale = 0):
    """
    Use multiple threads to run the 'sd' application and check latency and throughput.
    """
    DURATION = 100 # Seconds
    WORKER_NUM = 15
    
    global msg
    msg = {
    "user": "stream",
    "function": "sd_moving_avg",
    }
    file_path = 'tasks/stream/data/data_sensor.txt'
    records = read_data_from_file(file_path)
    records_len = len(records)

    if batchsize > 0:
        reset_batch_size(batchsize)

    if concurrency > 0:
        reset_max_replicas(concurrency)
    
    register_function_state("stream_sd_moving_avg", "partitionedAttribute", "partitionStateKey")

    flush_workers()
    flush_scheduler()

    # Send one message before the test
    input_data = generate_input_data(records, 0, 1)
    chainedId_list = [1]
    result = send_message_and_get_result(input_data, chainedId_list)
    # print(result)

    if scale > 0:
        scale_function_parallelism("stream", "sd_moving_avg" ,scale)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)
    
    lock = threading.Lock()
    batches_min_start_ts = None
    deadline = None
    function_metrics = defaultdict(lambda: defaultdict(list))
        
    global output_file

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, records, atomic_count, inputbatch) for _ in range(WORKER_NUM)]

        for future in concurrent.futures.as_completed(futures):
            try:
                results_json = future.result()
                for batch_result_json in results_json:
                    start_ts = int(min([result_json["start_ts"] for result_json in batch_result_json]))
                    # Get the deadline for the batch
                    with lock:
                        if batches_min_start_ts is None or start_ts < batches_min_start_ts:
                            batches_min_start_ts = start_ts
                            deadline = batches_min_start_ts + DURATION * 1000

                    actual_times, app_metrics = get_faasm_metrics_from_json(batch_result_json, deadline)
                    with lock:
                            actual_times_str = json.dumps(actual_times, indent=4)
                            write_string_to_log(output_file, actual_times_str)
                    for actual_time in actual_times.values():
                        total_time += actual_time
                        total_count += 1
                    for func_name, metrics in app_metrics.items():
                        for metric_name, times in metrics.items():
                            function_metrics[func_name][metric_name].extend(times)

            except Exception as exc:
                print(f"Generated an exception: {exc}")

    average_time = total_time / total_count if total_count > 0 else 0
    print(f"Total messages sent: {total_count}")
    print(f"Average actual time: {average_time} ms")

    for func_name, metrics in function_metrics.items():
        print(f"Metrics for {func_name}:")
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            print(f"  Average {metric_name}: {int(average_metric_time)} μs")


    global result_file
    write_metrics_to_log(result_file, batchsize, concurrency, inputbatch, total_count, average_time, function_metrics)

@task
def run_multiples(ctx):
    """
    Invoke 'run' function 10 times with batchsize=10 and inputbatch=100.
    Concurrency will vary from 1 to 10, and results will be logged to a file.
    """
    batchsize = 30
    inputbatch = 500
    concurrency_list = [5, 10, 15, 20, 30]
    global output_file 
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{output_file}{current_date}/sd_exp_1"

    for concurrency in concurrency_list:  # From 1 to 10
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale=3"
        print(start_message)
        write_string_to_log(result_file, start_message)
        write_string_to_log(output_file, start_message)
        
        run(ctx, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch, scale=3)
        print(f"Completed run with concurrency={concurrency}\n")

@task
def run_batchsize(ctx):
    """
    Invoke 'run' function with varying batch sizes and concurrency levels.
    Batchsize will vary among 5, 10, 20, 30, 40, 50, 75, 100
    and results will be logged to a file.
    """
    inputbatch = 300
    concurrency = 1
    batch_sizes = [5, 10, 15, 20, 30]
    
    global output_file 
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"{output_file}{current_date}/sd_exp_1"
    global result_file

    for batchsize in batch_sizes:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale=3"
        print(start_message)
        write_string_to_log(result_file, start_message)
        write_string_to_log(output_file, start_message)
        run(ctx, batchsize=batchsize, concurrency=concurrency, inputbatch=inputbatch)
        print(f"Completed run with batchsize={batchsize}, concurrency={concurrency}\n")