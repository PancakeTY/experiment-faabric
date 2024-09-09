import time
from collections import defaultdict
from invoke import task
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism, register_function_state,reset_max_replicas
from faasmctl.util.invoke import query_result
import concurrent.futures
import threading
import re
from tasks.util.thread import AtomicInteger
from datetime import datetime
import json

from tasks.util.faasm import (
    get_faasm_exec_chained_milli_time_from_json,
    get_faasm_metrics_from_json,
    post_async_batch_msg,
    write_metrics_to_log,
    write_string_to_log,
    get_result_thread,
    async_invoke_thread,
    generate_input_data,
)

def read_data_from_file(file_path):
    data_vectors = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors

current_date = datetime.now().strftime("%Y-%m-%d")
DURATION = 10
INPUT_FILE = 'tasks/stream/data/data_sensor.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "sd_moving_avg",
}
OUTPUT_FILE = f"tasks/stream/results/{current_date}/sd_exp_1.txt"
RESULT_FILE = 'tasks/stream/logs/my_sd_results.txt'

@task
def run(ctx, scale=0, batchsize=0):
    """
    Test the 'wordcount' function with resource contention.
    """
    global INPUT_FILE
    global INPUT_MSG
    global DURATION
    global OUTPUT_FILE
    global RESULT_FILE

    records = read_data_from_file(INPUT_FILE)
    records_len = len(records)

    flush_workers()
    flush_scheduler()
    
    register_function_state("stream_sd_moving_avg", "partitionedAttribute", "partitionStateKey")

    input_map = {"partitionedAttribute": 3, "temperature": 4}
    input_data = generate_input_data(records, 0, 1, input_map)
    appid = 1
    print(input_data)
    appid = post_async_batch_msg(appid, INPUT_MSG, batch_size = 1, input_list = input_data, chained_id_list = [1])
    query_result(appid)

    # adjust the parameters
    if scale > 1:
        scale_function_parallelism("stream", "sd_moving_avg" ,scale)

    if batchsize > 0:
        reset_batch_size(batchsize)

    reset_max_replicas(10)

    atomic_count = AtomicInteger(1)

    appid = 100000
    appid_list = []
    
    input_index = 0
    input_batchsize = 300

    appid_list_lock = threading.Lock()

    num_input_threads = 10
    input_threads = []
    # Launch multiple threads
    start_time = time.time()
    end_time = start_time + DURATION
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    for _ in range(num_input_threads):
        print(f"Launching input thread {_}")
        thread = threading.Thread(target=async_invoke_thread, args=(records, atomic_count, input_batchsize, INPUT_MSG, input_map, appid_list, appid_list_lock, end_time))
        input_threads.append(thread)
        thread.start()
    
    time.sleep(2)

    num_output_threads = 10
    output_threads = []
    batches_result = []
    shared_batches_min_start_ts = [None]
    start_ts_lock = threading.Lock()
    result_lock = threading.Lock()

    for _ in range(num_output_threads):
        print(f"Launching output thread {_}")
        thread = threading.Thread(target=get_result_thread, args=(appid_list, appid_list_lock, shared_batches_min_start_ts, start_ts_lock, batches_result, result_lock, end_time))
        output_threads.append(thread)
        thread.start()
    
    for thread in input_threads:
        thread.join()

    for thread in output_threads:
        thread.join()

    # unit second
    total_count = 0
    total_time = 0
    function_metrics = defaultdict(lambda: defaultdict(list))

    deadline = shared_batches_min_start_ts[0] + DURATION * 1000
    print(f"Deadline: {deadline}")

    result_lock = threading.Lock()

    for app_result in batches_result:
        actual_times, app_metrics = get_faasm_metrics_from_json(app_result, deadline)
        with result_lock:
            actual_times_str = json.dumps(actual_times, indent=4)
            write_string_to_log(OUTPUT_FILE, actual_times_str)       
        for actual_time in actual_times.values():
            total_time += actual_time
            total_count += 1
        for func_name, metrics in app_metrics.items():
            for metric_name, times in metrics.items():
                function_metrics[func_name][metric_name].extend(times)
 

    average_time = total_time / total_count if total_count > 0 else 0
    print(f"Total messages sent: {total_count}")
    print(f"Average actual time: {average_time} ms")

    for func_name, metrics in function_metrics.items():
        print(f"Metrics for {func_name}:")
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            print(f"  Average {metric_name}: {int(average_metric_time)} μs")
    write_metrics_to_log(RESULT_FILE, batchsize, 10, 300, total_count, average_time, function_metrics)
    write_string_to_log(OUTPUT_FILE, "end")


@task
def run_multiple_batches(ctx, scale=0):
    """
    Run the 'test_contention' task with different batch sizes: 5, 10, 15, 20, 30, 50.
    """
    inputbatch = 300
    concurrency = 10
    batch_sizes = [100, 75, 50, 30, 20, 15, 10, 5, 1]
    global OUTPUT_FILE
    global DURATION

    for batchsize in batch_sizes:
        timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
        start_message = f"{timestamp} Running with batchsize={batchsize}, concurrency={concurrency}, inputbatch={inputbatch}, scale={scale}, duration={DURATION}"   
        write_string_to_log(RESULT_FILE, start_message)
        write_string_to_log(OUTPUT_FILE, start_message)
        # Call the test_contention task with the current batchsize
        run(ctx, scale=scale, batchsize=batchsize)
        print(f"Completed test_contention with batchsize: {batchsize}")