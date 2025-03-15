import time
from invoke import task
import concurrent.futures
import threading
import re
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime

from tasks.util.faasm import (
    get_faasm_metrics_from_json,
    write_metrics_to_log,
    get_chained_faasm_exec_time_from_json,
    post_async_msg_and_get_result_json,
    write_string_to_log,
    statistics_result,
)

from tasks.util.thread import (
    AtomicInteger,
    batch_producer,
    native_batch_consumer,
)

from faasmctl.util.flush import flush_workers, flush_scheduler, flush_scheduler
from tasks.util.k8s import flush_redis
from tasks.util.file import read_data_from_txt_file

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
INPUT_BATCHSIZE = 1
NUM_INPUT_THREADS = 10
INPUT_FILE = 'tasks/stream/data/data_sensor_sorted.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "sd_moving_avg",
}
RESULT_FILE = 'tasks/stream/logs/exp_sd_results_cons_new.txt'
MAX_INPUT_COUNT = None
INPUT_MAP = {"sensorId": 3, "temperature": 4}

@task(default=True)
def run(ctx, input_rate, inputbatch = 1):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    global INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS, DURATION

    write_string_to_log(RESULT_FILE, f"New Exp --- Input Rates:{input_rate}, Batchsize: {inputbatch}, Workers: {NUM_INPUT_THREADS}, duration:{DURATION} \n")
    records = read_data_from_txt_file(INPUT_FILE)
    flush_workers()
    flush_scheduler()
    flush_redis()

    # Launch multiple threads
    atomic_count = AtomicInteger(1)
    result_list = []
    result_list_lock = threading.Lock()
    input_threads = []

    batch_queue = Queue()

    start_time = time.time()
    end_time = start_time + DURATION
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
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
                target=native_batch_consumer,
                args=(
                    batch_queue,
                    result_list,
                    result_list_lock,
                    INPUT_MSG,
                    inputbatch,
                    end_time,
                )
            )
            input_threads.append(thread)
            thread.start()

    print(f"length of result json is {len(result_list)}")

    np_result_message, function_metrics = statistics_result(result_list, DURATION, native = True)
    print(np_result_message)
    write_string_to_log(RESULT_FILE, np_result_message)

    for func_name, metrics in function_metrics.items():
        print(f"Metrics for {func_name}:")
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            print(f"  Average {metric_name}: {int(average_metric_time)} μs")
    write_metrics_to_log(RESULT_FILE, function_metrics)

@task
def overall_exp(ctx):
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT
    global NUM_INPUT_THREADS 

    NUM_INPUT_THREADS = 10

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_exp_sd_overall.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_exp_sd_overall")

    # input_rates = [1, 1000, 3000, 10000, 18000, 26000]
    input_rates = [18000, 26000]

    for rate in input_rates:
        run(ctx, input_rate = rate, inputbatch = 1)


@task
def latency_exp_3node(ctx):
    """
        inv stream.faasm-sd.latency-exp-3node
    """
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT
    global NUM_INPUT_THREADS 

    NUM_INPUT_THREADS = 1

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_sd_latency_3node.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_sd_latency_3node")

    input_rates = [1]
    for input_rate in input_rates:
        run(ctx, input_rate = input_rate, inputbatch = 1)

@task
def latency_exp_1node(ctx):
    """
        inv stream.faasm-sd.latency-exp-1node
    """
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT
    global NUM_INPUT_THREADS 

    NUM_INPUT_THREADS = 1

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_sd_latency_1node.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_sd_latency_1node")

    input_rates = [1]
    for input_rate in input_rates:
        run(ctx, input_rate = input_rate, inputbatch = 1)