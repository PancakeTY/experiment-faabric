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

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
INPUT_BATCHSIZE = 1
NUM_INPUT_THREADS = 10
INPUT_FILE = 'tasks/stream/data/Top100_Gutenberg_books.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "wc_split",
}
RESULT_FILE = 'tasks/stream/logs/native_exp_wc.txt'
MAX_INPUT_COUNT = None
INPUT_MAP = {"sentence": 0}

def read_sentences_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            text = file.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

    # Remove all non-alphabetic characters and convert to lowercase
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', text).lower()
    
    # Split text into words
    words = cleaned_text.split()
    print(f"Total words extracted: {len(words)}")

    # Group words into sentences of 10 words each
    sentences = [[' '.join(words[i:i+10])] for i in range(0, len(words), 10)]
    print(f"Total sentences created: {len(sentences)}")

    return sentences

@task(default=True)
def run(ctx, input_rate, inputbatch = 1):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    global INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS, DURATION

    write_string_to_log(RESULT_FILE, f"New Exp --- Input Rates:{input_rate}, Batchsize: {inputbatch}, Workers: {NUM_INPUT_THREADS}, duration:{DURATION} \n")
    records = read_sentences_from_file(INPUT_FILE)
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
    RESULT_FILE = 'tasks/stream/logs/native_exp_wc_overall.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_exp_wc_overall")

    # input_rates = [1000, 2000, 3500, 5000, 6500]
    input_rates = [6500]
    for input_rate in input_rates:
        run(ctx, input_rate = input_rate, inputbatch = 1)


@task
def latency_exp_3node(ctx):
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT
    global NUM_INPUT_THREADS 

    NUM_INPUT_THREADS = 10

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_wc_latency_3node.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_wc_latency_3node")

    input_rates = [1]
    for input_rate in input_rates:
        run(ctx, input_rate = input_rate, inputbatch = 1)

@task
def latency_exp_1node(ctx):
    """
        inv stream.faasm-wc.latency-exp-1node
    """
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT
    global NUM_INPUT_THREADS 

    NUM_INPUT_THREADS = 1

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_wc_latency_1node.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: native_wc_latency_1node")

    input_rates = [1]
    for input_rate in input_rates:
        run(ctx, input_rate = input_rate, inputbatch = 1)