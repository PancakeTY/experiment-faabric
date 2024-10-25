import time
from invoke import task
import concurrent.futures
import threading
import re
from collections import defaultdict
from datetime import datetime

from tasks.util.faasm import (
    get_faasm_metrics_from_json,
    get_chained_faasm_exec_time_from_json,
    post_async_msg_and_get_result_json,
    write_string_to_log,
)

from tasks.util.thread import AtomicInteger
from tasks.util.k8s import flush_redis
from faasmctl.util.flush import flush_workers

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
NUM_INPUT_THREADS = 10
RESULT_FILE = 'tasks/stream/logs/native_exp_wc.txt'
MAX_INPUT_COUNT = None

def read_sentences_from_file(file_path):
    global MAX_INPUT_COUNT
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
    sentences = [' '.join(words[i:i+10]) for i in range(0, len(words), 10)]
    print(f"Total sentences created: {len(sentences)}")

    if MAX_INPUT_COUNT is not None:
        sentences = sentences[:MAX_INPUT_COUNT]

    return sentences

# Helper function to generate input data ranges
def generate_input_data(sentences, start, end):
    return [{"sentence": sentence} for sentence in sentences[start:end + 1]]


def send_message_and_get_result(input_id, input_data, input_batch):
    msg = {
        "user": "stream",
        "function": "wc_split",
    }
    print(input_data)
    # chained Id should start from 1
    chainedId_list = list(range(input_id + 1, input_id + 1 + input_batch))

    result_json = post_async_msg_and_get_result_json(msg, num_message = input_batch, input_list=input_data, chainedId_list = chainedId_list)
    return result_json
    
def worker_thread(end_time, sentences, atomic_count, input_batch):
    count = 0
    total_time = 0
    results = []

    while time.time() < end_time:
        try:
            input_id = atomic_count.get_and_increment(input_batch)
            if input_id + input_batch >= len(sentences):
                break
            input_data = generate_input_data(sentences, input_id, input_id + input_batch -1)
            result = send_message_and_get_result(input_id, input_data, input_batch)
            results.append(result)
        except Exception as exc:
            print(f"Worker Generated an exception: {exc}")

    return results

@task(default=True)
def run(ctx, input_batch = 1):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    FILE_PATH = 'tasks/stream/data/books.txt'
    global DURATION
    WORKER_NUM = 1

    flush_workers()
    flush_redis()

    sentences = read_sentences_from_file(FILE_PATH)
    total_sentences = len(sentences)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)
    lock = threading.Lock()  # Create a lock

    batches_min_start_ts = None
    deadline = None

    function_metrics = defaultdict(lambda: defaultdict(list))
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, sentences, atomic_count, input_batch) for _ in range(WORKER_NUM)]

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

                    actual_times, app_metrics, msg_start_ts, msg_finish_ts = get_faasm_metrics_from_json(batch_result_json, deadline, True)
                    for actual_time in actual_times.values():
                        total_time += actual_time
                        total_count += 1
                    for func_name, metrics in app_metrics.items():
                        for metric_name, times in metrics.items():
                            function_metrics[func_name][metric_name].extend(times)

            except Exception as exc:
                print(f"Generated an exception: {exc}")

    average_time = total_time / total_count if total_count > 0 else 0
    metrics_str = ""
    metrics_str += f"Total messages sent: {total_count}\n"
    metrics_str += f"Average actual time: {average_time} ms\n"

    for func_name, metrics in function_metrics.items():
        metrics_str += f"Metrics for {func_name}:\n"
        for metric_name, times in metrics.items():
            average_metric_time = sum(times) / len(times) if times else 0
            metrics_str += f"  Average {metric_name}: {int(average_metric_time)} μs\n"

    print(metrics_str)

    write_string_to_log(RESULT_FILE, metrics_str)

@task
def state_single(ctx):
    global DURATION
    global RESULT_FILE
    global MAX_INPUT_COUNT

    DURATION = 600
    RESULT_FILE = 'tasks/stream/logs/native_exp_wc_single.txt'
    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    write_string_to_log(RESULT_FILE, "experiment result: wc_state_single_native remote vs local access")

    MAX_INPUT_COUNT = 2000
    repeat = 3
    for i in range(repeat):
        run(ctx)
