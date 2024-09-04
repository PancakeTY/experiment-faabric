from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism, register_function_state, reset_max_replicas
from tasks.util.faasm import (
    get_faasm_metrics_from_json,
    post_async_msg_and_get_result_json,
) 
from tasks.util.thread import AtomicInteger

import time
from invoke import task
import concurrent.futures
import threading
import re
from collections import defaultdict

msg = {}

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
    sentences = [' '.join(words[i:i+10]) for i in range(0, len(words), 10)]
    print(f"Total sentences created: {len(sentences)}")

    return sentences

# Helper function to generate input data ranges
def generate_input_data(sentences, start, size):
    return [{"sentence": sentence} for sentence in sentences[start:start + size]]


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
def run(ctx, batchsize=0, concurrency = 0, inputbatch = 1):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    DURATION = 20 # Seconds
    WORKER_NUM = 20
    
    global msg
    msg = {
    "user": "stream",
    "function": "wordcountindiv_split",
    }
    file_path = 'tasks/stream/data/books.txt'
    sentences = read_sentences_from_file(file_path)
    total_sentences = len(sentences)

    if batchsize > 0:
        reset_batch_size(batchsize)

    if concurrency > 0:
        reset_max_replicas(concurrency)
        
    register_function_state("stream_wordcountindiv_count", "partitionedAttribute", "partitionStateKey")

    flush_workers()
    flush_scheduler()

    # Send one message before the test
    input_data = generate_input_data(sentences, 0, 1)
    chainedId_list = [1]
    result = send_message_and_get_result(input_data, chainedId_list)
    # print(result)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)
    
    lock = threading.Lock()
    batches_min_start_ts = None
    deadline = None
    function_metrics = defaultdict(lambda: defaultdict(list))
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, sentences, atomic_count, inputbatch) for _ in range(WORKER_NUM)]

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