import time
from collections import defaultdict
from invoke import task
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism
from faasmctl.util.invoke import query_result
import concurrent.futures
from google.protobuf.json_format import MessageToDict
import threading
import re

from tasks.util.faasm import (
    get_faasm_exec_chained_milli_time_from_json,
    get_faasm_metrics_from_json,
    post_async_batch_msg_and_get_result_json,
    post_async_batch_msg,
)

def send_message_and_get_result(size=50):
    msg = {
        "user": "stream",
        "function": "wordcount_source",
        # "function": "function_parstate_source",
    }
    result_json = post_async_batch_msg_and_get_result_json(msg,size)
    msg_actual_times, msg_function_metrics , unused1, unuesd2= get_faasm_exec_chained_milli_time_from_json(result_json)
    return msg_actual_times, msg_function_metrics

def worker_thread(end_time):
    worker_count = 0
    worker_total_time = 0
    worker_function_metrics = defaultdict(lambda: defaultdict(list))

    while time.time() < end_time:
        try:
            result, func_metrics = send_message_and_get_result(size=100)
            for actual_time in result.values():
                worker_total_time += actual_time
                worker_count += 1
            for func_name, metrics in func_metrics.items():
                for metric_name, times in metrics.items():
                    worker_function_metrics[func_name][metric_name].extend(times)
        except Exception as exc:
            print(f"Generated an exception: {exc}")
    return worker_count, worker_total_time, worker_function_metrics

@task(default=True)
def test(ctx, scale=3, batchsize=0):
    """
    Use ten threads to run the 'hello' demo and check latency and throughput.
    In the next 5 minutes, send messages to the 'hello' function using ten threads.
    """
    flush_workers()
    flush_scheduler()
    send_message_and_get_result(1)
    scale_function_parallelism("stream", "wordcount_count" ,scale)

    end_time = time.time() + 10  # Running for 5 minutes
    total_count = 0
    total_time = 0
    worker_num = 30
    if batchsize > 0:
        reset_batch_size(batchsize)

    function_metrics = defaultdict(lambda: defaultdict(list))

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_num) as executor:
        futures = [executor.submit(worker_thread, end_time) for _ in range(worker_num)]

        for future in concurrent.futures.as_completed(futures):
            try:
                count, time_spent, func_metrics = future.result()
                total_count += count
                total_time += time_spent
                for func_name, metrics in func_metrics.items():
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

def send_message_without_result(size=50):
    msg = {
        "user": "stream",
        "function": "wordcount_source",
    }
    result_json = post_async_batch_msg_and_get_result_json(msg,size)
    actual_times, function_metrics, unused1, unused2 = get_faasm_exec_chained_milli_time_from_json(result_json)
    return actual_times, function_metrics

# Function to read sentences from the file
def read_sentences_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            text = file.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

    # Split text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    print(f"Total sentences extracted: {len(sentences)}")

    # Replace multiple spaces with a single space and filter sentences
    cleaned_sentences = [
        re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', s)).strip()
        for s in sentences
    ]
    print(f"Cleaned and filtered sentences: {len(cleaned_sentences)}")

    return cleaned_sentences

# Helper function to generate input data ranges
def generate_input_data(sentences, start, end):
    return [{"sentence": sentence} for sentence in sentences[start:end + 1]]

@task
def test_contention(ctx, scale=3, batchsize=0):
    """
    Test the 'wordcount' function with resource contention.
    """
    file_path = 'tasks/stream_hello/pg-being_ernest.txt'
    sentences = read_sentences_from_file(file_path)
    total_sentences = len(sentences)

    flush_workers()
    flush_scheduler()

    msg = {
        "user": "stream",
        "function": "wordcount_split",
    }
    input_data = generate_input_data(sentences, 0, 0)
    appid = 1
    print(input_data)
    appid = post_async_batch_msg(appid, msg, 1, input_data)
    query_result(appid)

    scale_function_parallelism("stream", "wordcount_count" ,scale)

    if batchsize > 0:
        reset_batch_size(batchsize)

    limit_time = 20

    appid = 100000
    appid_list = []
    
    start_time = time.time()
    end_time = start_time + limit_time
    batch_start = 0
    batch_size = 100

    # Invoke the function in batches
    while time.time() < end_time and batch_start < len(sentences):
        batch_end = min(batch_start + batch_size - 1, total_sentences - 1)
        input_data = generate_input_data(sentences, batch_start, batch_end)
        appid_return = post_async_batch_msg(appid, msg, batch_end - batch_start + 1, input_data)
        if appid_return is not None:
            appid_list.append(appid_return)
            appid += 1
        batch_start += batch_size
        if batch_start + batch_size >= total_sentences -1:
            batch_start = 0  # Restart from the beginning if the end is reached

    def get_result_thread(appid):
        try:
            ber_status = query_result(appid)
            json_results = MessageToDict(ber_status)
            print(f"Got result for appid {appid}")
            return json_results["messageResults"]
        except Exception as exc:
            print(f"Generated an exception: {exc}")
            return {}, defaultdict(lambda: defaultdict(list))

    # unit second
    total_count = 0
    total_time = 0
    function_metrics = defaultdict(lambda: defaultdict(list))
    lock = threading.Lock()
    batches_min_start_ts = None

    print("get result and minimum start time")
    batches_result = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_result_thread, appid) for appid in appid_list]
        for future in concurrent.futures.as_completed(futures):
            # get the min_start_ts
            try:
                json_results = future.result()
                start_ts = int(min([result_json["start_ts"] for result_json in json_results]))

                with lock:
                    if batches_min_start_ts is None or start_ts < batches_min_start_ts:
                        batches_min_start_ts = start_ts
                batches_result.append(json_results)

            except Exception as exc:
                print(f"Get minimum start time generated an exception: {exc}")
    print(f"Minimum start time: {batches_min_start_ts}")

    deadline = batches_min_start_ts + limit_time * 1000
    print(f"Deadline: {deadline}")
    for app_result in batches_result:
        actual_times, app_metrics = get_faasm_metrics_from_json(app_result, deadline)
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