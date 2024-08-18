import time
from collections import defaultdict
from invoke import task
from faasmctl.util.flush import flush_workers, flush_scheduler
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism
from faasmctl.util.invoke import query_result
import concurrent.futures
from google.protobuf.json_format import MessageToDict
import threading
import csv

from tasks.util.faasm import (
    get_faasm_metrics_from_json,
    post_async_batch_msg,
)

def read_data_from_file(file_path):
    records = []
    with open(file_path, 'r') as file:  # Open the file
        reader = csv.reader(file)
        for data in reader:
            records.append(data)
    return records

# Helper function to generate input data ranges
# machine_id, time_stamp, cpu, mem
def generate_input_data(records, start, end):
    return [{"machineId": record[0].split('_')[1], 
             "cpu": record[2], 
             "mem": record[3], 
             "timestamp": record[1]} for record in records[start:end + 1]]

@task
def test_contention(ctx, scale=1, batchsize=50):
    """
    Test the 'machine outlier' function with resource contention.
    """
    file_path = 'tasks/stream/data/machine_usage.csv'
    records = read_data_from_file(file_path)
    records_len = len(records)

    flush_workers()
    flush_scheduler()

    msg = {
        "user": "stream",
        "function": "mo_score",
    }

    input_data = generate_input_data(records, 0, 0)
    appid = 1
    print(input_data)
    appid = post_async_batch_msg(appid, msg, 1, input_data)
    query_result(appid)

    # if scale > 1:
    #     scale_function_parallelism("stream", "wordcount_countindiv" ,scale)

    # if batchsize > 0:
    #     reset_batch_size(batchsize)

    limit_time = 10

    appid = 100000
    appid_list = []
    
    start_time = time.time()
    end_time = start_time + limit_time
    batch_start = 0
    batch_size = 100

    # Invoke the function in batches
    while time.time() < end_time and batch_start < records_len:
        batch_end = min(batch_start + batch_size - 1, records_len - 1)
        input_data = generate_input_data(records, batch_start, batch_end)
        appid_return = post_async_batch_msg(appid, msg, batch_end - batch_start + 1, input_data)
        if appid_return is not None:
            appid_list.append(appid_return)
            appid += 1
        batch_start += batch_size
        if batch_start + batch_size >= records_len -1:
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