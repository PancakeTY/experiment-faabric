import time
from collections import defaultdict
from invoke import task
from faasmctl.util.flush import flush_workers
from faasmctl.util.planner import reset_batch_size, scale_function_parallelism
import concurrent.futures

from tasks.util.faasm import (
    get_faasm_exec_chained_milli_time_from_json,
    post_async_batch_msg_and_get_result_json,
)

def send_message_and_get_result(size=50):
    msg = {
        "user": "stream",
        "function": "wordcount_source",
        # "function": "function_parstate_source",
    }
    result_json = post_async_batch_msg_and_get_result_json(msg,size)
    actual_times, function_metrics = get_faasm_exec_chained_milli_time_from_json(result_json)
    return actual_times, function_metrics

def worker_thread(end_time):
    count = 0
    total_time = 0
    function_metrics = defaultdict(lambda: defaultdict(list))

    while time.time() < end_time:
        try:
            result, func_metrics = send_message_and_get_result(size=100)
            for actual_time in result.values():
                total_time += actual_time
                count += 1
            for func_name, metrics in func_metrics.items():
                for metric_name, times in metrics.items():
                    function_metrics[func_name][metric_name].extend(times)
        except Exception as exc:
            print(f"Generated an exception: {exc}")
    return count, total_time, function_metrics

@task(default=True)
def test(ctx, scale=3, batchsize=0):
    """
    Use ten threads to run the 'hello' demo and check latency and throughput.
    In the next 5 minutes, send messages to the 'hello' function using ten threads.
    """
    flush_workers()
    send_message_and_get_result(1)
    scale_function_parallelism("stream", "wordcount_count" ,scale)

    end_time = time.time() + 10  # Running for 5 minutes
    total_count = 0
    total_time = 0
    worker_num = 20
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
