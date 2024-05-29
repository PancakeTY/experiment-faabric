import time
from invoke import task
import concurrent.futures

from tasks.util.faasm import (
    get_faasm_exec_chained_milli_time_from_json,
    post_async_batch_msg_and_get_result_json,
)

def send_message_and_get_result():
    msg = {
        "user": "stream",
        "function": "wordcount_source",
        # "function": "function_parstate_source",
    }
    result_json = post_async_batch_msg_and_get_result_json(msg,100)
    actual_times = get_faasm_exec_chained_milli_time_from_json(result_json)
    # print(f"Actual times: {actual_times}")
    return actual_times

def worker_thread(end_time):
    count = 0
    total_time = 0

    while time.time() < end_time:
        try:
            result = send_message_and_get_result()
            for actual_time in result.values():
                total_time += actual_time
                count += 1
        except Exception as exc:
            print(f"Generated an exception: {exc}")

    return count, total_time

@task(default=True)
def test(ctx):
    """
    Use ten threads to run the 'hello' demo and check latency and throughput.
    In the next 5 minutes, send messages to the 'hello' function using ten threads.
    """
    end_time = time.time() + 10  # Running for 5 minutes
    total_count = 0
    total_time = 0
    worker_num = 5

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_num) as executor:
        futures = [executor.submit(worker_thread, end_time) for _ in range(worker_num)]

        for future in concurrent.futures.as_completed(futures):
            try:
                count, time_spent = future.result()
                total_count += count
                total_time += time_spent
            except Exception as exc:
                print(f"Generated an exception: {exc}")

    average_time = total_time / total_count if total_count > 0 else 0

    print(f"Total messages sent: {total_count}")
    print(f"Average actual time: {average_time} ms")