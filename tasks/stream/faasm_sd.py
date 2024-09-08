import time
from invoke import task
import concurrent.futures
import threading
import re

from tasks.util.faasm import (
    get_chained_faasm_exec_time_from_json,
    post_async_msg_and_get_result_json,
)

class AtomicInteger:
    def __init__(self, initial=1):
        self.value = initial
        self._lock = threading.Lock()

    def get_and_increment(self, increment_value=1):
        with self._lock:
            current_value = self.value
            self.value += increment_value
            return current_value

def read_data_from_file(file_path):
    data_vectors = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors

# Helper function to generate input data ranges
def generate_input_data(records, start, end):
    return [{"sensorId": record[3], 
             "temperature": record[4]} for record in records[start:end + 1]]


def send_message_and_get_result(input_id, input_data, input_batch):
    msg = {
        "user": "stream",
        "function": "sd_moving_avg",
    }
    # chained Id should start from 1
    chainedId_list = list(range(input_id + 1, input_id + 1 + input_batch))
    print(input_data)
    # print(chainedId_list)
    result_json = post_async_msg_and_get_result_json(msg, num_messages = input_batch, input_list=input_data, chainedId_list = chainedId_list)
    actual_times, num = get_chained_faasm_exec_time_from_json(result_json)
    return actual_times, num

def worker_thread(end_time, sentences, atomic_count, input_batch):
    count = 0
    total_time = 0

    while time.time() < end_time:
        try:
            input_id = atomic_count.get_and_increment(input_batch)
            input_data = generate_input_data(sentences, input_id, input_id + input_batch -1)
            actual_times, num = send_message_and_get_result(input_id, input_data, input_batch)
            total_time += actual_times
            count += num
        except Exception as exc:
            print(f"Generated an exception: {exc}")

    return count, total_time

@task(default=True)
def run(ctx, input_batch = 1):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    FILE_PATH = 'tasks/stream/data/data_sensor.txt'
    DURATION = 100 # Seconds
    WORKER_NUM = 15

    records = read_data_from_file(FILE_PATH)
    total_records = len(records)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)
    lock = threading.Lock()  # Create a lock

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, records, atomic_count, input_batch) for _ in range(WORKER_NUM)]

        for future in concurrent.futures.as_completed(futures):
            try:
                count, time_spent = future.result()
                with lock:
                    total_count += count
                    total_time += time_spent
            except Exception as exc:
                print(f"Generated an exception: {exc}")

    average_time = total_time / total_count if total_count > 0 else 0

    print(f"Total messages sent: {total_count}")
    print(f"Average actual time: {average_time} ms")