import time
from invoke import task
import concurrent.futures
import threading
import re

from tasks.util.faasm import (
    get_faasm_exec_time_from_json,
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


def send_message_and_get_result(input_data):
    msg = {
        "user": "stream",
        "function": "sd_moving_avg",
    }
    result_json = post_async_msg_and_get_result_json(msg, input_list=input_data)
    print(len(result_json))
    actual_time = get_faasm_exec_time_from_json(result_json)
    return actual_time

def worker_thread(end_time, records, atomic_count):
    count = 0
    total_time = 0

    while time.time() < end_time:
        try:
            input_id = atomic_count.get_and_increment()
            input_data = generate_input_data(records, input_id, input_id)
            print(input_data)
            actual_time = send_message_and_get_result(input_data)
            total_time += actual_time
            count += 1
        except Exception as exc:
            print(f"Generated an exception: {exc}")

    return count, total_time

@task(default=True)
def run(ctx):
    """
    Use multiple threads to run the 'wordcount' application and check latency and throughput.
    """
    FILE_PATH = 'tasks/stream/data/data_sensor.txt'
    DURATION = 10 # Seconds
    WORKER_NUM = 5

    records = read_data_from_file(FILE_PATH)
    total_records = len(records)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, records, atomic_count) for _ in range(WORKER_NUM)]

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