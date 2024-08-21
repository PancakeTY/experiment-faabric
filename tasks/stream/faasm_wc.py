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
def generate_input_data(sentences, start, end):
    return [{"sentence": sentence} for sentence in sentences[start:end + 1]]


def send_message_and_get_result(input_data):
    msg = {
        "user": "stream",
        "function": "wc_split",
    }
    result_json = post_async_msg_and_get_result_json(msg, input_list=input_data)
    print(len(result_json))
    actual_time = get_faasm_exec_time_from_json(result_json)
    return actual_time

def worker_thread(end_time, sentences, atomic_count):
    count = 0
    total_time = 0

    while time.time() < end_time:
        try:
            input_id = atomic_count.get_and_increment()
            input_data = generate_input_data(sentences, input_id, input_id)
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
    FILE_PATH = 'tasks/stream/data/books.txt'
    DURATION = 10 # Seconds
    WORKER_NUM = 2

    sentences = read_sentences_from_file(FILE_PATH)
    total_sentences = len(sentences)

    end_time = time.time() + DURATION
    total_count = 0
    total_time = 0
    atomic_count = AtomicInteger(0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        futures = [executor.submit(worker_thread, end_time, sentences, atomic_count) for _ in range(WORKER_NUM)]

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