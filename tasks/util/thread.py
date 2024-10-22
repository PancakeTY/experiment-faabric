from tasks.util.faasm import generate_input_data, post_async_batch_msg

import threading
import time
from queue import Queue, Empty


class AtomicInteger:
    def __init__(self, initial=1):
        self.value = initial
        self._lock = threading.Lock()

    def get_and_increment(self, increment_value=1):
        with self._lock:
            current_value = self.value
            self.value += increment_value
            return current_value


def batch_producer(records, atomic_count, input_batchsize, input_map, batch_queue, end_time, rate, num_consumers):
    batch_interval = input_batchsize / rate  # Interval between batches in seconds
    next_batch_time = time.time()
    total_items_produced = 0  # Initialize a counter for total items produced

    while time.time() < end_time:
        now = time.time()
        sleep_time = next_batch_time - now
        if sleep_time > 0:
            time.sleep(sleep_time)
            now = time.time()  # Update now after sleeping
        
        break_flag = False
        # Produce batches until we're back on schedule
        while now >= next_batch_time:
            input_index = atomic_count.get_and_increment(input_batchsize)
            if input_index + input_batchsize >= len(records):
                break_flag = True
                break
            input_data_list = generate_input_data(records, input_index, input_batchsize, input_map)
            chained_id_list = [input_index + i for i in range(input_batchsize)]
            # Put batch data into queue
            batch_queue.put((input_index, input_data_list, chained_id_list))
            total_items_produced += input_batchsize
            # Update next_batch_time for the next batch
            next_batch_time += batch_interval
            # Update now to reflect the current time after processing
            now = time.time()

        if break_flag:
            break

    # After production is done, signal consumers to exit
    for _ in range(num_consumers):
        batch_queue.put(None)
    print(f"Batch producer finished. Total items produced: {total_items_produced}")
    return total_items_produced


def batch_consumer(batch_queue, appid_list, appid_list_lock, INPUT_MSG, input_batchsize, end_time):
    while True:
        batch_data = batch_queue.get()  # Blocking call, waits indefinitely
        if batch_data is None:
            # Sentinel value received, exit the thread
            batch_queue.task_done()
            break  # Use break instead of return for clarity
        input_index, input_data, chained_id_list = batch_data
        # Send batch async message
        appid_return = post_async_batch_msg(input_index, INPUT_MSG, input_batchsize, input_data, chained_id_list)
        # Update shared resources safely
        if appid_return is not None:
            with appid_list_lock:
                appid_list.append(appid_return)
        batch_queue.task_done()

        # If the end time is reached, return.
        now = time.time()
        if now > end_time + 1:
            break
