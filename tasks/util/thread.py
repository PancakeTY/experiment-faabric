from tasks.util.faasm import (
    generate_input_data,
    post_async_batch_msg,
    post_async_msg_and_get_result_json,
)
from faasmctl.util.invoke import invoke_by_consumer

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


def token_producer(token_queue, rate, batch_size, duration, num_producers):
    """
    Produces tokens at a fixed rate to control the producers.
    Each token represents one batch.
    """
    start_time = time.time()
    end_time = start_time + duration

    # Calculate the interval between each batch to meet the overall item rate
    batches_per_second = rate / batch_size
    interval = 1.0 / batches_per_second if batches_per_second > 0 else 0

    while time.time() < end_time:
        if interval > 0:
            time.sleep(interval)
        token_queue.put(1)  # The token can be any value, it's just a signal

    # Signal all producer threads to stop
    for _ in range(num_producers):
        token_queue.put(None)


def batch_producer(
    pregenerated_work,
    token_queue,
    batch_queue,
    atomic_counter,
    num_consumers,
    end_time,
):
    """
    Waits for a token, then fetches a pre-generated batch and puts it on the
    work queue for consumers. Returns the number of batches produced.
    """
    batches_produced = 0

    try:
        while time.time() < end_time:
            token = token_queue.get()
            if token is None:
                break

            work_index = atomic_counter.get_and_increment()
            if work_index >= len(pregenerated_work):
                break

            batch_queue.put(pregenerated_work[work_index])
            batches_produced += 1
    finally:
        # ✨ This block is crucial. It runs when the loop breaks.
        # Signal all consumer threads that production is finished.
        print(
            f"Producer finished. Signaling {num_consumers} consumers to stop."
        )
        for _ in range(num_consumers):
            batch_queue.put(None)

    return batches_produced


def batch_consumer(batch_queue, end_time):
    # This consumer now takes the planner URL directly
    # local_appid_list = []
    while time.time() < end_time:
        work_item = batch_queue.get()
        if work_item is None:
            # Sentinel value received, exit the thread
            batch_queue.task_done()
            break

        app_id, msg_json = work_item

        print(f"Processing app_id: {app_id} ")
        invoke_by_consumer(
            msg_json,
            num_retries=1000,
            sleep_period_secs=0.1,
            end_time=end_time,
        )

        # Append to a local list to minimize locking
        # local_appid_list.append(app_id)
        batch_queue.task_done()

    # After the loop, acquire the lock ONCE to update the global list
    # if local_appid_list:
    #     with appid_list_lock:
    #         appid_list.extend(local_appid_list)


def native_batch_consumer(
    batch_queue,
    result_list,
    result_list_lock,
    INPUT_MSG,
    input_batchsize,
    end_time,
):
    while True:
        batch_data = batch_queue.get()  # Blocking call, waits indefinitely
        if batch_data is None:
            # Sentinel value received, exit the thread
            batch_queue.task_done()
            break  # Use break instead of return for clarity
        input_index, input_data, chained_id_list = batch_data
        # Send batch async message
        is_finished, result_json = post_async_msg_and_get_result_json(
            INPUT_MSG,
            num_message=input_batchsize,
            input_list=input_data,
            chainedId_list=chained_id_list,
        )

        if is_finished:
            with result_list_lock:
                result_list.append(result_json)
        batch_queue.task_done()
        # If the end time is reached, return.
        now = time.time()
        if now > end_time + 1:
            break
