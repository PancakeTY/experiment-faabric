from faasmctl.util.config import (
    get_faasm_ini_file,
    get_faasm_planner_host_port as faasmctl_get_planner_host_port,
)
from faasmctl.util.invoke import invoke_wasm as faasmctl_invoke_wasm
from faasmctl.util.invoke import invoke_wasm_messages as faasmctl_invoke_wasm_messages
from faasmctl.util.invoke import query_result as faasmctl_query_result
from os import environ
from collections import defaultdict
import re
import os
import threading
import time
from google.protobuf.json_format import MessageToDict
import numpy as np
import json

def get_faasm_exec_time_from_json(results_json, check=False):
    """
    Return the execution time (included in Faasm's response JSON) in seconds
    """
    start_ts = min([result_json["start_ts"] for result_json in results_json])
    finish_ts = max([result_json["finish_ts"] for result_json in results_json])

    actual_time = float(int(finish_ts) - int(start_ts)) / 1000
    return actual_time

def get_chained_faasm_exec_time_from_json(results_json, check=False):
    """
    Return the execution time (included in Faasm's response JSON) in milliseconds
    """
    # Group the results by chainedId
    grouped_results = defaultdict(list)
    for result in results_json:
        chained_id = result["chainedId"]
        grouped_results[chained_id].append(result)
    
    # Calculate the execution time for each group
    total_time = 0
    for chained_id, group in grouped_results.items():
        start_ts = min([result["start_ts"] for result in group])
        finish_ts = max([result["finish_ts"] for result in group])
        actual_time = int(finish_ts) - int(start_ts)
        total_time += actual_time
    
    # Return the total execution time and the number of unique chainedIds
    num_chained_ids = len(grouped_results)

    return total_time, num_chained_ids

def get_faasm_exec_milli_time_from_json(results_json, check=False):
    """
    Return the execution time (included in Faasm's response JSON) in milliseconds
    """
    start_ts = min([result_json["start_ts"] for result_json in results_json])
    finish_ts = max([result_json["finish_ts"] for result_json in results_json])

    actual_time = int(finish_ts) - int(start_ts)
    return actual_time

def get_faasm_exec_chained_milli_time_from_json(results_json, check=False):
    """
    Return the execution time (included in Faasm's response JSON) in milliseconds
    """
    # Group the results by chain ID and find the actual_time for each chained functions
    grouped_results = defaultdict(list)
    function_metrics = defaultdict(lambda: defaultdict(list))

    # For each Message
    for result in results_json:
        chained_id = result['chainedId']
        grouped_results[chained_id].append(result)
        # Get the mertics
        planner_queue_time = int(result['plannerQueueTime'])
        planner_pop_time = int(result['plannerPopTime'])
        planner_dispatch_time = int(result['plannerDispatchTime'])
        worker_queue_time = int(result['workerQueueTime'])
        worker_pop_time = int(result['workerPopTime'])
        executor_prepare_time = int(result['ExecutorPrepareTime'])
        worker_execute_start_time = int(result['workerExecuteStart'])
        worker_execute_end_time = int(result['workerExecuteEnd'])
        function_name = "unknown"
        if result.get('parallelismId') is not None:
            function_name = result['user'] + '_' + result['function'] + '_' + str(result['parallelismId'])
        else:
            function_name = result['user'] + '_' + result['function']
        planner_queue_elapse = planner_pop_time - planner_queue_time
        planner_consumed_elapse = planner_dispatch_time - planner_pop_time
        worker_queue_elapse = worker_pop_time - worker_queue_time
        worker_execute_elapse = worker_execute_end_time - worker_execute_start_time
        total_elapse = worker_execute_end_time - planner_queue_time

        function_metrics[function_name]['planner_queue_elapse'].append(planner_queue_elapse)
        function_metrics[function_name]['planner_consumed_elapse'].append(planner_consumed_elapse)
        function_metrics[function_name]['worker_queue_elapse'].append(worker_queue_elapse)
        function_metrics[function_name]['executor_prepare_time'].append(executor_prepare_time)
        function_metrics[function_name]['worker_execute_elapse'].append(worker_execute_elapse)
        function_metrics[function_name]['total_elapse'].append(total_elapse)

    actual_times = {}
    min_start_ts = None
    max_finish_ts = None
    for chained_id, results in grouped_results.items():
        start_ts = int(min(result['start_ts'] for result in results))
        finish_ts = int(max(result['finish_ts'] for result in results))
        if min_start_ts is None or start_ts < min_start_ts:
            min_start_ts = start_ts
        if max_finish_ts is None or finish_ts > max_finish_ts:
            max_finish_ts = finish_ts
        actual_time = finish_ts - start_ts
        actual_times[chained_id] = actual_time

    return actual_times, function_metrics, min_start_ts, max_finish_ts

def get_faasm_metrics_from_json(json_result, deadline, function_include=None, native=False):
    # Group the results by chain ID and find the actual_time for each chained functions
    grouped_results = defaultdict(list)
    function_metrics = defaultdict(lambda: defaultdict(list))
    for msg_result in json_result:
        chained_id = msg_result['chainedId']
        grouped_results[chained_id].append(msg_result)

    msg_start_ts = None
    msg_finish_ts = None

    invalid_chained_ids = []
    actual_times = {}
    for chained_id, results in grouped_results.items():
        start_ts = int(min(result['start_ts'] for result in results))
        finish_ts = int(max(result['finish_ts'] for result in results))
        # If the finish timestamp is greater than the deadline, skip the chained_id
        if finish_ts > deadline:
            invalid_chained_ids.append(chained_id)
            continue
        # If we must include some functions, skip the chained_id if it is not in the list
        if function_include is not None:
            valid = False
            for result in results:
                function_name = result['function']
                if function_name == function_include:
                    valid = True
            if not valid:
                invalid_chained_ids.append(chained_id)
        actual_time = finish_ts - start_ts
        actual_times[chained_id] = actual_time

    # For each Message
    for msg_result in json_result:
        chained_id = msg_result['chainedId']
        if chained_id in invalid_chained_ids:
            # print(f"Skip invalid chained_id: {chained_id}")
            continue
        if msg_start_ts is None or int(msg_result["start_ts"]) < msg_start_ts:
            msg_start_ts = int(msg_result["start_ts"])
        if msg_finish_ts is None or int(msg_result["finish_ts"]) > msg_finish_ts:
            msg_finish_ts = int(msg_result["finish_ts"])
        # Get the mertics
        if native:
            planner_queue_time = int(msg_result.get('plannerQueueTime', 0))
            planner_pop_time = int(msg_result.get('plannerPopTime', 0))
            planner_dispatch_time = int(msg_result.get('plannerDispatchTime', 0))
            worker_queue_time = int(msg_result.get('workerQueueTime', 0))
            worker_pop_time = int(msg_result.get('workerPopTime', 0))
            executor_prepare_time = int(msg_result.get('ExecutorPrepareTime', 0))
            worker_execute_start_time = int(msg_result.get('workerExecuteStart', 0))
            worker_execute_end_time = int(msg_result.get('workerExecuteEnd', 0))
        else:
            planner_queue_time = int(msg_result['plannerQueueTime'])
            planner_pop_time = int(msg_result['plannerPopTime'])
            planner_dispatch_time = int(msg_result['plannerDispatchTime'])
            worker_queue_time = int(msg_result['workerQueueTime'])
            worker_pop_time = int(msg_result['workerPopTime'])
            executor_prepare_time = int(msg_result['ExecutorPrepareTime'])
            worker_execute_start_time = int(msg_result['workerExecuteStart'])
            worker_execute_end_time = int(msg_result['workerExecuteEnd'])
        output_data = ""
        if 'output_data' in msg_result:
            output_data = msg_result['output_data']
        duration = None
        match = re.search(r'duration:(\d+)', output_data)
        if match:
            duration = int(match.group(1))
        # Regular expression to capture the input_size
        input_size = None
        input_match = re.search(r'input_size: (\d+)', output_data)
        if input_match:
            input_size = int(input_match.group(1))
        avg_tuple_duration = None
        if match and input_match and (input_size != 0):
            avg_tuple_duration = float(duration / input_size)

        function_name = "unknown"
        if msg_result.get('parallelismId') is not None:
            function_name = msg_result['user'] + '_' + msg_result['function'] + '_' + str(msg_result['parallelismId'])
        else:
            function_name = msg_result['user'] + '_' + msg_result['function']
        planner_queue_elapse = planner_pop_time - planner_queue_time
        planner_consumed_elapse = planner_dispatch_time - planner_pop_time
        worker_queue_elapse = worker_pop_time - worker_queue_time
        worker_execute_elapse = worker_execute_end_time - worker_execute_start_time
        total_elapse = worker_execute_end_time - planner_queue_time

        function_metrics[function_name]['planner_queue_elapse'].append(planner_queue_elapse)
        function_metrics[function_name]['planner_consumed_elapse'].append(planner_consumed_elapse)
        function_metrics[function_name]['worker_queue_elapse'].append(worker_queue_elapse)
        function_metrics[function_name]['executor_prepare_time'].append(executor_prepare_time)
        function_metrics[function_name]['worker_execute_elapse'].append(worker_execute_elapse)
        function_metrics[function_name]['total_elapse'].append(total_elapse)
        function_metrics[function_name]['count'].append(1)
        if duration is not None:
            function_metrics[function_name]['duration'].append(duration)
        if input_size is not None:
            function_metrics[function_name]['input_size'].append(input_size)
        if avg_tuple_duration is not None:
            function_metrics[function_name]['avg_tuple_duration'].append(avg_tuple_duration)    

    if msg_start_ts is not None and msg_finish_ts is not None:
        function_metrics["application"]["msg_latency"].append((msg_finish_ts - msg_start_ts) * 1000)

    return actual_times, function_metrics, msg_start_ts, msg_finish_ts

def get_faasm_planner_host_port():
    return faasmctl_get_planner_host_port(get_faasm_ini_file())


def get_faasm_version():
    if "FAASM_VERSION" in environ:
        return environ["FAASM_VERSION"]

    return "0.0.0"


def post_async_msg_and_get_result_json(msg, num_message = 1, host_list=None, req_dict=None, input_list=None, chainedId_list = []):
    # print num_message
    result = faasmctl_invoke_wasm(
        msg,
        num_messages=num_message,
        dict_out=True,
        host_list=host_list,
        req_dict=req_dict,
        input_list=input_list,
        chainedId_list=chainedId_list,
        poll_period_in=0.5,
        poll_retry_in=10,
    )
    if "finished" in result:
        is_finished = result["finished"]
    else:
        is_finished = False
    
    return is_finished, result["messageResults"]

def post_async_batch_msg_and_get_result_json(msg, batch_size=100, host_list=None):
    result = faasmctl_invoke_wasm(msg, num_messages=batch_size, dict_out=True, host_list=host_list)
    return result["messageResults"]


def has_app_failed(results_json):
    for result in results_json:
        if "returnValue" not in result:
            # Protobuf may omit zero values when serialising, so sometimes
            # the return value may not be set. So if the key is not there,
            # we assume execution was succesful
            # TODO: make sure return value is always passed
            return False

        if int(result["returnValue"]) != 0:
            return True

    return False
    # return any([result_json["returnValue"] for result_json in results_json])

def post_async_batch_msg(app_id, msg, batch_size=1, input_list=None, chained_id_list= None):
    if batch_size != len(input_list):
        print ("ERROR: batch_size != len(input_data)")
        assert False
    if batch_size != len(chained_id_list):
        print ("ERROR: batch_size != len(chained_id_list)")
        assert False
    chained_id_list = faasmctl_invoke_wasm_messages(app_id, msg_dict=msg, num_messages=batch_size, 
                                              input_list=input_list, chained_id_list=chained_id_list,
                                              num_retries = 10000, sleep_period_secs=0.05)
    return chained_id_list

def write_metrics_to_log(path, function_metrics):
    log_dir = os.path.dirname(path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    with open(path, 'a') as log_file:
        for func_name, metrics in function_metrics.items():
            log_file.write(f"Metrics for {func_name}:\n")
            for metric_name, times in metrics.items():
                if metric_name == "count":
                    metric_value = sum(times)
                    log_file.write(f"  Total count {metric_name}: {metric_value}\n")
                else:
                    average_metric_time = sum(times) / len(times) if times else 0
                    log_file.write(f"  Average {metric_name}: {int(average_metric_time)} μs\n")
        log_file.write("\n")

def write_string_to_log(path, log_message):
    """
    Writes a log message to a log file.
    
    Parameters:
        path (str): The path to the log file.
        log_message (str): The message to be written to the log file.
    """
    try:
        log_dir = os.path.dirname(path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        with open(path, 'a') as log_file:
            log_file.write(f"{log_message}\n")
    except Exception as e:
        print(f"Error writing to log file: {e}")

def generate_input_data(records, start, size, input_map):
    # Use input_map to access the specific indices for each attribute
    return [{key: record[input_map[key]] for key in input_map} for record in records[start:start + size]]

def filter_json_results(json_results):
    filtered_results = []
    for msg_result in json_results:
        # Extract only the required fields
        filtered_result = {
            'chainedId': msg_result.get('chainedId'),
            'start_ts': msg_result.get('start_ts'),
            'finish_ts': msg_result.get('finish_ts'),
            'plannerQueueTime': msg_result.get('plannerQueueTime'),
            'plannerPopTime': msg_result.get('plannerPopTime'),
            'plannerDispatchTime': msg_result.get('plannerDispatchTime'),
            'workerQueueTime': msg_result.get('workerQueueTime'),
            'workerPopTime': msg_result.get('workerPopTime'),
            'ExecutorPrepareTime': msg_result.get('ExecutorPrepareTime'),
            'workerExecuteStart': msg_result.get('workerExecuteStart'),
            'workerExecuteEnd': msg_result.get('workerExecuteEnd'),
            'output_data': msg_result.get('output_data'),
            'user': msg_result.get('user'),
            'function': msg_result.get('function'),
            'parallelismId': msg_result.get('parallelismId')  # This can be None
        }
        filtered_results.append(filtered_result)
    return filtered_results


def get_result_thread(appid_list, appid_list_lock, shared_batches_min_start_ts, start_ts_lock, batches_result, result_lock, end_time):
    # Get next appid to query
    while True:
        with appid_list_lock:
            if not appid_list:
                # If the time exceeds the end time, break the loop
                if time.time() > end_time:
                    break
            else:
                appid = appid_list.pop(0)
        if appid is None:
            time.sleep(1)
            continue

        # Get the result
        ber_status = faasmctl_query_result(appid)
        json_results = MessageToDict(ber_status)["messageResults"]
        filtered_json_results = filter_json_results(json_results)
        start_ts = int(min([result_json["start_ts"] for result_json in filtered_json_results]))
        with start_ts_lock:
            if shared_batches_min_start_ts[0] is None or start_ts < shared_batches_min_start_ts[0]:
                shared_batches_min_start_ts[0] = start_ts
        with result_lock:
            batches_result.append(filtered_json_results)
   
def statistics_result(batches_result, DURATION, function_include=None, native=False):
    function_metrics = defaultdict(lambda: defaultdict(list))
    actual_times_array = []

    all_start_ts = [
    result["start_ts"]
    for json_result in batches_result
    if json_result  
    for result in json_result
    if "start_ts" in result  
    ]

    # Calculate the minimum start_ts if any are found
    if all_start_ts:
        tmp_msg_start_ts = int(min(all_start_ts))

    deadline = tmp_msg_start_ts + DURATION * 1000
    min_start_ts = None
    max_finish_ts = None
    for app_result in batches_result:
        actual_times, app_metrics, msg_start_ts, msg_finish_ts = get_faasm_metrics_from_json(app_result, deadline, function_include, native)
        # Update the minimum start timestamp
        if msg_start_ts is None or msg_finish_ts is None:
            continue
        if min_start_ts is None or msg_start_ts < min_start_ts:
            min_start_ts = msg_start_ts
            deadline = min_start_ts + DURATION * 1000
        # Update the maximum finish timestamp
        if max_finish_ts is None or msg_finish_ts > max_finish_ts:
            max_finish_ts = msg_finish_ts
        actual_times_str = json.dumps(actual_times, indent=4)
        actual_times_array.extend(actual_times.values())
        for func_name, metrics in app_metrics.items():
            for metric_name, times in metrics.items():
                function_metrics[func_name][metric_name].extend(times)
    print(f"Length of actual_times_array: {len(actual_times_array)}")
    np_average_time = np.mean(actual_times_array)
    np_median_time = np.median(actual_times_array)
    np_percentile_99_time = np.percentile(actual_times_array, 99)
    np_percentile_95_time = np.percentile(actual_times_array, 95)
    duration = max_finish_ts - min_start_ts
    np_result_message = (
                "Numpy result: \n"
                f"Total messages processed: {len(actual_times_array)},\n"
                f"Average throughput: {1000*len(actual_times_array)/duration} tuples/s,\n"
                f"Average actual time: {np_average_time} ms,\n"
                f"Median actual time: {np_median_time} ms,\n"
                f"99th percentile actual time: {np_percentile_99_time} ms,\n"
                f"95th percentile actual time: {np_percentile_95_time} ms,\n"
                f"Start timestamp: {min_start_ts}\n"
                f"Finish timestamp: {max_finish_ts}\n"
                f"Actual Duration: {duration} ms\n")
    return np_result_message, function_metrics