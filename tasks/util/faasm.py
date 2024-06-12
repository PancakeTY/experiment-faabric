from faasmctl.util.config import (
    get_faasm_ini_file,
    get_faasm_planner_host_port as faasmctl_get_planner_host_port,
)
from faasmctl.util.invoke import invoke_wasm as faasmctl_invoke_wasm
from os import environ
from collections import defaultdict


def get_faasm_exec_time_from_json(results_json, check=False):
    """
    Return the execution time (included in Faasm's response JSON) in seconds
    """
    start_ts = min([result_json["start_ts"] for result_json in results_json])
    finish_ts = max([result_json["finish_ts"] for result_json in results_json])

    actual_time = float(int(finish_ts) - int(start_ts)) / 1000
    return actual_time

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

    for result in results_json:
        chained_id = result['chainedId']
        grouped_results[chained_id].append(result)
        planner_queue_time = int(result['plannerQueueTime'])
        planner_pop_time = int(result['plannerPopTime'])
        planner_dispatch_time = int(result['plannerDispatchTime'])
        worker_queue_time = int(result['workerQueueTime'])
        worker_pop_time = int(result['workerPopTime'])
        executor_prepare_time = int(result['ExecutorPrepareTime'])
        worker_execute_start_time = int(result['workerExecuteStart'])
        worker_execute_end_time = int(result['workerExecuteEnd'])

        function_name = result['user'] + '_' + result['function']
        # function_name = result['user'] + '_' + result['function'] + '_' + result['parallelismId']
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
    for chained_id, results in grouped_results.items():
        start_ts = min(result['start_ts'] for result in results)
        finish_ts = max(result['finish_ts'] for result in results)
        actual_time = int(finish_ts) - int(start_ts)
        actual_times[chained_id] = actual_time

    return actual_times, function_metrics


def get_faasm_planner_host_port():
    return faasmctl_get_planner_host_port(get_faasm_ini_file())


def get_faasm_version():
    if "FAASM_VERSION" in environ:
        return environ["FAASM_VERSION"]

    return "0.0.0"


def post_async_msg_and_get_result_json(msg, host_list=None):
    result = faasmctl_invoke_wasm(msg, num_messages=100, dict_out=True, host_list=host_list)
    return result["messageResults"]

def post_async_batch_msg_and_get_result_json(msg, batch_size=100, host_list=None):
    result = faasmctl_invoke_wasm(msg, num_messages=batch_size, dict_out=True, host_list=host_list)
    return result["messageResults"]
