import time
import threading
from datetime import datetime
from invoke import task
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.planner import run_application_with_input
from tasks.util.faasm import write_string_to_log
from tasks.util.stats import extract_data
from tasks.util.file import read_data_from_txt_file

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 15
INPUT_BATCHSIZE = 500
NUM_INPUT_THREADS = 10
APPLICATION_NAME = "nwm_application"
INPUT_FILE = '/pvol/runtime/experiment-faabric/tasks/stream/data/nwm_dataset.txt'
INPUT_MSG = {
    "user": "stream",
    "function": "nwm_parse_lines",
}
RESULT_FILE = 'tasks/stream/logs/exp_nwm_results.txt'
INPUT_MAP = {"json": 0}

@task
def run(ctx, scale, batchsize, concurrency, inputbatch, input_rate, duration):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)

    print(records[0])  # Print first 10 records for debugging

    node1 = {"name": "stream_nwm_parse_lines",
             "input": True,
             "inputFields": ["json"],
             "successorNode": ["stream_nwm_split"],
            }
    node2 = {"name": "stream_nwm_split",
             "inputFields": ["host", "status", "method", "region", "event_time"],
             "successorNode": ["stream_nwm_success_filter", "stream_nwm_fail_filter"],
            }
    node3 = {"name": "stream_nwm_success_filter",
             "inputFields": ["host", "status", "method", "region", "event_time"],
             "successorNode": ["stream_nwm_success_parse"],
            }
    node4 = {"name": "stream_nwm_success_parse",
             "inputFields": ["host", "status", "method", "region", "event_time"],
             "successorNode": ["stream_nwm_join"],
            }
    node5 = {"name": "stream_nwm_fail_filter",
             "inputFields": ["host", "status", "method", "region", "event_time"],
             "successorNode": ["stream_nwm_fail_parse"],
            }
    node6 = {"name": "stream_nwm_fail_parse",
             "inputFields": ["host", "status", "method", "region", "event_time"],
             "successorNode": ["stream_nwm_fail_aggregation"],
            }
    node7 = {"name": "stream_nwm_fail_aggregation",
             "node_type": "PARTITIONED_STATEFUL",
             "partitionBy": "host",             
             "parallelism": scale,
             "inputFields": ["host", "event_time"],
             "successorNode": ["stream_nwm_fail_aggrfilter"],
            }
    node8 = {"name": "stream_nwm_fail_aggrfilter",
             "inputFields": ["host", "event_time"],
             "successorNode": ["stream_nwm_fail_functor"],
            }
    node9 = {"name": "stream_nwm_fail_functor",
             "node_type": "PARTITIONED_STATEFUL",
             "partitionBy": "host",             
             "parallelism": scale,
             "inputFields": ["host", "event_time"],
             "successorNode": ["stream_nwm_join"],
            }
    node10 = {"name": "stream_nwm_join",
             "node_type": "PARTITIONED_STATEFUL",
             "partitionBy": "host",             
             "parallelism": scale,
             "inputFields": ["host", "event_time", "type", "is_first"],
             "successorNode": [],
            }            
    nodes = [node1, node2, node3, node4, node5, node6, node7, node8, node9, node10]

    run_application_with_input(
        application_name = APPLICATION_NAME,
        nodes = nodes,
        records=records,
        result_file=RESULT_FILE,
        input_map=INPUT_MAP,
        input_msg=INPUT_MSG,
        num_input_threads=NUM_INPUT_THREADS,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=inputbatch,
        input_rate=input_rate,
        duration=duration,
    )

@task
def test(ctx, scale=10):
    global DURATION , INPUT_BATCHSIZE
    global RESULT_FILE
    
    DURATION = 60
    RESULT_FILE = 'tasks/stream/logs/nwm_temp_test.txt'

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 10000
    concurrency = 10
    batchsize = 20

    # rates = [2500, 5000, 7500, 10000]
    rates = [50000]
    schedule_modes = [0,1,2,0,1,2]
    
    for schedule_mode in schedule_modes:
        reset_stream_parameter("schedule_mode", schedule_mode)
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}"   
            write_string_to_log(RESULT_FILE, start_message)
            # Call the test_contention task with the current batchsize
            run(ctx, scale=scale, batchsize=batchsize, concurrency=concurrency, inputbatch=INPUT_BATCHSIZE, input_rate=rate, duration=DURATION)
            print(f"Completed test_contention with con: {concurrency}")
