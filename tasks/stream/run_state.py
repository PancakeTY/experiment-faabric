from datetime import datetime
from invoke import task

from tasks.util.planner import run_application_with_input
from tasks.util.faasm import write_string_to_log
from tasks.util.file import read_data_from_txt_file_noparse
from tasks.util.stats import parse_log, average_metrics
from tasks.util.plot import plot_stats

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 15
INPUT_BATCHSIZE = 500
NUM_INPUT_THREADS = 1
APPLICATION_NAME = "state"
INPUT_MSG = {
    "user": "stream",
    "function": "state",
}
RESULT_FILE = "tasks/stream/logs/exp_state_results.txt"
INPUT_MAP = {"attribute": 0}


@task
def run(
    ctx,
    scale,
    batchsize,
    concurrency,
    inputbatch,
    input_rate,
    duration,
    schedule_mode,
    application_name,
    nodes,
    input_msg,
):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global RESULT_FILE, NUM_INPUT_THREADS

    # Prepare the input data
    records = ["value"] * 20000
    persistent_state = {"test": "1"}

    run_application_with_input(
        application_name=application_name,
        nodes=nodes,
        records=records,
        result_file=RESULT_FILE,
        input_map=INPUT_MAP,
        input_msg=input_msg,
        num_input_threads=NUM_INPUT_THREADS,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=inputbatch,
        input_rate=input_rate,
        duration=duration,
        persistent_state=persistent_state,
        schedule_mode=schedule_mode,
        reschedule=False,
    )


@task
def test(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 60
    RESULT_FILE = "tasks/stream/logs/sd_temp_test.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1
    concurrency = 5
    batchsize = 1
    rate = 1
    schedule_mode = 0

    application_name = "stateless"
    node1 = {
        "name": "stream_state_stateless",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["attribute"],
        "successorNode": [],
    }
    nodes = [node1]
    input_msg = {
        "user": "stream",
        "function": "state_stateless",
    }
    timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
    start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, application={application_name}"
    write_string_to_log(RESULT_FILE, start_message)
    # Call the test_contention task with the current batchsize
    run(
        ctx,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=INPUT_BATCHSIZE,
        input_rate=rate,
        duration=DURATION,
        schedule_mode=schedule_mode,
        application_name=application_name,
        nodes=nodes,
        input_msg=input_msg,
    )

    # ------------------------------------------------
    # remote once
    # ------------------------------------------------

    application_name = "remote_once"
    node1 = {
        "name": "stream_state_remote_once",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["attribute"],
        "successorNode": [],
    }
    nodes = [node1]
    input_msg = {
        "user": "stream",
        "function": "state_remote_once",
    }
    timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
    start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, application={application_name}"
    write_string_to_log(RESULT_FILE, start_message)
    # Call the test_contention task with the current batchsize
    run(
        ctx,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=INPUT_BATCHSIZE,
        input_rate=rate,
        duration=DURATION,
        schedule_mode=schedule_mode,
        application_name=application_name,
        nodes=nodes,
        input_msg=input_msg,
    )

    # ------------------------------------------------
    # remote twice
    # ------------------------------------------------

    application_name = "remote_twice"
    node1 = {
        "name": "stream_state_remote_twice",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["attribute"],
        "successorNode": [],
    }
    nodes = [node1]
    input_msg = {
        "user": "stream",
        "function": "state_remote_twice",
    }
    timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
    start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, application={application_name}"
    write_string_to_log(RESULT_FILE, start_message)
    # Call the test_contention task with the current batchsize
    run(
        ctx,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=INPUT_BATCHSIZE,
        input_rate=rate,
        duration=DURATION,
        schedule_mode=schedule_mode,
        application_name=application_name,
        nodes=nodes,
        input_msg=input_msg,
    )

    # ------------------------------------------------
    # local once
    # ------------------------------------------------

    application_name = "local_once"
    node1 = {
        "name": "stream_state_local_once",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["attribute"],
        "successorNode": [],
    }
    nodes = [node1]
    input_msg = {
        "user": "stream",
        "function": "state_local_once",
    }
    timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
    start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, application={application_name}"
    write_string_to_log(RESULT_FILE, start_message)
    # Call the test_contention task with the current batchsize
    run(
        ctx,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=INPUT_BATCHSIZE,
        input_rate=rate,
        duration=DURATION,
        schedule_mode=schedule_mode,
        application_name=application_name,
        nodes=nodes,
        input_msg=input_msg,
    )

    # ------------------------------------------------
    # local twice
    # ------------------------------------------------

    application_name = "local_twice"
    node1 = {
        "name": "stream_state_local_twice",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["attribute"],
        "successorNode": [],
    }
    nodes = [node1]
    input_msg = {
        "user": "stream",
        "function": "state_local_twice",
    }
    timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
    start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, application={application_name}"
    write_string_to_log(RESULT_FILE, start_message)
    # Call the test_contention task with the current batchsize
    run(
        ctx,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=INPUT_BATCHSIZE,
        input_rate=rate,
        duration=DURATION,
        schedule_mode=schedule_mode,
        application_name=application_name,
        nodes=nodes,
        input_msg=input_msg,
    )
