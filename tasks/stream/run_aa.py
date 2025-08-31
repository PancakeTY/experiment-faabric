from datetime import datetime
from invoke import task
import json

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.planner import run_application_with_input
from tasks.util.faasm import generate_all_input_batch, write_string_to_log
from tasks.stream.data_generator.aa_data import get_persistent_state
from tasks.util.file import read_data_from_txt_file_noparse
from tasks.util.stats import parse_log, average_metrics
from tasks.util.plot import plot_stats

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 15
INPUT_BATCHSIZE = 500
NUM_INPUT_THREADS = 15
APPLICATION_NAME = "aa_application"
INPUT_FILE = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/aa_dataset.txt"
)
INPUT_MSG = {
    "user": "stream",
    "function": "aa_deserialize",
}
RESULT_FILE = "tasks/stream/logs/exp_aa_results.txt"
INPUT_MAP = {"json": 0}


def enwrap_json(record):
    """
    Enwrap the record into a JSON object.
    """
    ad_id, campaign_id, event_time, event_type = record
    json_str = json.dumps(
        {
            "user_id": "stream",
            "page_id": "page",
            "ad_id": ad_id,
            "ad_type": "ad",
            "event_type": event_type,
            "event_time": event_time,
            "ip_address": "1.1.1.1",
        }
    )
    return [json_str]


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
    pregenerated_work,
):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    # # Prepare the persistent state
    persistent_state = get_persistent_state()

    node1 = {
        "name": "stream_aa_deserialize",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["json"],
        "successorNode": ["stream_aa_filter"],
    }
    node2 = {
        "name": "stream_aa_filter",
        "node_type": "STATELESS",
        "input": None,
        "inputFields": ["ad_id"],
        "successorNode": ["stream_aa_projection"],
    }
    node3 = {
        "name": "stream_aa_projection",
        "node_type": "STATELESS",
        "input": None,
        "inputFields": ["ad_id"],
        "successorNode": ["stream_aa_join"],
    }
    node4 = {
        "name": "stream_aa_join",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "ad_id",
        "input": None,
        "parallelism": scale,
        "inputFields": ["ad_id"],
        "successorNode": ["stream_aa_campaign"],
    }
    node5 = {
        "name": "stream_aa_campaign",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "campaign_id",
        "input": None,
        "parallelism": scale,
        "inputFields": ["stream_campaign_id"],
        "successorNode": [],
    }

    nodes = [node1, node2, node3, node4, node5]

    run_application_with_input(
        application_name=APPLICATION_NAME,
        nodes=nodes,
        result_file=RESULT_FILE,
        pregenerated_work=pregenerated_work,
        num_input_threads=NUM_INPUT_THREADS,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=inputbatch,
        input_rate=input_rate,
        duration=duration,
        persistent_state=persistent_state,
        schedule_mode=schedule_mode,
    )


@task
def test(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE, INPUT_FILE, INPUT_MAP, INPUT_MSG
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs_hs/test_aa.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 5000
    concurrency = 10
    batchsize = 20

    # rates = [2500, 5000, 7500, 10000]
    rates = [50000]
    schedule_modes = [2, 0, 5, 6, 1, 3]

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    # Prepare the input data
    raw_records = read_data_from_txt_file_noparse(INPUT_FILE)
    records = [enwrap_json(record) for record in raw_records]
    pregenerated_work = generate_all_input_batch(
        records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}"
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
                pregenerated_work=pregenerated_work,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def stats(ctx):
    RESULT_FILE = "tasks/stream/logs_hs/test_aa.txt"
    df = parse_log(RESULT_FILE)

    average_metrics(df)

    plot_stats("aa", df)


@task
# inv stream.run-aa.trans-exp
def trans_exp(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE, INPUT_FILE, INPUT_MAP, INPUT_MSG
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs_hs/trans_aa.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1
    concurrency = 1
    batchsize = 1

    rates = [1]
    schedule_modes = [2, 5, 3] * 3

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    # Prepare the input data
    raw_records = read_data_from_txt_file_noparse(INPUT_FILE)
    records = [enwrap_json(record) for record in raw_records]
    pregenerated_work = generate_all_input_batch(
        records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}"
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
                pregenerated_work=pregenerated_work,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def disp(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE, INPUT_FILE, INPUT_MAP, INPUT_MSG
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs_hs/no_period_aa.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 5000
    concurrency = 10
    batchsize = 20

    # rates = [2500, 5000, 7500, 10000]
    rates = [50000]
    schedule_modes = [2, 0, 5, 6, 1, 3]

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    # Prepare the input data
    raw_records = read_data_from_txt_file_noparse(INPUT_FILE)
    records = [enwrap_json(record) for record in raw_records]
    pregenerated_work = generate_all_input_batch(
        records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}"
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
                pregenerated_work=pregenerated_work,
            )
            print(f"Completed test_contention with con: {concurrency}")
