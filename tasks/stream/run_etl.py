from datetime import datetime
from invoke import task

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.planner import run_application_with_input
from tasks.util.faasm import generate_all_input_batch, write_string_to_log
from tasks.util.file import (
    read_data_from_txt_file,
    read_persistent_state_from_txt_file,
)
from tasks.util.stats import parse_log, average_metrics
from tasks.util.plot import plot_stats

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 15
INPUT_BATCHSIZE = 500
NUM_INPUT_THREADS = 10
APPLICATION_NAME = "etl_application"
INPUT_FILE = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/etl_dataset.txt"
)
PERSISTENT_OUTPUT_PATH = "/pvol/runtime/experiment-faabric/tasks/stream/data/etl_persistent_data.txt"
INPUT_MSG = {
    "user": "stream",
    "function": "etl_senml_parse",
}
RESULT_FILE = "tasks/stream/logs/exp_etl_results.txt"
INPUT_MAP = {"msg_id": 0, "json": 1}


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
    max_inflight_reqs,
):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    persistent_state = read_persistent_state_from_txt_file(
        PERSISTENT_OUTPUT_PATH
    )

    node1 = {
        "name": "stream_etl_senml_parse",
        "input": True,
        "inputFields": ["msg_id", "json"],
        "successorNode": ["stream_etl_filter_range"],
    }
    node2 = {
        "name": "stream_etl_filter_range",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "obs_type",
        "parallelism": scale,
        "inputFields": ["msg_id", "sensor_id", "meta", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_filter_bloom"],
    }
    node3 = {
        "name": "stream_etl_filter_bloom",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "obs_type",
        "parallelism": scale,
        "inputFields": ["msg_id", "sensor_id", "meta", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_interpolation"],
    }
    node4 = {
        "name": "stream_etl_interpolation",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "obs_type_sensor_id",
        "parallelism": scale,
        "inputFields": [
            "obs_type_sensor_id",
            "msg_id",
            "sensor_id",
            "meta",
            "obs_type",
            "obs_val",
        ],
        "successorNode": ["stream_etl_join"],
    }
    node5 = {
        "name": "stream_etl_join",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "msg_id",
        "parallelism": scale,
        "inputFields": ["msg_id", "sensor_id", "meta", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_annotate"],
    }
    node6 = {
        "name": "stream_etl_annotate",
        "inputFields": ["msg_id", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_csv2ml", "stream_etl_azure"],
    }
    node7 = {
        "name": "stream_etl_csv2ml",
        "inputFields": ["msg_id", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_mqtt"],
    }
    node8 = {
        "name": "stream_etl_mqtt",
        "inputFields": ["msg_id", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_sink"],
    }
    node9 = {
        "name": "stream_etl_azure",
        "node_type": "STATEFUL",
        "parallelism": scale,
        "inputFields": ["msg_id", "obs_type", "obs_val"],
        "successorNode": ["stream_etl_sink"],
    }
    node10 = {
        "name": "stream_etl_sink",
        "inputFields": ["msg_id", "obs_type", "obs_val"],
        "successorNode": [],
    }
    nodes = [
        node1,
        node2,
        node3,
        node4,
        node5,
        node6,
        node7,
        node8,
        node9,
        node10,
    ]

    run_application_with_input(
        application_name=APPLICATION_NAME,
        nodes=nodes,
        pregenerated_work=pregenerated_work,
        result_file=RESULT_FILE,
        num_input_threads=NUM_INPUT_THREADS,
        scale=scale,
        batchsize=batchsize,
        concurrency=concurrency,
        inputbatch=inputbatch,
        input_rate=input_rate,
        duration=duration,
        persistent_state=persistent_state,
        schedule_mode=schedule_mode,
        max_inflight_reqs=max_inflight_reqs,
    )


@task
def test(ctx, scale=5):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/test_etl.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 1
    rates = [15000000]
    schedule_modes = [7, 5, 3]
    schedule_modes = [5] * 3

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 0)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 30
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def stats(ctx):
    RESULT_FILE = "tasks/stream/logs_hs/test_etl.txt"
    df = parse_log(RESULT_FILE)

    average_metrics(df)

    valid_modes = [2, 3, 5]

    # Filter the DataFrame to preserve only the rows with the valid modes.
    # Note: Replace 'schedule_mode' with the actual column name if it's different.
    df = df[df["schedulemode"].isin(valid_modes)]

    # 2. Create a dictionary to map the mode numbers to their names.
    scheduler_map = {
        2: "Centralized Scheduler",
        3: "FaaSFlow",
        5: "Our Method",
    }

    # 3. Create a new column 'scheduler_name' with the descriptive names.
    df["schedulemode"] = df["schedulemode"].map(scheduler_map)

    plot_stats("etl", df)


@task
# inv stream.run-etl.trans-exp
def trans_exp(ctx, scale=1):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/trans_etl.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1
    concurrency = 1
    batchsize = 1

    max_inflight_reqs = 1

    # rates = [2500, 5000, 7500, 10000]
    rates = [20]
    schedule_modes = [7, 5, 3] * 3

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    reset_stream_parameter("runtime_reconfig", 0)
    reset_stream_parameter("planner_call_interval", 0)
    reset_stream_parameter("runtime_reconfig_period", 100000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("max_inflight_reqs", 1000)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def reconfig(ctx, scale=5):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_etl_config.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 1
    rates = [15000000]
    schedule_modes = [5] * 2

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 30
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def noreconfig(ctx, scale=5):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_etl_noconfig.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 0
    rates = [15000000]
    schedule_modes = [5]

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 30
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def cpu(ctx, scale=5):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/cpu_etl.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 500
    concurrency = 5
    batchsize = 1

    max_inflight_reqs = 5000

    runtime_reconfig = 1
    rates = [15000000]
    schedule_modes = [5] * 1

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 0)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 30
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def test3(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/test_etl_3.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 1
    rates = [15000000]
    schedule_modes = [7, 5, 3]

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 0)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_data_from_txt_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 30
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
                max_inflight_reqs=max_inflight_reqs,
            )
            print(f"Completed test_contention with con: {concurrency}")
