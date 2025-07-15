from datetime import datetime
from invoke import task

from faasmctl.util.planner import reset_stream_parameter
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
NUM_INPUT_THREADS = 10
APPLICATION_NAME = "sd_application"
INPUT_FILE = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/data_sensor_sorted.txt"
)
INPUT_MSG = {
    "user": "stream",
    "function": "sd_moving_avg",
}
RESULT_FILE = "tasks/stream/logs/exp_sd_results.txt"
INPUT_MAP = {"sensor_id": 3, "temperature": 4}


@task
def run(ctx, scale, batchsize, concurrency, inputbatch, input_rate, duration):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)

    print(records[0])  # Print first 10 records for debugging

    node1 = {
        "name": "stream_sd_moving_avg",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "sensor_id",
        "parallelism": scale,
        "input": True,
        "inputFields": ["sensor_id", "temperature"],
        "successorNode": ["stream_sd_spike_detect"],
    }
    node2 = {
        "name": "stream_sd_spike_detect",
        "node_type": "STATELESS",
        "inputFields": ["sensor_id", "temperature", "movingAverage"],
        "successorNode": [],
    }
    nodes = [node1, node2]

    run_application_with_input(
        application_name=APPLICATION_NAME,
        nodes=nodes,
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
def test(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs/sd_temp_test.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 10000
    concurrency = 5
    batchsize = 20

    # rates = [2500, 5000, 7500, 10000]
    rates = [100000]
    schedule_modes = [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2]

    for schedule_mode in schedule_modes:
        reset_stream_parameter("schedule_mode", schedule_mode)

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
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def stats(ctx):
    RESULT_FILE = "tasks/stream/logs/sd_temp_test.txt"
    df = parse_log(RESULT_FILE)

    average_metrics(df)

    plot_stats("sd", df)
