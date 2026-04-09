from datetime import datetime
from invoke import task

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.planner import run_application_with_input
from tasks.util.faasm import generate_all_input_batch, write_string_to_log
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


def run(
    pregenerated_work,
    input_rate,
    inputbatch,
    batchsize,
    scale,
    concurrency,
    max_inflight_reqs,
    max_waiting_queue_size,
    num_hosts_scheduled_in,
    schedule_mode,
):
    """Input rate unit: data/second"""
    global APPLICATION_NAME, INPUT_MSG, RESULT_FILE, NUM_INPUT_THREADS

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
        result_file=RESULT_FILE,
        num_input_threads=NUM_INPUT_THREADS,
        duration=DURATION,
        pregenerated_work=pregenerated_work,
        input_rate=input_rate,
        inputbatch=inputbatch,
        batchsize=batchsize,
        concurrency=concurrency,
        max_inflight_reqs=max_inflight_reqs,
        max_waiting_queue_size=max_waiting_queue_size,
        num_hosts_scheduled_in=num_hosts_scheduled_in,
        schedule_mode=schedule_mode,
    )


@task
def test(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 200
    RESULT_FILE = "tasks/stream/logs_hs/test_sd_5.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 15000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 150000

    runtime_reconfig = 1
    rates = [20000000]
    schedule_modes = [7, 5, 3]

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, runtime_reconfig={runtime_reconfig}"
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
def test20(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 200
    RESULT_FILE = "tasks/stream/logs_hs/test_sd_20.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 30000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 300000

    runtime_reconfig = 1
    rates = [20000000]
    schedule_modes = [7, 5, 3]

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, runtime_reconfig={runtime_reconfig}"
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
    RESULT_FILE = "tasks/stream/logs_hs/test_sd.txt"
    df = parse_log(RESULT_FILE)

    average_metrics(df)

    valid_modes = [7, 3, 5]

    # Filter the DataFrame to preserve only the rows with the valid modes.
    # Note: Replace 'schedule_mode' with the actual column name if it's different.
    df = df[df["schedulemode"].isin(valid_modes)]

    # 2. Create a dictionary to map the mode numbers to their names.
    scheduler_map = {
        7: "Centralized Scheduler",
        3: "FaaSFlow",
        5: "Our Method",
    }

    # 3. Create a new column 'scheduler_name' with the descriptive names.
    df["schedulemode"] = df["schedulemode"].map(scheduler_map)

    plot_stats("sd", df)


@task
def trans_exp(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/trans_sd.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1
    concurrency = 1
    batchsize = 1

    max_inflight_reqs = 1

    rates = [20]
    schedule_modes = [7, 5, 3] * 3

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    reset_stream_parameter("runtime_reconfig", 0)
    reset_stream_parameter("planner_call_interval", 0)
    reset_stream_parameter("runtime_reconfig_period", 1000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("max_inflight_reqs", 1000)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
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
def disp(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs_hs/no_period_sd.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 12500
    concurrency = 5
    batchsize = 20

    rates = [125000]
    schedule_modes = [2, 0, 5, 6, 1, 3]

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
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
def reconfig(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_sd_config.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 15000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 150000

    runtime_reconfig = 1
    rates = [15000000]
    schedule_modes = [5] * 3

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, runtime_reconfig={runtime_reconfig}"
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
def noreconfig(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_sd_noconfig.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 15000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 150000

    runtime_reconfig = 0
    rates = [15000000]
    schedule_modes = [5] * 3

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, runtime_reconfig={runtime_reconfig}"
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
    RESULT_FILE = "tasks/stream/logs_hs/cpu_sd.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 5000
    concurrency = 5
    batchsize = 1

    max_inflight_reqs = 50000

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
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
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
def test3(ctx, scale=2):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/test_sd_3.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 15000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 150000

    runtime_reconfig = 1
    rates = [20000000]
    schedule_modes = [7, 5, 3] * 4

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 50
    )

    for schedule_mode in schedule_modes:
        for rate in rates:
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            start_message = f"{timestamp} Running with rate={rate}, batchsize={batchsize}, concurrency={concurrency}, inputbatch={INPUT_BATCHSIZE}, scale={scale}, duration={DURATION}, schedulemode={schedule_mode}, runtime_reconfig={runtime_reconfig}"
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


def configure_stream_params(settings):
    """Resets stream parameters based on a dictionary."""
    defaults = {
        "dispatch_period": 20,
        "batch_check_period": 20,
        "planner_call_interval": 20,
        "parallel_dispatch": 0,
    }
    # Merge defaults with specific settings
    final_settings = {**defaults, **settings}
    for key, value in final_settings.items():
        reset_stream_parameter(key, value)


def get_pregenerated_work(batch_size):
    """Loads and prepares the input data."""
    records = read_data_from_txt_file_noparse(INPUT_FILE)
    return (
        generate_all_input_batch(records, batch_size, INPUT_MAP, INPUT_MSG)
        * 30
    )


def execute_benchmark(
    ctx,
    result_file,
    duration,
    rates,
    inputbatch,
    batchsize,
    scale,  # Scale means the parallelism of the stateful operator.
    concurrency,  # Concurrency means the number of executors running in one worker.
    max_inflight,
    max_waiting_queue_size,
    hosts_range,
    schedule_modes=[0],
    reconfig_period=1000,
    # The following parameters are not used.
    reconfig=0,
    alpha=300,
):
    global RESULT_FILE, DURATION

    RESULT_FILE = result_file
    DURATION = duration

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    configure_stream_params(
        {
            "runtime_reconfig": reconfig,
            "runtime_reconfig_period": reconfig_period,
            "alpha": alpha,
        }
    )

    work = get_pregenerated_work(inputbatch)

    for mode in schedule_modes:
        for rate, num_hosts in (
            [(rates[0], h) for h in hosts_range]
            if hosts_range
            else [(r, 0) for r in rates]
        ):
            timestamp = datetime.now().strftime("%d--%b--%Y %H:%M:%S")
            msg = (
                f"{timestamp} Running with rate={rate}, batchsize={batchsize}, "
                f"concurrency={concurrency}, inputbatch={inputbatch}, "
                f"scale={scale}, duration={DURATION}, schedulemode={mode}, "
                f"hosts={num_hosts}, max_inflight={max_inflight}"
            )
            write_string_to_log(RESULT_FILE, msg)

            run(
                pregenerated_work=work,
                input_rate=rate,
                inputbatch=inputbatch,
                batchsize=batchsize,
                scale=scale,
                concurrency=concurrency,
                max_inflight_reqs=max_inflight,
                max_waiting_queue_size=max_waiting_queue_size,
                num_hosts_scheduled_in=num_hosts,
                schedule_mode=mode,
            )
            print(f"Completed test with con: {concurrency}")


@task
def host(ctx, scale=5):
    execute_benchmark(
        ctx,
        result_file="tasks/stream/logs_elst/host_sd_fix_rate_1.txt",
        duration=50,
        scale=scale,
        schedule_modes=[0],
        inputbatch=500,
        rates=[3000],
        max_inflight=3000,
        max_waiting_queue_size=3000,
        batchsize=1,
        hosts_range=[1, 3, 5, 7, 10],
        reconfig_period=1000,
    )
