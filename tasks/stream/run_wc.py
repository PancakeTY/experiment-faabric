from datetime import datetime
from invoke import task
import re
from datetime import datetime, timedelta
import statistics

from faasmctl.util.planner import reset_stream_parameter
from tasks.util.faasm import generate_all_input_batch, write_string_to_log
from tasks.util.stats import parse_log, average_metrics
from tasks.util.plot import plot_stats
from tasks.util.planner import run_application_with_input

# Static
CUTTING_LINE = "-------------------------------------------------------------------------------"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
# Mutable
DURATION = 600
INPUT_BATCHSIZE = 500
NUM_INPUT_THREADS = 10
APPLICATION_NAME = "wc_application"
INPUT_FILE = "tasks/stream/data/wc_dataset.txt"
INPUT_MSG = {
    "user": "stream",
    "function": "wc_split",
}
RESULT_FILE = "tasks/stream/logs/exp_wc.txt"
INPUT_MAP = {"sentence": 0}


def read_sentences_from_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

    # Replace newlines so sentences spanning lines aren’t broken prematurely
    text = text.replace("\n", " ")

    # 1. Split into raw sentences by period
    raw_sentences = text.split(".")

    sentences = []
    for raw in raw_sentences:
        # 2. Clean and normalize
        cleaned = re.sub(r"[^a-zA-Z\s]", "", raw).lower().strip()
        if not cleaned:
            continue

        words = cleaned.split()
        # 3. SOME ARCTILE DOESN'T HAS FULL STOP !!!!!
        # If over 10 words, break into 10-word chunks.
        for i in range(0, len(words), 10):
            chunk = " ".join(words[i : i + 10])
            sentences.append(chunk)

    print(f"Total sentences created: {len(sentences)}")

    # --- NEW: wrap each string in a single-element list ---
    records = [[s] for s in sentences]
    return records


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
    global APPLICATION_NAME, INPUT_MSG, RESULT_FILE, NUM_INPUT_THREADS

    node1 = {
        "name": "stream_wc_split",
        "node_type": "STATELESS",
        "input": True,
        "inputFields": ["sentence"],
        "successorNode": ["stream_wc_count"],
    }
    node2 = {
        "name": "stream_wc_count",
        "node_type": "PARTITIONED_STATEFUL",
        "partitionBy": "word",
        "parallelism": scale,
        "inputFields": ["word"],
        "successorNode": [],
    }
    nodes = [node1, node2]

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
        schedule_mode=schedule_mode,
        max_inflight_reqs=max_inflight_reqs,
    )


@task
def test(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 200
    RESULT_FILE = "tasks/stream/logs_hs/test_wc_5.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 1
    rates = [30000000]
    schedule_modes = [7, 5, 3] * 1

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 15
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
    RESULT_FILE = "tasks/stream/logs_hs/test_wc.txt"
    df = parse_log(RESULT_FILE)

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

    print(df)
    average_metrics(df)

    plot_stats("wc", df)


@task
def test20(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs_hs/test_20_wc.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 5000
    concurrency = 5
    batchsize = 20

    rates = [50000]
    schedule_modes = [2, 0, 5, 6, 1, 3]

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
            )
            print(f"Completed test_contention with con: {concurrency}")


@task
def stats20(ctx):
    RESULT_FILE = "tasks/stream/logs_hs/test_20_wc.txt"
    df = parse_log(RESULT_FILE)

    average_metrics(df)

    plot_stats("20_wc", df)


@task
def analyze(ctx):
    records = read_sentences_from_file(INPUT_FILE)
    # pull out just the sentence text
    sentences = [rec[0].strip() for rec in records if rec[0].strip()]
    # compute word counts
    lengths = [len(s.split()) for s in sentences]

    # --- new: average length ---
    avg_length = statistics.mean(lengths) if lengths else 0.0

    # compute absolute gaps between adjacent lengths
    gaps = [abs(lengths[i] - lengths[i - 1]) for i in range(1, len(lengths))]

    # summary metrics
    median_gap = statistics.median(gaps) if gaps else 0
    mean_gap = statistics.mean(gaps) if gaps else 0.0

    # print all stats
    print(f"Average sentence length:          {avg_length:.2f} words")
    print(f"Median length-gap between adjacent sentences: {median_gap} words")
    print(
        f"Mean   length-gap between adjacent sentences: {mean_gap:.2f} words"
    )


@task
# inv stream.run-wc.trans-exp
def trans_exp(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE, INPUT_FILE, INPUT_MAP, INPUT_MSG
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/trans_wc.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1
    concurrency = 1
    batchsize = 1

    max_inflight_reqs = 1

    rates = [20]
    schedule_modes = [7, 5, 3] * 2

    reset_stream_parameter("dispatch_period", 0)
    reset_stream_parameter("batch_check_period", 0)
    reset_stream_parameter("runtime_reconfig", 0)
    reset_stream_parameter("planner_call_interval", 0)
    reset_stream_parameter("runtime_reconfig_period", 1000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("max_inflight_reqs", 1000)

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)
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
def cpu(ctx, scale=5):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/cpu_wc.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 1000
    concurrency = 5
    batchsize = 1

    max_inflight_reqs = 10000

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
    records = read_sentences_from_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 15
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
def reconfig(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_wc_config.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 1
    rates = [30000000]
    schedule_modes = [5] * 2

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 15
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
def noreconfig(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/reconfig_wc_noconfig.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 30000

    runtime_reconfig = 0
    rates = [30000000]
    schedule_modes = [5] * 2

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 20000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 50)

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 15
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
def test3(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 600
    RESULT_FILE = "tasks/stream/logs_hs/test_wc_3.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 3000
    concurrency = 5
    batchsize = 20

    max_inflight_reqs = 15000

    runtime_reconfig = 1
    rates = [30000000]
    schedule_modes = [7, 5, 3]

    reset_stream_parameter("dispatch_period", 20)
    reset_stream_parameter("batch_check_period", 20)
    reset_stream_parameter("planner_call_interval", 20)
    reset_stream_parameter("runtime_reconfig", runtime_reconfig)
    reset_stream_parameter("runtime_reconfig_period", 10000)
    reset_stream_parameter("parallel_dispatch", 1)
    reset_stream_parameter("alpha", 300)

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)
    pregenerated_work = (
        generate_all_input_batch(
            records, INPUT_BATCHSIZE, INPUT_MAP, INPUT_MSG
        )
        * 15
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
