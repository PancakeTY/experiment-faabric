import time
from datetime import datetime
from invoke import task
import re
from datetime import datetime, timedelta
import time

# Custom utility functions
from tasks.util.faasm import (
    write_string_to_log,
)
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
        # 3. If over 20 words, break into 20-word chunks
        for i in range(0, len(words), 20):
            chunk = " ".join(words[i : i + 20])
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
):
    """
    Test the 'an' function with resource contention.
    Input rate unit: data/ second
    """
    global APPLICATION_NAME, INPUT_FILE, INPUT_MSG, RESULT_FILE, INPUT_MAP, NUM_INPUT_THREADS

    # Prepare the input data
    records = read_sentences_from_file(INPUT_FILE)

    print(records[0])  # Print first 10 records for debugging

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
        schedule_mode=schedule_mode,
    )


@task
def test(ctx, scale=3):
    global DURATION, INPUT_BATCHSIZE
    global RESULT_FILE

    DURATION = 100
    RESULT_FILE = "tasks/stream/logs/wc_temp_test.txt"

    write_string_to_log(RESULT_FILE, CUTTING_LINE)
    INPUT_BATCHSIZE = 2500
    concurrency = 5
    batchsize = 20

    rates = [25000]
    # schedule_modes = [0, 1, 2, 3]
    schedule_modes = [4]

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
def analyze(ctx):
    records = read_sentences_from_file(INPUT_FILE)
    print(records[0])
    worker_count = 3
    stats = [0, 0, 0, 0, 0]
    for i in range(len(records)):
        len_record = records[i][0].split()
        if len_record:
            stats[i % worker_count] += len(len_record)
    print(f"Stats: {stats}")
