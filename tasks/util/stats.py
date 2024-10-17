import re

# Input log text
def extract_data(file_path):
    log_text = ""
    with open(file_path, "r") as file:
        log_text = file.read()

    # Regular expressions to match the required data
    run_pattern = re.compile(r"Running with rate=(?P<input_rate>\d+), batchsize=(?P<batch_size>\d+), concurrency=(?P<concurrency>\d+), .* scale=(?P<scale>\d+), duration=\d+")
    total_messages_pattern = re.compile(r"Total messages processed: (?P<total_messages>\d+),")
    median_time_pattern = re.compile(r"Median actual time: (?P<median_time>[\d\.]+) ms,")
    percentile_95_time_pattern = re.compile(r"95th percentile actual time: (?P<percentile_95_time>[\d\.]+) ms,")
    percentile_time_pattern = re.compile(r"99th percentile actual time: (?P<percentile_time>[\d\.]+) ms,")
    actual_duration_pattern = re.compile(r"Actual Duration: (?P<actual_duration>[\d\.]+) ms")
    average_duration_pattern = re.compile(r"Average duration: (?P<average_duration>[\d\.]+) μs")

    # Lists to store the extracted data
    data = []

    # Split the log text into sections for each run
    runs = re.split(r"\n(?=\d{2}--\w{3}--\d{4} \d{2}:\d{2}:\d{2} Running with rate=)", log_text)

    for run in runs:
        print("-"*120)
        print(run)
        print("-"*120)
        if not run.strip():
            continue  # Skip empty strings

        run_data = {}

        # Extract input rate, batch size, concurrency, and scale
        run_info = run_pattern.search(run)
        if run_info:
            run_data['Input Rate'] = int(run_info.group('input_rate'))
            run_data['Batch Size'] = int(run_info.group('batch_size'))
            run_data['Concurrency'] = int(run_info.group('concurrency'))
            run_data['Scale'] = int(run_info.group('scale'))
        else:
            continue  # Skip if essential information is missing

        # Extract total messages processed
        total_messages = total_messages_pattern.search(run)
        if total_messages:
            run_data['Total Messages Processed'] = int(total_messages.group('total_messages'))

        # Extract median actual time
        median_time = median_time_pattern.search(run)
        if median_time:
            run_data['Median Actual Time (ms)'] = float(median_time.group('median_time'))

        percentile_95_time = percentile_95_time_pattern.search(run)
        if percentile_95_time:
            run_data['95th Percentile Actual Time (ms)'] = float(percentile_95_time.group('percentile_95_time'))

        # Extract 99th percentile actual time
        percentile_time = percentile_time_pattern.search(run)
        if percentile_time:
            run_data['99th Percentile Actual Time (ms)'] = float(percentile_time.group('percentile_time'))

        # Extract actual duration
        actual_duration = actual_duration_pattern.search(run)
        if actual_duration:
            run_data['Actual Duration (ms)'] = int(actual_duration.group('actual_duration'))

        # Extract average duration (calculate average if multiple values are present)
        average_durations = average_duration_pattern.findall(run)
        if average_durations:
            run_data['Average Duration (µs)'] = sum(map(float, average_durations)) / len(average_durations)

        # Calculate throughput
        run_data['Throughput (msg/sec)'] = (run_data['Total Messages Processed'] / run_data['Actual Duration (ms)']) * 1000

        data.append(run_data)

    return data


def print_data(data):
    # Print the extracted data
    print("Extracted Data:")
    print("-" * 120)
    print("{:<12} {:<11} {:<11} {:<6} {:<25} {:<22} {:<32} {:<20} {:<20}".format(
        "Input Rate", "Batch Size", "Concurrency", "Scale", "Total Messages Processed",
        "Median Actual Time (ms)", "99th Percentile Actual Time (ms)", "Actual Duration (ms)", "Average Duration (µs)"
    ))
    print("-" * 120)
    for entry in data:
        print("{:<12} {:<11} {:<11} {:<6} {:<25} {:<22} {:<32} {:<20} {:<20} {:.2f}".format(
            entry.get('Input Rate', 'N/A'),
            entry.get('Batch Size', 'N/A'),
            entry.get('Concurrency', 'N/A'),
            entry.get('Scale', 'N/A'),
            entry.get('Total Messages Processed', 'N/A'),
            entry.get('Median Actual Time (ms)', 'N/A'),
            entry.get('99th Percentile Actual Time (ms)', 'N/A'),
            entry.get('Actual Duration (ms)', 'N/A'),
            entry.get('Average Duration (µs)', 'N/A'),
            entry.get('Throughput (msg/sec)', 0),
        ))
        