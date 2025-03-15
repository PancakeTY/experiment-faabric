import re

# Input log text
import re

def extract_data(file_path, function_include=None):
    log_text = ""
    with open(file_path, "r") as file:
        log_text = file.read()

    # Regular expressions to match the required data
    run_pattern = re.compile(
        r"Running with rate=(?P<input_rate>\d+), batchsize=(?P<batch_size>\d+), concurrency=(?P<concurrency>\d+), .* scale=(?P<scale>\d+), duration=\d+"
    )
    total_messages_pattern = re.compile(r"Total messages processed: (?P<total_messages>\d+),")
    median_time_pattern = re.compile(r"Median actual time: (?P<median_time>[\d\.]+) ms,")
    percentile_95_time_pattern = re.compile(r"95th percentile actual time: (?P<percentile_95_time>[\d\.]+) ms,")
    percentile_time_pattern = re.compile(r"99th percentile actual time: (?P<percentile_time>[\d\.]+) ms,")
    actual_duration_pattern = re.compile(r"Actual Duration: (?P<actual_duration>[\d\.]+) ms")

    # Patterns for function metrics
    function_pattern = re.compile(
        r"Metrics for (?P<function_name>[\w_]+):\n((?:  .+\n)+)"
    )
    total_count_pattern = re.compile(r"  Total count count: (?P<total_count>[\d\.]+)")
    average_duration_pattern = re.compile(r"  Average duration: (?P<average_duration>[\d\.]+) μs")
    average_tuple_duration_pattern = re.compile(r"  Average avg_tuple_duration: (?P<avg_tuple_duration>[\d\.]+) μs")

    # Lists to store the extracted data
    data = []

    # Split the log text into sections for each run
    runs = re.split(
        r"\n(?=\d{2}--\w{3}--\d{4} \d{2}:\d{2}:\d{2} Running with rate=)", log_text
    )

    for run in runs:
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

        # Extract metrics for each function
        functions_data = {}
        for function_match in function_pattern.finditer(run):
            function_name = function_match.group('function_name')
            function_metrics = function_match.group(0)  # The whole match including the data

            # Extract total count
            total_count_match = total_count_pattern.search(function_metrics)
            total_count = int(total_count_match.group('total_count')) if total_count_match else None

            # Extract average duration
            average_duration_match = average_duration_pattern.search(function_metrics)
            average_duration = float(average_duration_match.group('average_duration')) if average_duration_match else None

            # Extract average tuple duration
            avg_tuple_duration_match = average_tuple_duration_pattern.search(function_metrics)
            avg_tuple_duration = float(avg_tuple_duration_match.group('avg_tuple_duration')) if avg_tuple_duration_match else None

            # Store the data
            functions_data[function_name] = {
                'Total Count': total_count,
                'Average Duration (µs)': average_duration,
                'Average Tuple Duration (µs)': avg_tuple_duration
            }

        # Store functions_data in run_data
        run_data['Functions'] = functions_data

        if function_include is not None:
            run_data['Total Messages Processed'] = 0
            for function_name, metrics in run_data['Functions'].items():
                if function_include + '_' in function_name:
                    run_data['Total Messages Processed'] = run_data['Total Messages Processed'] + metrics['Total Count']

        # Calculate throughput
        run_data['Throughput (msg/sec)'] = (
            run_data['Total Messages Processed'] / run_data['Actual Duration (ms)']
        ) * 1000

        data.append(run_data)

    return data


def extract_avg_tuple_duration(func_data, function_name):
    if isinstance(func_data, dict):
        return func_data.get(function_name, {}).get("Average Tuple Duration (µs)")
    else:
        try:
            func_dict = ast.literal_eval(func_data)
            return func_dict.get(function_name, {}).get("Average Tuple Duration (µs)")
        except Exception:
            return None

def extract_avg_tuple_duration_prefix(func_data, function_name_prefix):
    durations = []

    if isinstance(func_data, dict):
        # Iterate over all keys in the dictionary
        for key, value in func_data.items():
            if key.startswith(function_name_prefix) and "Average Tuple Duration (µs)" in value:
                durations.append(value["Average Tuple Duration (µs)"])
    else:
        try:
            # Parse string representation of the dictionary
            func_dict = ast.literal_eval(func_data)
            for key, value in func_dict.items():
                if key.startswith(function_name_prefix) and "Average Tuple Duration (µs)" in value:
                    durations.append(value["Average Tuple Duration (µs)"])
        except Exception:
            return None

    # Return the average duration if any values were found, else None
    if durations:
        return sum(durations) / len(durations)
    return None

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
        