import numpy as np
import json

# Step 1: Read the file content
with open('/pvol/runtime/experiment-faabric/tasks/stream/results/2024-09-09/sd_exp_1', 'r') as file:
    file_content = file.read()

# Step 2: Parse the content to extract dictionaries
# We replace single quotes with double quotes to make it JSON-compatible
file_content = file_content.replace("'", '"')

# Split the content by '{}' to separate dictionaries
raw_dicts = [s for s in file_content.split('}') if s.strip()]

latencies = []

# Step 3: Process each dictionary
for raw_dict in raw_dicts:
    raw_dict = raw_dict.strip().lstrip(',') + '}'  # Add missing closing braces
    try:
        data = json.loads(raw_dict)
        latencies.extend(data.values())  # Extract and collect the latencies
    except json.JSONDecodeError:
        print("Error parsing dictionary:", raw_dict)

# Step 4: Calculate the median and 99th percentile latencies
print("Latencies count:", len(latencies))
if latencies:
    latencies = np.array(latencies)
    median_latency = np.median(latencies)
    percentile_99_latency = np.percentile(latencies, 99)

    print(f"Median Latency: {median_latency}")
    print(f"99th Percentile Latency: {percentile_99_latency}")
else:
    print("No valid latencies found.")
