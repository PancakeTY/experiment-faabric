import json
import random
import base64
import struct

# --- Configuration ---
# The path to the output file where the dataset will be saved.
OUTPUT_PATH = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/etl_dataset.txt"
)

# Define the number of data rows to generate.
NUM_ROWS = 1000000

# Define the ranges for the random data generation.
MSG_ID_RANGE = (100, 301)
SENSOR_ID_RANGE = (1, 6)
OBS_TYPE_RANGE = (1, 201)
OBS_VALUE_RANGE = (1, 101)
# ---------------------


def generate_dataset():
    """
    Generates a dataset based on specified parameters and writes it to a file.
    """
    print(f"Generating {NUM_ROWS} data rows and writing to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, "w") as f:
        for _ in range(NUM_ROWS):
            msg_id = random.randint(MSG_ID_RANGE[0], MSG_ID_RANGE[1])
            sensor_id = (
                f"s{random.randint(SENSOR_ID_RANGE[0], SENSOR_ID_RANGE[1])}"
            )
            obs_type = (
                f"t{random.randint(OBS_TYPE_RANGE[0], OBS_TYPE_RANGE[1])}"
            )
            obs_value = str(
                random.randint(OBS_VALUE_RANGE[0], OBS_VALUE_RANGE[1])
            )

            json_payload = {
                "sensor_id": sensor_id,
                "meta": "meta",
                "obs_type": obs_type,
                "obs_val": obs_value,
            }

            json_string = json.dumps(json_payload)
            f.write(f"{msg_id} {json_string}\n")

    print("Dataset generation complete.")


# --- Configuration for persistent state---
PERSISTENT_OUTPUT_PATH = "/pvol/runtime/experiment-faabric/tasks/stream/data/etl_persistent_data.txt"

# --- Bloom Filter Configuration ---
BLOOM_FILTER_VALUE_RANGE = (20, 80)
M_HASHES = 3
M_BITS = 50


def get_hashes(s: str, num_hashes: int, num_bits: int) -> list[int]:
    """
    Generates multiple hash values for a given string.
    """
    result = []
    h1 = hash(s)
    h2 = hash(s[::-1])
    for i in range(num_hashes):
        composite_hash = (h1 + i * h2) % num_bits
        result.append(composite_hash)
    return result


def generate_persistent_state():
    """
    Generates key-value pairs for filters and annotations.
    """
    print(
        f"Generating persistent state and writing to {PERSISTENT_OUTPUT_PATH}..."
    )

    with open(PERSISTENT_OUTPUT_PATH, "w") as f:
        # 1. Generate filters for each obs_type
        for i in range(OBS_TYPE_RANGE[0], OBS_TYPE_RANGE[1]):
            obs_type = f"t{i}"

            range_key = f"etl_filter_range_{obs_type}"
            range_value = (
                f"{BLOOM_FILTER_VALUE_RANGE[0]}, {BLOOM_FILTER_VALUE_RANGE[1]}"
            )
            f.write(f"{range_key} {range_value}\n")

            bloom_key = f"etl_filter_bloom_{obs_type}"
            bit_array = [False] * M_BITS

            start, end = BLOOM_FILTER_VALUE_RANGE
            all_values_in_range = list(range(start, end + 1))
            random.shuffle(all_values_in_range)
            num_to_add = len(all_values_in_range) // 2
            values_to_add = all_values_in_range[:num_to_add]

            for val in values_to_add:
                hashes = get_hashes(str(val), M_HASHES, M_BITS)
                for h in hashes:
                    bit_array[h] = True

            # --- MODIFICATION START ---
            # Convert the list of booleans to a string of '1's and '0's
            bit_array_str = "".join(["1" if b else "0" for b in bit_array])
            f.write(f"{bloom_key} {bit_array_str}\n")
            # --- MODIFICATION END ---

        # 2. Generate annotations for each obs_value
        for i in range(OBS_VALUE_RANGE[0], OBS_VALUE_RANGE[1]):
            annotate_key = f"etl_annotate_{i}"
            annotate_value = "annotation"
            f.write(f"{annotate_key} {annotate_value}\n")

        f.write("etl_annotate_unknown annotation\n")

    print("Persistent state generation complete.")


if __name__ == "__main__":
    generate_dataset()
    generate_persistent_state()
