import random
import os

# --- Configuration ---
NUM_RECORDS = 500000
URL_RANGE = (1, 20)
USER_RANGE = (1, 1000)
LOADTIME_RANGE = (1, 100)

OUTPUT_PATH = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/pl_dataset.txt"
)
PERSISTENT_OUTPUT_PATH = (
    "/pvol/runtime/experiment-faabric/tasks/stream/data/pl_persistent_data.txt"
)


def generate_dataset():
    """Generates the main dataset with biased URL selection."""

    # Ensure the directory exists before trying to write the file
    output_dir = os.path.dirname(OUTPUT_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    print(f"Generating {NUM_RECORDS} records for the main dataset...")

    # Build weighted list of possible URLs
    weighted_urls = []
    for i in range(URL_RANGE[0], URL_RANGE[1] + 1):
        weight = 20 if i % 4 == 0 else 1
        weighted_urls.extend([i] * weight)

    with open(OUTPUT_PATH, "w") as f:
        for _ in range(NUM_RECORDS):
            url_path = random.choice(weighted_urls)
            url = f"www.test.com/{url_path}"

            user_id = random.randint(USER_RANGE[0], USER_RANGE[1])
            loadtime = random.randint(LOADTIME_RANGE[0], LOADTIME_RANGE[1])

            f.write(f"{url} {user_id} {loadtime}\n")

    print(f"✅ Success! Main dataset has been written to: {OUTPUT_PATH}")


def generate_persistent_state():
    """Generates a mapping file for each URL path to a category."""

    # Ensure the directory exists
    output_dir = os.path.dirname(PERSISTENT_OUTPUT_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    print("Generating persistent state data (URL to category mapping)...")

    with open(PERSISTENT_OUTPUT_PATH, "w") as f:
        # Iterate through all possible URL paths from the specified range
        for i in range(URL_RANGE[0], URL_RANGE[1] + 1):
            key = f"pg_join_www.test.com/{i}"
            category = f"category{i}"

            # Write the mapping to the file
            f.write(f"{key} {category}\n")

    print(
        f"✅ Success! Persistent state has been written to: {PERSISTENT_OUTPUT_PATH}"
    )


if __name__ == "__main__":
    generate_dataset()
    print("-" * 20)  # Adding a separator for clarity
    generate_persistent_state()
