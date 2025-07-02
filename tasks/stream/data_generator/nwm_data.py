import json
import random
from datetime import datetime, timedelta

# --- Configuration ---
NUM_HOSTS = 1000
LOGINS_PER_HOST_PER_DAY = 5
FAIL_RATE = 0.90
START_DATE = "20200101"
TOTAL_RECORDS = 1000000
OUTPUT_PATH = "/pvol/runtime/experiment-faabric/tasks/stream/data/nwm_dataset.txt"

def generate_dataset_to_file():
    """
    Generates a dataset of login events and writes them to a file.
    Each day’s batch of events is fully randomized in order.
    """
    # 1. generate hosts
    hosts = [f"host-{i:04d}" for i in range(NUM_HOSTS)]
    
    # 2. compute days needed
    records_per_day = NUM_HOSTS * LOGINS_PER_HOST_PER_DAY
    num_days = (TOTAL_RECORDS + records_per_day - 1) // records_per_day

    print(f"Preparing to generate {TOTAL_RECORDS} records into '{OUTPUT_PATH}'…")
    print(f"Simulating {num_days} days of activity for {NUM_HOSTS} hosts.")
    print("---------------------\n")

    current_date = datetime.strptime(START_DATE, "%Y%m%d")
    records_generated = 0

    try:
        with open(OUTPUT_PATH, "w") as f:
            for _ in range(num_days):
                if records_generated >= TOTAL_RECORDS:
                    break

                event_time_str = current_date.strftime("%Y%m%d")
                daily_records = []

                # build today's records
                for host in hosts:
                    for _ in range(LOGINS_PER_HOST_PER_DAY):
                        if records_generated >= TOTAL_RECORDS:
                            break
                        status = "fail" if random.random() < FAIL_RATE else "success"
                        daily_records.append({
                            "host":       host,
                            "status":     status,
                            "method":     "pwd",
                            "region":     "AS",
                            "event_time": event_time_str,
                        })
                        records_generated += 1
                    if records_generated >= TOTAL_RECORDS:
                        break

                # shuffle entire day
                random.shuffle(daily_records)

                # write shuffled records
                for rec in daily_records:
                    f.write(json.dumps(rec) + "\n")

                current_date += timedelta(days=1)

    except IOError as e:
        print(f"Error writing to file: {e}")
        return

    print(f"\n--- Generation Complete ---")
    print(f"Wrote {records_generated} records to '{OUTPUT_PATH}'")

if __name__ == "__main__":
    generate_dataset_to_file()
