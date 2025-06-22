import random
import time
import json

OUTPUT_PATH = "/pvol/runtime/experiment-faabric/tasks/stream/data/aa_dataset.txt"

def generate_campaign_ad_mapping(num_campaigns, ads_per_campaign):
    campaign_ids = list(range(num_campaigns))
    ad_ids = list(range(num_campaigns * ads_per_campaign))
    
    # build mapping: campaign_id -> list of ads_per_campaign ad IDs
    return {
        cid: ad_ids[cid * ads_per_campaign : (cid + 1) * ads_per_campaign]
        for cid in campaign_ids
    }

def get_persistent_state(num_campaigns=100, ads_per_campaign=10):
    # use a consistent name here
    campaign_ad_map = generate_campaign_ad_mapping(num_campaigns, ads_per_campaign)

    # invert to ad_id -> campaign_id
    ad_campaign_map = {
        ad_id: campaign_id
        for campaign_id, ads in campaign_ad_map.items()
        for ad_id in ads
    }
    
    return { str(ad_id): str(campaign_id)
             for ad_id, campaign_id in ad_campaign_map.items() }
    
# -----------------------------------------------------------------------------
# generate_dataset
#
# Generates a synthetic event log file where each record consists of:
#   • ad           — an integer ad ID in [0, num_campaigns*ads_per_campaign - 1]
#   • campaign     — the campaign ID matching that ad (via generate_campaign_ad_mapping)
#   • event_time   — a UNIX timestamp in milliseconds, randomly placed within
#                    consecutive windows of length `interval_ms`
#
# Parameters:
#   num_campaigns         Total number of campaigns (default 100)
#   ads_per_campaign      Number of ads assigned to each campaign (default 10)
#   num_intervals         How many consecutive time windows to generate (default 10)
#   interval_ms           Duration of each time window in ms (default 10_000 ms = 10 s)
#   records_per_interval  Number of event records to emit per window (default 10 000)
#
# The output file at OUTPUT_PATH will start with a header line:
#   ad campaign event_time
# and then `num_intervals * records_per_interval` lines of data.
# !!!MAXIMUM NUMBER OF num_intervals is 214748 when interval_ms is 10000!!!
# -----------------------------------------------------------------------------

MAX_INT32 = 2**31 - 1  # 2147483647

def generate_dataset(num_campaigns=100,
                     ads_per_campaign=10,
                     num_intervals=100,
                     interval_ms=10000,
                     records_per_interval=10000):
    """
    Generate a dataset with columns: ad, campaign, event_time.
    - ad: random integer [0, num_campaigns*ads_per_campaign-1]
    - campaign: looked up so it matches the ad
    - event_time: UNIX timestamp in ms, randomized within each 10s window
    """
    # build ad→campaign map once
    ad_campaign_map = get_persistent_state(num_campaigns, ads_per_campaign)
    start_time_ms = 0

    max_intervals = (MAX_INT32 + 1) // interval_ms
    if num_intervals > max_intervals:
        print(f"Clamping num_intervals from {num_intervals} to {max_intervals}")
        num_intervals = max_intervals

    with open(OUTPUT_PATH, 'w') as f:
        for window_idx in range(num_intervals):
            window_start = start_time_ms + window_idx * interval_ms

            # Generate this window’s records (only records_per_interval of them)
            bucket = [
                (
                    window_start + random.randint(0, interval_ms - 1),
                    random.randint(0, num_campaigns * ads_per_campaign - 1)
                )
                for _ in range(records_per_interval)
            ]

            # Sort by timestamp so they stream in order
            bucket.sort(key=lambda rec: rec[0])

            # Write sorted events directly to file
            for event_time, ad in bucket:
                campaign = int(ad_campaign_map[str(ad)])
                # 90% "view", 10% "other"
                event_type = "view" if random.random() < 0.9 else "other"
                f.write(f"{ad} {campaign} {event_time} {event_type}\n")

# Quick sanity check:
if __name__ == "__main__":
    num_campaigns = 100
    ads_per_campaign = 10
    print("Generating Adevertisement Analytic Dataset")
    # state = get_persistent_state(num_campaigns, ads_per_campaign)
    generate_dataset()
