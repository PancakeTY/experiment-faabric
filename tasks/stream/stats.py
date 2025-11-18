from invoke import task
import pandas as pd

from tasks.util.stats import parse_log


@task
def transtime(ctx, app):
    """
    Parses the transaction time log for a given app, then calculates and
    displays the average median and P95 latency for each schedule mode.
    """
    try:
        # Construct the file path based on the app name
        result_file = f"tasks/stream/logs_hs/trans_{app}.txt"
        print(f"INFO: Parsing log file: {result_file}")

        # Call your existing function to parse the log into a DataFrame
        df = parse_log(result_file)
        print(df)

        # --- Analysis Code ---
        df["medianTotalLatency"] = pd.to_numeric(
            df["medianTotalLatency"], errors="coerce"
        )
        df["p95TotalLatency"] = pd.to_numeric(
            df["p95TotalLatency"], errors="coerce"
        )

        average_latencies = df.groupby("schedulemode")[
            ["medianTotalLatency", "p95TotalLatency"]
        ].mean()

        # To make the output cleaner, you can round the results
        average_latencies = average_latencies.round(2)

        print("\n--- Average Latency per Schedule Mode ---")

        # Define the desired order for schedule modes from your example
        output_order = [5, 3, 7]

        # Filter and reorder the DataFrame to match the desired output order
        # This also handles cases where a mode might not be in the data
        modes_present = [
            mode for mode in output_order if mode in average_latencies.index
        ]
        reordered_df = average_latencies.loc[modes_present]

        if not reordered_df.empty:
            # Get the schedule modes (index) as a comma-separated string
            modes_str = ",".join(map(str, reordered_df.index))

            # Loop through each column to print in the desired format
            for col_name in reordered_df.columns:
                # Get the values as a comma-separated string
                values_str = ",".join(
                    map(str, reordered_df[col_name].tolist())
                )
                print(f"{col_name} {modes_str} {values_str}")
        else:
            print(
                f"No data available for the specified schedule modes: {output_order}"
            )

        print("-----------------------------------------")

    except FileNotFoundError:
        print(f"ERROR: Log file not found at: {result_file}")
    except KeyError as e:
        print(f"ERROR: A required column is missing from the log file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


@task
def overall(ctx, app):
    """
    Parses the transaction time log for a given app, then calculates and
    displays the average median and P95 latency for each schedule mode.
    """
    try:
        # Construct the file path based on the app name
        result_file = f"tasks/stream/logs_hs/test_{app}.txt"
        print(f"INFO: Parsing log file: {result_file}")

        # Call your existing function to parse the log into a DataFrame
        df = parse_log(result_file)
        print(df)

        # --- Analysis Code ---
        df["medianTotalLatency"] = pd.to_numeric(
            df["medianTotalLatency"], errors="coerce"
        )
        df["p95TotalLatency"] = pd.to_numeric(
            df["p95TotalLatency"], errors="coerce"
        )

        average_latencies = df.groupby("schedulemode")[
            ["medianTotalLatency", "p95TotalLatency", "throughput"]
        ].mean()

        # To make the output cleaner, you can round the results
        average_latencies = average_latencies.round(2)

        print("\n--- Average Latency per Schedule Mode ---")

        # Define the desired order for schedule modes from your example
        output_order = [5, 3, 7]

        # Filter and reorder the DataFrame to match the desired output order
        # This also handles cases where a mode might not be in the data
        modes_present = [
            mode for mode in output_order if mode in average_latencies.index
        ]
        reordered_df = average_latencies.loc[modes_present]

        if not reordered_df.empty:
            # Get the schedule modes (index) as a comma-separated string
            modes_str = ",".join(map(str, reordered_df.index))

            # Loop through each column to print in the desired format
            for col_name in reordered_df.columns:
                # Get the values as a comma-separated string
                values_str = ",".join(
                    map(str, reordered_df[col_name].tolist())
                )
                print(f"{col_name} {modes_str} {values_str}")
        else:
            print(
                f"No data available for the specified schedule modes: {output_order}"
            )

        print("-----------------------------------------")

    except FileNotFoundError:
        print(f"ERROR: Log file not found at: {result_file}")
    except KeyError as e:
        print(f"ERROR: A required column is missing from the log file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


@task
def cpu(ctx, app):
    """
    Parses the log for a given app, then calculates and displays the
    average cpuExecutePct and cpuSchedulePct across all runs and all hosts.
    """
    try:
        result_file = f"tasks/stream/logs_hs/cpu_{app}.txt"
        print(f"INFO: Parsing log file: {result_file}")
        df = parse_log(result_file)

        # If the DataFrame is empty, stop here.
        if df.empty:
            print(
                "WARNING: Log parsing resulted in an empty DataFrame. Cannot calculate CPU stats."
            )
            return

        # --- NEW LOGIC STARTS HERE ---

        # These lists will store ALL CPU percentages from every run and every host
        all_execute_pcts = []
        all_schedule_pcts = []

        # Iterate over each row of the DataFrame (each row is one experimental run)
        for index, row in df.iterrows():
            schedule_mode = row["schedulemode"]
            if schedule_mode != 5:
                continue
            worker_stats_dict = row["workerStats"]

            # Check if worker_stats_dict is a valid dictionary
            if isinstance(worker_stats_dict, dict):
                # Iterate over each host's data (e.g., "172.18.0.11", "172.18.0.8", ...)
                for host_ip, measurements in worker_stats_dict.items():
                    # Each host has a list of measurement dictionaries
                    for measurement in measurements:
                        if "cpuExecutePct" in measurement:
                            all_execute_pcts.append(
                                measurement["cpuExecutePct"]
                            )
                        if "cpuSchedulePct" in measurement:
                            all_schedule_pcts.append(
                                measurement["cpuSchedulePct"]
                            )

        # --- CALCULATION AND DISPLAY ---

        print("\n--- Average CPU Utilization Across All Runs and Hosts ---")
        if all_execute_pcts:
            avg_execute = sum(all_execute_pcts) / len(all_execute_pcts)
            print(f"Average Execution CPU %: {avg_execute:.2f}%")
        else:
            print("No 'cpuExecutePct' data found.")

        if all_schedule_pcts:
            avg_schedule = sum(all_schedule_pcts) / len(all_schedule_pcts)
            print(f"Average Scheduler CPU %: {avg_schedule:.2f}%")
        else:
            print("No 'cpuSchedulePct' data found.")

        # --- END OF NEW LOGIC ---

    except Exception as e:
        print(f"An unexpected error occurred in the cpu task: {e}")
