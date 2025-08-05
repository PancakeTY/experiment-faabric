import time
import threading
import json
import math
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from time import sleep
from collections import defaultdict

from faasmctl.util.invoke import invoke_by_consumer
from tasks.util.thread import (
    AtomicInteger,
    token_producer,
    batch_producer,
    batch_consumer,
)
from tasks.util.k8s import flush_all
from tasks.util.faasm import write_string_to_log
from faasmctl.util.planner import (
    register_application,
    reset_stream_parameter,
    reset_batch_size,
    output_result,
    get_available_hosts as planner_get_available_hosts,
    get_in_fligh_apps as planner_get_in_fligh_apps,
    set_persistent_state,
    custom_request,
)
from tasks.util.faasm import generate_input_data
from faasmctl.util.batch import get_msg_from_input_data


# This method also returns the number of used VMs
def get_num_idle_cpus_from_in_flight_apps(
    num_vms, num_cpus_per_vm, in_flight_apps
):
    total_cpus = int(num_vms) * int(num_cpus_per_vm)

    worker_occupation = {}
    total_used_cpus = 0
    for app in in_flight_apps.apps:
        for ip in app.hostIps:
            if ip not in worker_occupation:
                worker_occupation[ip] = 0

            worker_occupation[ip] += 1
            total_used_cpus += 1

    num_idle_vms = int(num_vms) - len(worker_occupation.keys())
    num_idle_cpus = total_cpus - total_used_cpus

    return num_idle_vms, num_idle_cpus


def get_num_available_slots_from_in_flight_apps(
    num_vms,
    num_cpus_per_vm,
    user_id=None,
    num_evicted_vms=None,
    openmp=False,
    # Used to leave some slack CPUs to help de-fragment (for `mpi-locality`)
    next_task_size=None,
    # Used to make Granny behave like batch (for `mpi-locality`)
    batch=False,
):
    """
    For Granny baselines, we cannot use static knowledge of the
    allocated slots, as migrations may happen so we query the planner
    """
    short_sleep_secs = 0.25
    long_sleep_secs = 1

    while True:
        in_flight_apps = planner_get_in_fligh_apps()
        available_hosts = planner_get_available_hosts()
        available_ips = [host.ip for host in available_hosts.hosts]

        if len(available_ips) != num_vms:
            print(
                "Not enough hosts registered ({}/{}). Retrying...".format(
                    len(available_ips), num_vms
                )
            )
            sleep(short_sleep_secs)
            continue

        available_slots = sum(
            [
                int(host.slots - host.usedSlots)
                for host in available_hosts.hosts
            ]
        )

        used_slots_map = {
            host.ip: host.usedSlots for host in available_hosts.hosts
        }

        next_evicted_vm_ips = []
        try:
            next_evicted_vm_ips = in_flight_apps.nextEvictedVmIps
        except AttributeError:
            pass

        if (
            num_evicted_vms is not None
            and len(next_evicted_vm_ips) != num_evicted_vms
        ):
            print("Not enough evicted VMs registered. Retrying...")
            sleep(short_sleep_secs)
            continue

        worker_occupation = {}

        for next_evicted_vm_ip in next_evicted_vm_ips:
            worker_occupation[next_evicted_vm_ip] = int(num_cpus_per_vm)
            available_slots -= int(num_cpus_per_vm)

        # Annoyingly, we may query for the in-flight apps as soon as we
        # schedule them, missing the init stage of the mpi app. Thus we
        # sleep for a bit and ask again (we allow the size to go over the
        # specified size in case of an elsatic scale-up)
        if any([len(app.hostIps) < app.size for app in in_flight_apps.apps]):
            sleep(short_sleep_secs)
            continue

        # Also prevent from scheduling an app while another app is waiting
        # to be migrated from an evicted VM
        must_hold_back = False
        for app in in_flight_apps.apps:
            if any([ip in next_evicted_vm_ips for ip in app.hostIps]):
                print(
                    "Detected app {} scheduled in to-be evicted VM. Retrying...".format(
                        app.appId
                    )
                )
                must_hold_back = True
                break

        if must_hold_back:
            sleep(long_sleep_secs)
            continue

        for app in in_flight_apps.apps:
            # If the subtype is 0, protobuf will optimise it away and the field
            # won't be there. This try/except guards against that
            this_app_uid = 0
            try:
                this_app_uid = app.subType
            except AttributeError:
                pass

            must_prune_vm = user_id is not None and user_id != this_app_uid

            for ip in app.hostIps:

                # This pruning corresponds to a multi-tenant setting using
                # mpi-evict
                if must_prune_vm:
                    worker_occupation[ip] = int(num_cpus_per_vm)
                    continue

                if ip not in worker_occupation:
                    worker_occupation[ip] = 0

                if worker_occupation[ip] < int(num_cpus_per_vm):
                    worker_occupation[ip] += 1

        # For OpenMP, we only care if any VM has enough slots to run the full
        # application. Otherwise we wait.
        if openmp:
            if num_vms > len(list(worker_occupation.keys())):
                return num_cpus_per_vm

            return max(
                [
                    num_cpus_per_vm - worker_occupation[ip]
                    for ip in worker_occupation
                ]
            )

        # In a batch setting, we allocate resources to jobs at VM granularity
        # The planner will by default do so, if enough free VMs are available
        if batch and next_task_size is not None:
            num_needed_vms = ceil(next_task_size / num_cpus_per_vm)
            if (
                num_vms - len(list(worker_occupation.keys()))
            ) < num_needed_vms:
                sleep(5 * long_sleep_secs)
                continue

        num_available_slots = (
            num_vms - len(list(worker_occupation.keys()))
        ) * num_cpus_per_vm
        for ip in worker_occupation:
            if worker_occupation[ip] != used_slots_map[ip]:
                print(
                    "Inconsistent worker used slots map for ip: {}".format(ip)
                )
                must_hold_back = True
                break

            num_available_slots += num_cpus_per_vm - worker_occupation[ip]

        if must_hold_back:
            sleep(long_sleep_secs)
            continue

        # Double-check the number of available slots with our other source of truth
        # are consistent with the in-flight apps?
        if num_available_slots != available_slots:
            print(
                "WARNING: inconsistency in the number of available slots"
                " (in flight: {} - registered: {})".format(
                    num_available_slots, available_slots
                )
            )
            sleep(short_sleep_secs)
            continue

        # TODO: decide on the percentage, 10% or 5% ?
        # with 10% we almost always have perfect locality
        pctg = 0.05
        if (
            next_task_size is not None
            and not batch
            and (num_available_slots - next_task_size)
            < int(num_vms * num_cpus_per_vm * pctg)
        ):
            sleep(long_sleep_secs)
            continue

        # If we have made it this far, we are done
        break

    # If we have any frozen apps, we want to un-FREEZE them to prevent building
    # up a buffer in the planner
    if len(in_flight_apps.frozenApps) > 0:
        print(
            "Detected frozen apps, so returning 0 slots: {}".format(
                in_flight_apps.frozenApps
            )
        )
        return 0

    return num_available_slots


def get_xvm_links_from_part(part):
    """
    Calculate the number of cross-VM links for a given partition

    The number of cross-VM links is the sum for each process of all the
    non-local processes divided by two.
    """
    if len(part) == 1:
        return 0

    count = 0
    for ind in range(len(part)):
        count += sum(part[0:ind] + part[ind + 1 :]) * part[ind]

    return int(count / 2)


def get_num_xvm_links_from_in_flight_apps(in_flight_apps):
    total_xvm_links = 0

    for app in in_flight_apps.apps:
        app_ocupation = {}
        for ip in app.hostIps:
            if ip not in app_ocupation:
                app_ocupation[ip] = 0
            app_ocupation[ip] += 1

        part = list(app_ocupation.values())
        # TODO: delete me
        #    print("DEBUG - App: {} - Occupation: {} - Part: {} - Links: {}".format(
        #               app.appId,
        #               app_ocupation,
        #               part,
        #               get_xvm_links_from_part(part)))
        total_xvm_links += get_xvm_links_from_part(part)

    return total_xvm_links


def format_nodes(nodes):
    """
    Ensure each node dict has keys:
      - 'input'        (default: None)
      - 'node_type'    (default: "STATELESS")
      - 'parallelism'  (default: 1)
    Leaves all other keys untouched.
    """
    for node in nodes:
        node.setdefault("input", None)
        node.setdefault("node_type", "STATELESS")
        node.setdefault("parallelism", 1)
    return nodes


def run_application_with_input(
    application_name: str,
    nodes,
    records,
    result_file: str,
    input_map: dict,
    input_msg: str,
    num_input_threads: int,
    scale: int,
    batchsize: int,
    concurrency: int,
    inputbatch: int,
    input_rate: float,
    duration: float,
    schedule_mode: int,
    persistent_state=None,
    num_hosts_scheduled_in=0,
    reschedule=True,
):
    write_string_to_log(
        result_file,
        f"Input Rates:{input_rate}, Batchsize:{batchsize}, "
        f"Concurrency:{concurrency}, InputBatch:{inputbatch}, Scale:{scale}\n",
    )

    print("Pre-generating all JSON payloads...")
    pregenerated_work = []
    if len(records) < 10000:
        print("Not enough records to pre-generate payloads. Exiting.")
        return
    num_batches = math.floor(len(records) / inputbatch)

    for i in range(num_batches):
        start_idx = i * inputbatch

        input_data_list = generate_input_data(
            records, start_idx, inputbatch, input_map
        )
        chained_id_list = [
            j for j in range(start_idx + 1, start_idx + inputbatch + 1)
        ]
        app_id = start_idx + 1

        # 2. Create the final payload
        msg_json = get_msg_from_input_data(
            app_id,
            input_msg,
            inputbatch,
            input_data_list,
            chained_id_list,
        )

        # 3. Store the (app_id, json_payload) tuple
        pregenerated_work.append((app_id, msg_json))

    print(f"Pre-generation complete: {len(pregenerated_work)} payloads.")

    # Flush the scheduler and workers
    flush_all()

    reset_stream_parameter("schedule_mode", schedule_mode)
    format_nodes(nodes)
    # if num_hosts_scheduled_in > 0:
    #     reset_stream_parameter("num_hosts_scheduled", num_hosts_scheduled_in)
    register_application(application_name, nodes)

    if persistent_state is not None:
        set_persistent_state(persistent_state)

    # Reset the parameters
    reset_stream_parameter("is_outputting", 0)
    reset_stream_parameter("max_inflight_reqs", input_rate)
    reset_stream_parameter("max_executors", 80)
    reset_stream_parameter("max_replicas", concurrency)
    if batchsize > 0:
        reset_batch_size(batchsize)

    # If schedule mode is 0 or 3 (Our designed scheduler or FaaSFlow scheduler)
    if reschedule and (schedule_mode == 0 or schedule_mode == 3):
        print("Pre-invoking the application...")
        pre_num_batches = int(10000 / inputbatch)
        for i in range(pre_num_batches):
            msg_json = pregenerated_work[i][1]
            invoke_by_consumer(
                msg_json,
                num_retries=1000,
                sleep_period_secs=0.1,
                end_time=time.time() + 10,
            )
        print("Pre-invocation complete, rescheduling...")
        time.sleep(5)
        custom_request(key="reschedule", value="1")
        print("Rescheduling complete.")

    # Initialize variables for running application
    atomic_counter = AtomicInteger(1)
    input_threads = []

    # Launch multiple threads
    batch_queue = Queue()  # Bounded queue
    token_queue = Queue()

    # Setup the start and end time for the application running
    start_time = time.time()
    end_time = start_time + duration
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")

    num_producer_threads = 1

    # Start the ThreadPoolExecutor
    with ThreadPoolExecutor(
        max_workers=num_input_threads + num_producer_threads + 1
    ) as executor:

        executor.submit(
            token_producer,
            token_queue,
            input_rate,
            inputbatch,
            duration,
            num_producer_threads,
        )

        producer_future = executor.submit(
            batch_producer,
            pregenerated_work,
            token_queue,
            batch_queue,
            atomic_counter,
            num_input_threads,
            end_time,
        )

        # 4. Start consumer threads
        consumer_futures = []
        for _ in range(num_input_threads):
            future = executor.submit(
                batch_consumer,
                batch_queue,
                end_time,
            )
            consumer_futures.append(future)

        batches_produced = producer_future.result()
        # Wait for the producer to finish and get the result
        total_items_produced = batches_produced * inputbatch
        produce_messenger = f"Total items produced: {total_items_produced}"
        print(produce_messenger)
        write_string_to_log(result_file, produce_messenger)

        for future in consumer_futures:
            future.result()  # This will block until the consumer has exited

    print("All threads finished and waiting for the application to finish...")
    time.sleep(10)

    stats_result_str = output_result()

    # Process the output
    stats_dict = json.loads(stats_result_str)
    duration = (stats_dict["endTime"] - stats_dict["startTime"]) / 1_000_000
    stats_dict["execution_duration"] = duration

    # Use defaultdict to simplify the summation
    operator_statistics = defaultdict(int)

    # Iterate through each instance's data dictionary
    for instance_name, instance_data in stats_dict["instances"].items():
        # Extract the base operator name (e.g., "stream_etl_join_0" -> "stream_etl_join")
        operator_name = instance_name.rsplit("_", 1)[0]

        # Add the instance's 'count' to the operator's total
        operator_statistics[operator_name] += instance_data.get("count", 0)

    # Add the final sums back to the main dictionary
    stats_dict["operatorStatistics"] = dict(operator_statistics)
    stats_result = json.dumps(stats_dict, indent=4)

    print(stats_result)
    write_string_to_log(result_file, stats_result)
