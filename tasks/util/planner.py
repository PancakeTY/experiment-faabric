import time
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from time import sleep

from tasks.util.thread import AtomicInteger, batch_producer, batch_consumer
from tasks.util.k8s import flush_all
from tasks.util.faasm import write_string_to_log
from faasmctl.util.planner import (
    register_application,
    reset_stream_parameter,
    reset_batch_size,
    reset_max_replicas,
    output_result,
    get_available_hosts as planner_get_available_hosts,
    get_in_fligh_apps as planner_get_in_fligh_apps,
    set_persistent_state,
)

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
        node.setdefault('input', None)
        node.setdefault('node_type', 'STATELESS')
        node.setdefault('parallelism', 1)
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
    persistent_state = None,
):
    write_string_to_log(
        result_file,
        f"Input Rates:{input_rate}, Batchsize:{batchsize}, "
        f"Concurrency:{concurrency}, InputBatch:{inputbatch}, Scale:{scale}\n"
    )

    # Flush the scheduler and workers
    flush_all()

    format_nodes(nodes)

    register_application(application_name, nodes)

    if persistent_state is not None:
        set_persistent_state(persistent_state)

    # Reset the parameters
    reset_stream_parameter("is_outputting", 0)
    reset_stream_parameter("max_inflight_reqs", 15000)
    
    if batchsize > 0:
        reset_batch_size(batchsize)
    
    if concurrency > 0:
        reset_max_replicas(concurrency)

    # Initialize variables for running application
    atomic_count = AtomicInteger(1)
    appid_list = []
    appid_list_lock = threading.Lock()
    input_threads = []

    # Launch multiple threads
    batch_queue = Queue()
    
    # Setup the start and end time for the application running
    start_time = time.time()
    end_time = start_time + duration
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")

    # Start the ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # Submit the batch_producer function to the executor
        future = executor.submit(
            batch_producer,
            records,
            atomic_count,
            inputbatch,
            input_map,
            batch_queue,
            end_time,
            input_rate,
            num_input_threads
        )

        # Start consumer threads
        for _ in range(num_input_threads):
            thread = threading.Thread(
                target=batch_consumer,
                args=(
                    batch_queue,
                    appid_list,
                    appid_list_lock,
                    input_msg,
                    inputbatch,
                    end_time,
                )
            )
            input_threads.append(thread)
            thread.start()

        # Wait for the producer to finish and get the result
        total_items_produced = future.result()
        produce_messenger = f"Total items produced: {total_items_produced}"
        print(produce_messenger)
        write_string_to_log(result_file, produce_messenger)

        # Wait for consumer threads to finish
        for thread in input_threads:
            thread.join()

    print("All threads finished and waiting for the application to finish...")
    time.sleep(10)

    stats_result = output_result()
    print(stats_result)
    write_string_to_log(result_file, stats_result)