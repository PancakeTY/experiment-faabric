from faasmctl.util.config import get_faasm_ini_file as faasmctl_get_faasm_ini_file
from faasmctl.util.config import get_faasm_ini_value as faasmctl_get_faasm_ini_value
from faasmctl.util.flush import flush_workers, flush_scheduler

import subprocess
from subprocess import run
from time import sleep


def wait_for_pods(namespace, label, num_expected=1, quiet=False):
    # Wait for the faasm pods to be ready
    while True:
        if not quiet:
            print("Waiting for {} pods...".format(namespace))
        cmd = [
            "kubectl",
            "-n {}".format(namespace),
            "get pods -l {}".format(label),
            "-o jsonpath='{..status.conditions[?(@.type==\"Ready\")].status}'",
        ]
        cmd = " ".join(cmd)

        output = (
            run(cmd, shell=True, check=True, capture_output=True)
            .stdout.decode("utf-8")
            .rstrip()
        )
        statuses = [o.strip() for o in output.split(" ") if o.strip()]
        statuses = [s == "True" for s in statuses]
        true_statuses = [s for s in statuses if s]
        if len(true_statuses) == num_expected and all(statuses):
            if not quiet:
                print("All {} pods ready, continuing...".format(namespace))
            break

        if not quiet:
            print(
                "{} pods not ready, waiting ({}/{})".format(
                    namespace, len(true_statuses), num_expected
                )
            )
        sleep(5)

def flush_redis():
    ini_file = faasmctl_get_faasm_ini_file()
    backend = faasmctl_get_faasm_ini_value(ini_file, "Faasm", "backend")

    if backend == "k8s":
        try:
            result = subprocess.run(
                ["kubectl", "exec", "-n", "faasm", "redis-state", "--", "redis-cli", "FLUSHALL"],
                check=True,
                capture_output=True,
                text=True
            )
            print("Output:", result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error:", e.stderr)
    
    else:
        cluster_name = faasmctl_get_faasm_ini_value(ini_file, "Faasm", "cluster_name")
        redis_name = cluster_name + '-redis-state-1'
        command = ['docker', 'exec', redis_name, 'redis-cli', 'FLUSHALL']
        try:
            # Run the command and capture the output
            result = subprocess.run(
                command,
                check=True,             # Raise CalledProcessError on non-zero exit
                stdout=subprocess.PIPE, # Capture standard output
                stderr=subprocess.PIPE, # Capture standard error
                text=True               # Decode bytes to string
            )
            # Print the output from the command
            print('Redis FLUSH STDOUT:', result.stdout)
            print('Redis FLUSH STDERR:', result.stderr)
        except subprocess.CalledProcessError as e:
            # Handle errors in execution
            print('An error occurred:', e.stderr)

def flush_all():
    flush_workers()
    flush_scheduler()
    flush_redis()