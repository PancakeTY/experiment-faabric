from faasmctl.util.config import faasmctl_get_faasm_ini_file, faasmctl_get_faasm_ini_value

import json
from typing import Dict, Any, List

def copy_outout(destination_path = 'tasks/stream/tmp/faasm_result.txt'):
    ini_file = faasmctl_get_faasm_ini_file()
    backend = faasmctl_get_faasm_ini_value(ini_file, "Faasm", "backend")
    source_path = "/tmp/faasm_result.txt"

    if backend == "compose":
        cluster_name = faasmctl_get_faasm_ini_value(ini_file, "Faasm", "cluster_name")
        planner_name = cluster_name+"-planner-1"
        command = ["docker", "cp", f"{planner_name}:{source_path}", destination_path]
        print(f"running command: {command}")
        # Run the command
        try:
            subprocess.run(command, check=True)
            print("File copied successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")
    elif backend == "k8s":
        command = ["kubectl", "cp", f"planner:{source_path}", destination_path, "-n", "faasm"]
        print(f"running command: {command}")
        try:
            subprocess.run(command, check=True)
            print("File copied successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")        
    else:
        raise RuntimeError("Unsupported backend: {}".format(backend)) 


def load_app_results(file_path: str = 'tasks/stream/tmp/faasm_result.txt') -> Dict[int, List[Dict[str, Any]]]:
    try:
        with open(file_path, 'r') as file:
            app_results = json.load(file)
        # Convert the outer string keys to integers and keep the inner lists as is
        all_msg_results = [v for v in app_results.values()]
        print(f"Successfully loaded results from {file_path}")
        return all_msg_results
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except json.JSONDecodeError:
        print(f"Error: The file {file_path} does not contain valid JSON.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    return {}

def read_data_from_txt_file(file_path):
    data_vectors = []
    with open(file_path, 'r') as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors