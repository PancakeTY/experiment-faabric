from faasmctl.util.config import (
    get_faasm_ini_file as faasmctl_get_faasm_ini_file,
)
from faasmctl.util.config import (
    get_faasm_ini_value as faasmctl_get_faasm_ini_value,
)

import subprocess
import json
from typing import Dict, Any, List
import os
import time


def copy_outout(destination_path="tasks/stream/tmp/faasm_result.txt"):
    ini_file = faasmctl_get_faasm_ini_file()
    backend = faasmctl_get_faasm_ini_value(ini_file, "Faasm", "backend")
    source_path = "/tmp/faasm_result.txt"

    if os.path.exists(destination_path):
        try:
            os.remove(destination_path)
            print(f"Deleted existing file at {destination_path}.")
        except Exception as e:
            print(f"Failed to delete existing file: {e}")
            return

    if backend == "compose":
        cluster_name = faasmctl_get_faasm_ini_value(
            ini_file, "Faasm", "cluster_name"
        )
        planner_name = cluster_name + "-planner-1"
        command = [
            "docker",
            "cp",
            f"{planner_name}:{source_path}",
            destination_path,
        ]
    elif backend == "k8s":
        command = [
            "kubectl",
            "cp",
            f"planner:{source_path}",
            destination_path,
            "-n",
            "faasm",
        ]
    else:
        raise RuntimeError("Unsupported backend: {}".format(backend))

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        print(f"Running command (attempt {attempt}): {command}")
        try:
            subprocess.run(command, check=True)
            print("File copied successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred during copy: {e}")

        # Check if the file exists
        if os.path.exists(destination_path):
            print("File exists at destination after copy.")
            return
        else:
            print(f"File not found at {destination_path}, retrying...")

        time.sleep(1)

    # If we reach here, it means the file was not copied successfully after 5 attempts
    raise RuntimeError(
        f"Failed to copy file to {destination_path} after {max_retries} attempts."
    )


def load_app_results(
    file_path: str = "tasks/stream/tmp/faasm_result.txt",
) -> Dict[int, List[Dict[str, Any]]]:
    try:
        with open(file_path, "r") as file:
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


def parse_line(line):
    """
    Parses a line containing one JSON object and a variable number of
    other space-separated components.

    Returns:
        A single list containing all components in order, with the
        JSON part parsed as a Python dictionary. Returns None if parsing fails.
    """
    try:
        # 1. Find the boundaries of the JSON object.
        start_index = line.find("{")
        end_index = line.rfind("}") + 1

        # Return None if the line doesn't contain a JSON object.
        if start_index == -1 or end_index == 0:
            return None

        # 2. Extract the three main sections.
        before_text = line[:start_index]
        json_string = line[
            start_index:end_index
        ]  # This is the raw string we want to keep
        after_text = line[end_index:]

        # 3. Process each section.
        # Split the surrounding text by any whitespace to get the other parts.
        before_parts = before_text.split()
        after_parts = after_text.split()

        # 4. Combine all parts into a single, ordered list.
        result = before_parts + [json_string] + after_parts

        return result

    except (ValueError, json.JSONDecodeError):
        # Handle malformed lines gracefully.
        print(f"Could not parse line: {line}")
        return None


def read_data_from_txt_file(file_path):
    data_vectors = []
    with open(file_path, "r") as file:
        for line in file:
            data = parse_line(line)
            data_vectors.append(data)
    return data_vectors


def read_data_from_txt_file_noparse(file_path):
    data_vectors = []
    with open(file_path, "r") as file:
        for line in file:
            data = line.strip().split()
            data_vectors.append(data)
    return data_vectors


def read_persistent_state_from_txt_file(file_path: str) -> dict[str, str]:
    """
    Reads a text file with space-separated key-value pairs into a dictionary.

    Args:
        file_path (str): The path to the text file.

    Returns:
        dict[str, str]: A dictionary containing the key-value pairs from the file.
    """
    data_map = {}
    print(f"\nReading persistent state from {file_path}...")
    try:
        with open(file_path, "r") as f:
            for line in f:
                # Strip leading/trailing whitespace and split on the first space.
                # This correctly handles values that might contain spaces.
                parts = line.strip().split(" ", 1)
                if len(parts) == 2:
                    key, value = parts
                    data_map[key] = value
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    return data_map
