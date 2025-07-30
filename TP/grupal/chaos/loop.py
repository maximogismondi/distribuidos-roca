import multiprocessing
from os import getenv
import random
from re import I
import subprocess
import sys
from pathlib import Path
from time import sleep
from tracemalloc import stop
from typing import List, Dict
from random import choice, normalvariate, random
import signal
import os


BLUE = "\033[94m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

cwd = Path(__file__).parent.parent
docker_compose_file = str(cwd / "docker-compose.yaml")
compose_base_cmd = ["docker", "compose", "-f", docker_compose_file]


def get_container_names_from_compose(compose_file_path: str) -> List[str]:
    """
    Extract all container names from a docker-compose.yaml file without dependencies

    Args:
        compose_file_path: Path to the docker-compose.yaml file

    Returns:
        List of container names
    """
    try:
        container_names = []

        with open(compose_file_path, 'r') as file:
            for line in file:
                # Look for container_name definitions
                line = line.strip()
                if line.startswith('container_name:'):
                    # Extract the container name (after the colon and space)
                    container_name = line.split(
                        'container_name:', 1)[1].strip()
                    container_names.append(container_name)

        # Sort for consistent output
        container_names.sort()

        return container_names

    except FileNotFoundError:
        print(f"Error: File {compose_file_path} not found")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []


def categorize_containers(container_names: List[str]) -> Dict:
    """
    Categorize containers by their type based on prefix
    """
    categories = {
        "no_clients": [],
        "clients": [],
    }

    for name in container_names:
        if name.startswith("client_"):
            categories["clients"].append(name)
        else:
            categories["no_clients"].append(name)

    return categories


def print_containers(containers_by_category):
    """
    Print containers organized by category
    """
    print("=== Docker Containers ===")

    print("\n== Servers ==")
    for server in containers_by_category["servers"]:
        print(f"- {server}")

    print("\n== Clients ==")
    for client in containers_by_category["clients"]:
        print(f"- {client}")

    print("\n== Infrastructure ==")
    for infra in containers_by_category["infrastructure"]:
        print(f"- {infra}")

    print("\n== Workers ==")
    for worker_type, workers in containers_by_category["workers"].items():
        if workers:
            print(f"\n= {worker_type.capitalize()} Workers =")
            for worker in workers:
                print(f"- {worker}")


def get_containers() -> Dict[str, str]:
    # Path to the docker-compose.yaml file
    compose_file_path = Path(__file__).parent.parent / "docker-compose.yaml"

    # Get all container names from compose file
    container_names = get_container_names_from_compose(str(compose_file_path))

    if not container_names:
        print("No container names found in the docker-compose.yaml file")
        sys.exit(1)

    # Categorize containers

    # Print containers by category
    if getenv("DEBUG"):
        containers_by_category = categorize_containers(container_names)
        print_containers(containers_by_category)
        print(f"\nTotal containers: {len(container_names)}")

    # Return all container names as a flat list
    return categorize_containers(container_names)


def run_client(client_name: str, expected_results_file: str = "expected_results.json"):
    running = True

    def handle_sigterm(signum, frame):
        print(f"{BLUE}Received SIGTERM in {client_name}, shutting down...{RESET}")
        nonlocal running
        running = False
        stop_cmd = compose_base_cmd + ["stop", client_name]
        print(f"{BLUE}Running cmd: {' '.join(stop_cmd)}{RESET}")
        subprocess.run(stop_cmd, stdout=sys.stdout, stderr=sys.stderr)

    signal.signal(signal.SIGTERM, handle_sigterm)

    iteration = 0
    try:
        while running:
            up_cmd = compose_base_cmd + ["up", client_name]
            log_file_path = f"{str(cwd)}/logs/{client_name}_{iteration}.log"

            delete_log_file = False
            with open(log_file_path, "w", encoding="utf-8") as log_file:
                print(
                    f"{BLUE}Starting client: {client_name} for iteration {iteration}{RESET}")
                exec = subprocess.run(up_cmd, stdout=log_file, stderr=log_file)

                if exec.returncode == 0:
                    delete_log_file = True

                    check_cmd = [
                        "python3", f"{cwd}/test/compare_results.py",
                        f"actual_results_{client_name}.json", expected_results_file
                    ]

                    check = subprocess.run(
                        check_cmd, stdout=log_file, stderr=log_file)

                    if check.returncode != 0:
                        print(
                            f"{RED}Results for {client_name} do not match expected results. Check logs {log_file_path}{RESET}"
                        )
                        delete_log_file = False

            if delete_log_file:
                print(
                    f"{YELLOW}Client {client_name} finished successfully in iteration {iteration}{RESET}")
                os.remove(log_file_path)

            iteration += 1
    except KeyboardInterrupt:
        print(f"{RED}Client {client_name} interrupted and exiting cleanly.{RESET}")


def validate_expected_results_file() -> str:
    expected_results_file = sys.argv[1]
    if not expected_results_file.endswith(".json"):
        print("Expected results file must be a JSON file.")
        sys.exit(1)

    if not (cwd / "test" / expected_results_file).is_file():
        print(f"Expected results file {expected_results_file} does not exist.")
        sys.exit(1)

    if not expected_results_file.startswith("expected_results"):
        print(
            "Expected results file must start with 'expected_results' to match the naming convention."
        )
        sys.exit(1)

    return expected_results_file


def main():
    logs_dir = cwd / "logs"
    logs_dir.mkdir(exist_ok=True)

    for log_file in logs_dir.iterdir():
        if log_file.is_file():
            new_name = log_file.with_name(
                f"{log_file.stem}_old{log_file.suffix}")
            log_file.rename(new_name)

    expected_results_file = validate_expected_results_file()

    container_names = get_containers()

    not_clients = [name for name in container_names["no_clients"]]
    command = compose_base_cmd + ["up"]
    command.extend(not_clients)

    client_processes = [
        multiprocessing.Process(target=run_client, args=(client_name, expected_results_file)) for client_name in container_names["clients"]
    ]

    print(f"{GREEN}Running cmd: {' '.join(command + ['-d'])}{RESET}")

    log_file_path = f"{str(cwd)}/logs/compose.log"
    log_file = sys.stdout
    if not getenv("DEBUG"):
        log_file = open(log_file_path, "w", encoding="utf-8")

    try:
        subprocess.run(command + ['-d'], stdout=log_file, stderr=log_file)

        print(f"{GREEN}Waiting for clients to start...{RESET}")
        for process in client_processes:
            process.start()

        print(f"{GREEN}Attach to compose{RESET}")

        subprocess.run(command, stdout=log_file, stderr=log_file)

        print(f"{GREEN}Waiting for clients to finish...{RESET}")
    except KeyboardInterrupt:
        print(f"{RED}Interrupted by user, terminating clients...{RESET}")
    finally:
        for process in client_processes:
            process.terminate()

        for process in client_processes:
            process.join()

        down_command = compose_base_cmd + [
            "down", "--volumes", "--remove-orphans"
        ]

        print(f"{GREEN}Running cmd: {' '.join(down_command)}{RESET}")
        subprocess.run(down_command, stdout=log_file, stderr=log_file)

        if log_file is not sys.stdout:
            log_file.close()
            print(f"{GREEN}Logs saved to {log_file_path}{RESET}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python loop.py <expected_results_file_name>")
        sys.exit(1)

    main()
