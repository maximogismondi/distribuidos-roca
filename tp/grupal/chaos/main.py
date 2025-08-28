from os import getenv
import random
import subprocess
import sys
from pathlib import Path
from time import sleep
from typing import List, Dict
from random import choice, normalvariate, random

GREEN = "\033[92m"
RESET = "\033[0m"


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


def exclude_external_containers(container_names: List[str]) -> List[str]:
    """
    Exclude containers that are not defined in the docker-compose.yaml file
    """
    # Assuming external containers are those that do not start with a specific prefix
    # For this example, we will exclude any container that does not start with 'server_' or 'worker_'
    return [name for name in container_names if not name.startswith(('client_', 'proxy', 'rabbitmq'))]


def categorize_containers(container_names: List[str]) -> Dict:
    """
    Categorize containers by their type based on prefix
    """
    categories = {
        "servers": [],
        "clients": [],
        "workers": {
            "filter": [],
            "overviewer": [],
            "mapper": [],
            "joiner": [],
            "reducer": [],
            "merger": [],
            "topper": []
        },
        "infrastructure": []
    }

    for name in container_names:
        if name.startswith("server_"):
            categories["servers"].append(name)
        elif name.startswith("client_"):
            categories["clients"].append(name)
        elif name.startswith("filter_"):
            categories["workers"]["filter"].append(name)
        elif name.startswith("overviewer_"):
            categories["workers"]["overviewer"].append(name)
        elif name.startswith("mapper_"):
            categories["workers"]["mapper"].append(name)
        elif name.startswith("joiner_"):
            categories["workers"]["joiner"].append(name)
        elif name.startswith("reducer_"):
            categories["workers"]["reducer"].append(name)
        elif name.startswith("merger_"):
            categories["workers"]["merger"].append(name)
        elif name.startswith("topper_"):
            categories["workers"]["topper"].append(name)
        elif name in ["proxy", "rabbitmq"]:
            categories["infrastructure"].append(name)

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


def get_containers() -> List[str]:
    # Path to the docker-compose.yaml file
    compose_file_path = Path(__file__).parent.parent / "docker-compose.yaml"

    # Get all container names from compose file
    container_names = get_container_names_from_compose(str(compose_file_path))

    if not container_names:
        print("No container names found in the docker-compose.yaml file")
        sys.exit(1)

    container_names = exclude_external_containers(container_names)

    # Categorize containers

    # Print containers by category
    if getenv("DEBUG"):
        containers_by_category = categorize_containers(container_names)
        print_containers(containers_by_category)
        print(f"\nTotal containers: {len(container_names)}")

    # Return all container names as a flat list
    return container_names


def random_malfunction(containers: List[str]) -> None:
    """
    Randomly stop a container from the list
    """
    chosen_container = choice(containers)

    if random() <= 0.8:
        print(f"Killing random container: {chosen_container}")
        # Here you would typically call a command to stop the container, e.g.:
        subprocess.run(["docker", "kill", chosen_container])
    else:
        print(f"Stopping random container: {chosen_container}")
        # Here you would typically call a command to stop the container, e.g.:
        subprocess.run(["docker", "stop", chosen_container])


def loop(func, mu, sigma):
    """
    Loop the given function until it is interrupted
    """
    try:
        i: int = 0
        print(f"Starting loop with mu={mu} and sigma={sigma}\n")

        while True:
            func()
            sleep_time = abs(normalvariate(mu, sigma))
            print(
                f"{GREEN}Next random node malfunction in {sleep_time:.2f} seconds (iteration {i}){RESET}\n")
            sleep(sleep_time)
            i += 1
    except KeyboardInterrupt:
        print("\nLoop interrupted by user.")
        sys.exit(0)


def single_malfunction():
    if len(sys.argv) != 4:
        print("Usage: python main.py single <mu> <sigma>")
        sys.exit(1)

    mu = float(sys.argv[2])
    sigma = float(sys.argv[3])

    container_names = get_containers()

    loop(lambda: random_malfunction(container_names), mu, sigma)


def all_malfunction():
    if len(sys.argv) != 3:
        print("Usage: python main.py all <seconds>")
        sys.exit(1)

    seconds = int(sys.argv[2])

    container_names = get_containers()

    container_names = [
        name for name in container_names if not name.startswith(('health'))
    ]

    container_names = [
        name for name in container_names if not name.startswith('server')]

    print(container_names)

    print(f"Starting random malfunctions every {seconds} seconds\n")

    try:
        while True:
            subprocess.run(["docker", "kill"] + container_names)
            sleep(seconds)
    except KeyboardInterrupt:
        print("\nRandom malfunctions stopped by user.")
        sys.exit(0)


def main():
    if len(sys.argv) <= 2:
        print("Wrong number of arguments.")
        print("Usage: python main.py all <seconds>")
        print("Usage: python main.py single <mu> <sigma>")
        sys.exit(1)

    match sys.argv[1]:
        case "all":
            all_malfunction()
        case "single":
            single_malfunction()
        case _:
            print("Usage: python main.py all <seconds>")
            print("Usage: python main.py single <mu> <sigma>")
            sys.exit(1)


if __name__ == "__main__":
    main()
