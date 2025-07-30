import os
import sys
from enum import Enum
from typing import List, Tuple, Dict, Optional

NAME: str = "Movies Analysis"
INSTANCE_SEPARATOR: str = "="
MOVIES_NETWORK_NAME: str = "movies_network"

RESULTS_OUTPUT_FILE_NAME: str = "actual_results"
RESULTS_OUTPUT_FILE_EXTENSION: str = ".json"
RESULTS_OUTPUT_DIR_PATH: str = os.path.join(
    os.path.dirname(__file__), "..", "test")


def indent(text: str, level: int) -> str:
    return "  " * level + text


class ServiceType(Enum):
    SERVER = "SERVER"
    PROXY = "PROXY"
    CLIENT = "CLIENT"
    RABBIT_MQ = "RABBITMQ"
    FILTER = "FILTER"
    JOINER = "JOINER"
    TOP = "TOPPER"
    OVERVIEW = "OVERVIEWER"
    MAP = "MAPPER"
    REDUCE = "REDUCER"
    MERGE = "MERGER"
    HEALTH = "HEALTH"

    def to_service(
        self, id: int, instances_per_service: Dict["ServiceType", int]
    ) -> "Service":
        match self:
            case ServiceType.PROXY:
                server_count = instances_per_service.get(ServiceType.SERVER, 0)
                depends_on = (
                    {
                        f"server_{i}": {"condition": "service_started"}
                        for i in range(server_count)
                    }
                    if server_count > 0
                    else None
                )

                return Service(
                    container_name=f"proxy",
                    image="proxy:latest",
                    environment={
                        "PROXY_PORT": str(8080),
                        "PROXY_SERVER_COUNT": str(server_count),
                    },
                    networks=[MOVIES_NETWORK_NAME],
                    depends_on=depends_on,
                    volumes={
                        "./proxy/config.yaml": "/app/config.yaml",
                    },
                )

            case ServiceType.SERVER:
                depends_on = {"rabbitmq": {"condition": "service_healthy"}}

                filter_count = instances_per_service.get(ServiceType.FILTER, 0)
                for i in range(filter_count):
                    depends_on[f"filter_{i}"] = {
                        "condition": "service_started"}

                overview_count = instances_per_service.get(
                    ServiceType.OVERVIEW, 0)
                for i in range(overview_count):
                    depends_on[f"overviewer_{i}"] = {
                        "condition": "service_started"}

                joiner_count = instances_per_service.get(ServiceType.JOINER, 0)
                for i in range(joiner_count):
                    depends_on[f"joiner_{i}"] = {
                        "condition": "service_started"}

                return Service(
                    container_name=f"server_{id}",
                    image="server:latest",
                    environment={
                        "SERVER_PORT": str(8080 + id),
                        "SERVER_ID": str(id),
                        "SERVER_NAME": f"server_{id}",
                        "SERVER_FILTER_COUNT": str(
                            instances_per_service.get(ServiceType.FILTER, 0)
                        ),
                        "SERVER_OVERVIEW_COUNT": str(
                            instances_per_service.get(ServiceType.OVERVIEW, 0)
                        ),
                        "SERVER_MAP_COUNT": str(
                            instances_per_service.get(ServiceType.MAP, 0)
                        ),
                        "SERVER_JOIN_COUNT": str(
                            instances_per_service.get(ServiceType.JOINER, 0)
                        ),
                        "SERVER_REDUCE_COUNT": str(
                            instances_per_service.get(ServiceType.REDUCE, 0)
                        ),
                        "SERVER_MERGE_COUNT": str(
                            instances_per_service.get(ServiceType.MERGE, 0)
                        ),
                        "SERVER_TOP_COUNT": str(
                            instances_per_service.get(ServiceType.TOP, 0)
                        ),
                    },
                    networks=[MOVIES_NETWORK_NAME],
                    depends_on=depends_on,
                    volumes={
                        "./server/config.yaml": "/app/config.yaml",
                        "./rabbitConfig.yaml": "/app/rabbitConfig.yaml",
                    },
                )
            case ServiceType.HEALTH:
                return Service(
                    container_name=f"health_{id}",
                    image="health:latest",
                    environment={
                        "HEALTH_ID": str(id),
                        "HEALTH_NAME": f"health_{id}",
                        "HEALTH_FILTER_COUNT": str(
                            instances_per_service.get(ServiceType.FILTER, 0)
                        ),
                        "HEALTH_OVERVIEW_COUNT": str(
                            instances_per_service.get(ServiceType.OVERVIEW, 0)
                        ),
                        "HEALTH_MAP_COUNT": str(
                            instances_per_service.get(ServiceType.MAP, 0)
                        ),
                        "HEALTH_JOIN_COUNT": str(
                            instances_per_service.get(ServiceType.JOINER, 0)
                        ),
                        "HEALTH_REDUCE_COUNT": str(
                            instances_per_service.get(ServiceType.REDUCE, 0)
                        ),
                        "HEALTH_MERGE_COUNT": str(
                            instances_per_service.get(ServiceType.MERGE, 0)
                        ),
                        "HEALTH_TOP_COUNT": str(
                            instances_per_service.get(ServiceType.TOP, 0)
                        ),
                        "HEALTH_HEALTH_COUNT": str(
                            instances_per_service.get(ServiceType.HEALTH, 0)
                        ),
                        "HEALTH_SERVER_COUNT": str(
                            instances_per_service.get(ServiceType.SERVER, 0)
                        ),
                    },
                    depends_on={"rabbitmq": {"condition": "service_healthy"}},
                    networks=[MOVIES_NETWORK_NAME],
                    volumes={
                        "./health/config.yaml": "/app/config.yaml",
                        "./rabbitConfig.yaml": "/app/rabbitConfig.yaml",
                        "/var/run/docker.sock": "/var/run/docker.sock"
                    },
                )
            case ServiceType.CLIENT:
                server_count = instances_per_service.get(ServiceType.SERVER, 0)
                depends_on = {"proxy": {"condition": "service_started"}}

                return Service(
                    container_name=f"client_{id}",
                    image="client:latest",
                    environment={
                        "CLIENT_ID": str(id),
                        "CLIENT_OUTPUT_FILE": "./test/"
                        + RESULTS_OUTPUT_FILE_NAME
                        + f"_client_{id}{RESULTS_OUTPUT_FILE_EXTENSION}",
                    },
                    depends_on={"proxy": {"condition": "service_started"}},
                    networks=[MOVIES_NETWORK_NAME],
                    volumes={
                        "./client/config.yaml": "/app/config.yaml",
                        os.path.abspath("./.data/"): "/app/.data",
                        os.path.abspath(RESULTS_OUTPUT_DIR_PATH): "/app/test/",
                    },
                )
            case ServiceType.RABBIT_MQ:
                return Service(
                    container_name="rabbitmq",
                    image="rabbitmq:4-management",
                    networks=[MOVIES_NETWORK_NAME],
                    ports={"15672": "15672", "5672": "5672"},
                    volumes={"rabbit": "/var/lib/rabbitmq"},
                    environment={
                        "RABBITMQ_DEFAULT_USER": "guest",
                        "RABBITMQ_DEFAULT_PASS": "guest",
                    },
                    healthcheck={
                        "test": ["CMD", "rabbitmq-diagnostics", "ping"],
                        "interval": "10s",
                        "timeout": "5s",
                        "retries": 5,
                    },
                )
            case (
                ServiceType.FILTER
                | ServiceType.OVERVIEW
                | ServiceType.MAP
            ):
                worker_name = self.value.split("_", 1)[0].lower()
                return Service(
                    container_name=f"{worker_name}_{id}",
                    image="worker:latest",
                    environment={
                        "WORKER_ID": str(id),
                        "WORKER_TYPE": self.value,
                        "WORKER_NAME": f"{worker_name}_{id}",
                        "WORKER_FILTER_COUNT": str(
                            instances_per_service.get(ServiceType.FILTER, 0)
                        ),
                        "WORKER_OVERVIEW_COUNT": str(
                            instances_per_service.get(ServiceType.OVERVIEW, 0)
                        ),
                        "WORKER_MAP_COUNT": str(
                            instances_per_service.get(ServiceType.MAP, 0)
                        ),
                        "WORKER_JOIN_COUNT": str(
                            instances_per_service.get(ServiceType.JOINER, 0)
                        ),
                        "WORKER_REDUCE_COUNT": str(
                            instances_per_service.get(ServiceType.REDUCE, 0)
                        ),
                        "WORKER_MERGE_COUNT": str(
                            instances_per_service.get(ServiceType.MERGE, 0)
                        ),
                        "WORKER_TOP_COUNT": str(
                            instances_per_service.get(ServiceType.TOP, 0)
                        ),
                    },
                    networks=[MOVIES_NETWORK_NAME],
                    depends_on={"rabbitmq": {"condition": "service_healthy"}},
                    volumes={
                        "./worker/config.yaml": "/app/config.yaml",
                        "./rabbitConfig.yaml": "/app/rabbitConfig.yaml",
                    },
                )

            case (
                ServiceType.JOINER
                | ServiceType.TOP
                | ServiceType.REDUCE
                | ServiceType.MERGE
            ):

                worker_name = self.value.split("_", 1)[0].lower()
                volume_name = f"{worker_name}_{id}_data"
                mount_path = f"/app/data/{worker_name}_{id}"

                return Service(
                    container_name=f"{worker_name}_{id}",
                    image="worker:latest",
                    environment={
                        "WORKER_ID": str(id),
                        "WORKER_TYPE": self.value,
                        "WORKER_NAME": f"{worker_name}_{id}",
                        "WORKER_DATA_DIR": mount_path,
                        "WORKER_FILTER_COUNT": str(
                            instances_per_service.get(ServiceType.FILTER, 0)
                        ),
                        "WORKER_OVERVIEW_COUNT": str(
                            instances_per_service.get(ServiceType.OVERVIEW, 0)
                        ),
                        "WORKER_MAP_COUNT": str(
                            instances_per_service.get(ServiceType.MAP, 0)
                        ),
                        "WORKER_JOIN_COUNT": str(
                            instances_per_service.get(ServiceType.JOINER, 0)
                        ),
                        "WORKER_REDUCE_COUNT": str(
                            instances_per_service.get(ServiceType.REDUCE, 0)
                        ),
                        "WORKER_TOP_COUNT": str(
                            instances_per_service.get(ServiceType.TOP, 0)
                        ),
                        "WORKER_MERGE_COUNT": str(
                            instances_per_service.get(ServiceType.MERGE, 0)
                        ),
                    },
                    networks=[MOVIES_NETWORK_NAME],
                    depends_on={"rabbitmq": {"condition": "service_healthy"}},
                    volumes={
                        "./worker/config.yaml": "/app/config.yaml",
                        "./rabbitConfig.yaml": "/app/rabbitConfig.yaml",
                        volume_name: mount_path,
                    },
                )


class DockerCompose:
    name: str
    services: Optional[List["Service"]]
    networks: Optional[List["Network"]]
    volumes: Optional[List["Volume"]]

    def __init__(
        self,
        name: str,
        services: Optional[List["Service"]] = None,
        networks: Optional[List["Network"]] = None,
        volumes: Optional[List["Volume"]] = None,
    ) -> None:
        self.name: str = name
        self.services = services if services is not None else []
        self.networks = networks if networks is not None else []
        self.volumes = volumes if volumes is not None else []

    def __str__(self) -> str:
        lines: List[str] = []
        lines.append(f"name: {self.name}")

        if self.services:
            lines.append("services:")
            for service in self.services:
                lines.append(str(service))

        if self.networks:
            lines.append("networks:")
            for network in self.networks:
                lines.append(str(network))

        if self.volumes:
            lines.append("volumes:")
            for volume in self.volumes:
                lines.append(str(volume))

        return "\n".join(lines) + "\n"


class Service:
    container_name: str
    image: str
    entrypoint: Optional[str]
    networks: Optional[List[str]]
    environment: Optional[Dict[str, str]]
    depends_on: Optional[Dict[str, dict]]
    volumes: Optional[Dict[str, str]]
    ports: Optional[Dict[str, str]]
    healthcheck: Optional[Dict[str, any]]
    indent_level: int

    def __init__(
        self,
        container_name: str,
        image: str,
        entrypoint: Optional[str] = None,
        environment: Optional[Dict[str, str]] = None,
        networks: Optional[List[str]] = None,
        depends_on: Optional[Dict[str, dict]] = None,
        ports: Optional[Dict[str, str]] = None,
        volumes: Optional[Dict[str, str]] = None,
        healthcheck: Optional[Dict[str, any]] = None,
        indent_level: int = 1,
    ) -> None:
        self.container_name = container_name
        self.image = image
        self.entrypoint = entrypoint
        self.environment = environment
        self.networks = networks
        self.depends_on = depends_on
        self.ports = ports
        self.volumes = volumes
        self.healthcheck = healthcheck
        self.indent_level = indent_level

    def __str__(self) -> str:
        level = self.indent_level
        lines = [indent(f"{self.container_name}:", level)]
        lines.append(
            indent(f"container_name: {self.container_name}", level + 1))
        lines.append(indent(f"image: {self.image}", level + 1))
        if self.entrypoint:
            lines.append(indent(f"entrypoint: {self.entrypoint}", level + 1))

        if self.environment:
            lines.append(indent("environment:", level + 1))
            for key, value in self.environment.items():
                lines.append(indent(f"- {key}={value}", level + 2))

        if self.networks:
            lines.append(indent("networks:", level + 1))
            for network in self.networks:
                lines.append(indent(f"- {network}", level + 2))

        if self.depends_on:
            lines.append(indent("depends_on:", level + 1))
            for service_name, options in self.depends_on.items():
                if options:
                    lines.append(indent(f"{service_name}:", level + 2))
                    for k, v in options.items():
                        lines.append(indent(f"{k}: {v}", level + 3))
                else:
                    lines.append(indent(f"- {service_name}", level + 2))

        if self.ports:
            lines.append(indent("ports:", level + 1))
            for key, value in self.ports.items():
                lines.append(indent(f"- {key}:{value}", level + 2))

        if self.volumes:
            lines.append(indent("volumes:", level + 1))
            for key, value in self.volumes.items():
                lines.append(indent(f"- {key}:{value}", level + 2))

        if self.healthcheck:
            lines.append(indent("healthcheck:", level + 1))
            for key, value in self.healthcheck.items():
                if isinstance(value, list):
                    lines.append(indent(f"{key}:", level + 2))
                    for item in value:
                        lines.append(indent(f"- {item}", level + 3))
                else:
                    lines.append(indent(f"{key}: {value}", level + 2))

        return "\n".join(lines) + "\n"


class Network:
    network_name: str
    indent_level: int

    def __init__(self, network_name: str, indent_level: int = 1) -> None:
        self.network_name: str = network_name
        self.indent_level: int = indent_level

    def __str__(self) -> str:
        level: int = self.indent_level

        lines: List[str] = []
        lines.append(indent(f"{self.network_name}:", level))

        return "\n".join(lines) + "\n"


class Volume:
    volume_name: str
    indent_level: int

    def __init__(self, volume_name: str, indent_level: int = 1) -> None:
        self.volume_name: str = volume_name
        self.indent_level: int = indent_level

    def __str__(self) -> str:
        level: int = self.indent_level

        lines: List[str] = []
        lines.append(indent(f"{self.volume_name}:", level))

        return "\n".join(lines)


def get_args() -> Tuple[str, str]:
    try:
        return sys.argv[1], sys.argv[2]
    except IndexError:
        print(
            "Usage: python3 generate_compose.py <path_to_instances_configuration> <path_to_output_docker_compose>"
        )
        sys.exit(1)


def read_instances(file_path: str) -> Dict["ServiceType", int]:
    instances_per_service: Dict[ServiceType, int] = {}

    with open(file_path, "r") as file:
        for line in file:
            try:
                service_type_str, count_str = line.strip().split(INSTANCE_SEPARATOR, 1)
                service_type: ServiceType = ServiceType(
                    service_type_str.strip().upper()
                )
                count: int = int(count_str.strip())

                instances_per_service[service_type] = count
            except (ValueError, KeyError):
                print(f"Invalid line in instances file: {line.strip()}")
                continue

    return instances_per_service


def generate_docker_compose(
    instances_per_service: Dict[ServiceType, int],
) -> DockerCompose:
    services: List[Service] = [
        service_type.to_service(id, instances_per_service)
        for service_type, count in instances_per_service.items()
        for id in range(count)
    ]

    networks: List[Network] = [
        Network(
            network_name=MOVIES_NETWORK_NAME,
        )
    ]

    volumes_dict: Dict[str, Volume] = {
        name: Volume(volume_name=name)
        for service in services
        if service.volumes
        for name in service.volumes.keys()
        if name and not name.startswith((".", "/"))  # only named volumes
    }

    volumes: List[Volume] = list(volumes_dict.values())

    docker_compose = DockerCompose(
        name=NAME, services=services, networks=networks, volumes=volumes
    )

    return docker_compose


def write_to_file(output_file: str, compose: DockerCompose) -> None:
    with open(output_file, "w") as file:
        file.write(str(compose))


def main() -> None:
    n_instances_path, output_file_path = get_args()
    instances_per_service = read_instances(n_instances_path)
    docker_compose: DockerCompose = generate_docker_compose(
        instances_per_service)
    write_to_file(output_file_path, docker_compose)


if __name__ == "__main__":
    main()
