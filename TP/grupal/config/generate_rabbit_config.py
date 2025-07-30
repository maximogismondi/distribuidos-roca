import sys

PACKET_TTL = 1000 * 60 * 2  # 2 minutes
CLIENT_QUEUE_TTL = 1000 * 60 * 60  # 1 hour

FILTER_EXCHANGE = {"name": "filterExchange", "kind": "fanout"}

OVERVIEW_EXCHANGE = {"name": "overviewExchange", "kind": "fanout"}

MAP_EXCHANGE = {"name": "mapExchange", "kind": "fanout"}

REDUCE_EXCHANGE = {"name": "reduceExchange", "kind": "direct"}

JOIN_EXCHANGE = {"name": "joinExchange", "kind": "direct"}

MERGE_EXCHANGE = {"name": "mergeExchange", "kind": "direct"}

TOP_EXCHANGE = {"name": "topExchange", "kind": "direct"}

RESULT_EXCHANGE = {"name": "resultExchange", "kind": "direct"}

EOF_EXCHANGE = {"name": "eofExchange", "kind": "direct"}

CONTROL_EXCHANGE = {"name": "controlExchange", "kind": "direct"}

HEALTH_EXCHANGE = {"name": "healthExchange", "kind": "fanout"}

FILTER_QUEUE = {"name": "filterQueue"}

OVERVIEW_QUEUE = {"name": "overviewQueue"}

MAP_QUEUE = {"name": "mapQueue"}

REDUCE_QUEUE = {"name": ""}

JOIN_QUEUE = {"name": ""}

MERGE_QUEUE = {"name": ""}

TOP_QUEUE = {"name": ""}

CONTROL_QUEUE = {"name": ""}

HEALTH_QUEUE = {"name": ""}

LEADER_QUEUE = {"name": "leaderQueue"}

BROADCAST_ID = ""

BROADCAST_EOF_ROUTING_KEY = "eof"
PING_ROUTING_KEY = "ping"
LEADER_ROUTING_KEY = "leader"

PARKING_EOF_EXCHANGE = {
    "name": "parkingEofExchange",
    "kind": "direct",
}


class Server:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}SERVER:')
        lines.append(f'{" " * 4}exchanges:')

        lines.append(f'{" " * 6}filterExchange:')
        lines.append(f'{" " * 8}name: "{FILTER_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{FILTER_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}overviewExchange:')
        lines.append(f'{" " * 8}name: "{OVERVIEW_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{OVERVIEW_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}joinExchange:')
        lines.append(f'{" " * 8}name: "{JOIN_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{JOIN_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}resultExchange:')
        lines.append(f'{" " * 8}name: "{RESULT_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{RESULT_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}resultQueue:')
        lines.append(f'{" " * 8}exchange: "{RESULT_EXCHANGE["name"]}"')

        return "\n".join(lines) + "\n"


class Filter:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}FILTER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}filterExchange:')
        lines.append(f'{" " * 8}name: "{FILTER_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{FILTER_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}joinExchange:')
        lines.append(f'{" " * 8}name: "{JOIN_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{JOIN_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}mapExchange:')
        lines.append(f'{" " * 8}name: "{MAP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MAP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}resultExchange:')
        lines.append(f'{" " * 8}name: "{RESULT_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{RESULT_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}filterQueue:')
        lines.append(f'{" " * 8}name: "{FILTER_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}filterQueue:')
        lines.append(f'{" " * 8}exchange: "{FILTER_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{FILTER_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Overview:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}OVERVIEWER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}overviewExchange:')
        lines.append(f'{" " * 8}name: "{OVERVIEW_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{OVERVIEW_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}mapExchange:')
        lines.append(f'{" " * 8}name: "{MAP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MAP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}resultExchange:')
        lines.append(f'{" " * 8}name: "{RESULT_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{RESULT_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}overviewQueue:')
        lines.append(f'{" " * 8}name: "{OVERVIEW_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}overviewQueue:')
        lines.append(f'{" " * 8}exchange: "{OVERVIEW_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{OVERVIEW_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Map:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}MAPPER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}mapExchange:')
        lines.append(f'{" " * 8}name: "{MAP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MAP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}reduceExchange:')
        lines.append(f'{" " * 8}name: "{REDUCE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{REDUCE_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}mapQueue:')
        lines.append(f'{" " * 8}name: "{MAP_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}mapQueue:')
        lines.append(f'{" " * 8}exchange: "{MAP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{MAP_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Reduce:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}REDUCER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}reduceExchange:')
        lines.append(f'{" " * 8}name: "{REDUCE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{REDUCE_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}mergeExchange:')
        lines.append(f'{" " * 8}name: "{MERGE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MERGE_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}parkingEofExchange:')
        lines.append(f'{" " * 8}name: "{PARKING_EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{PARKING_EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}reduceQueue:')
        lines.append(f'{" " * 8}name: "{REDUCE_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}reduceQueue:')
        lines.append(f'{" " * 8}exchange: "{REDUCE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{REDUCE_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Join:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}JOINER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}joinExchange:')
        lines.append(f'{" " * 8}name: "{JOIN_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{JOIN_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}mapExchange:')
        lines.append(f'{" " * 8}name: "{MAP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MAP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}parkingEofExchange:')
        lines.append(f'{" " * 8}name: "{PARKING_EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{PARKING_EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}joinQueue:')
        lines.append(f'{" " * 8}name: "{JOIN_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}joinQueue:')
        lines.append(f'{" " * 8}exchange: "{JOIN_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{JOIN_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Merge:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}MERGER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}mergeExchange:')
        lines.append(f'{" " * 8}name: "{MERGE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{MERGE_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}resultExchange:')
        lines.append(f'{" " * 8}name: "{RESULT_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{RESULT_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}topExchange:')
        lines.append(f'{" " * 8}name: "{TOP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{TOP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}parkingEofExchange:')
        lines.append(f'{" " * 8}name: "{PARKING_EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{PARKING_EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}mergeQueue:')
        lines.append(f'{" " * 8}name: "{MERGE_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}mergeQueue:')
        lines.append(f'{" " * 8}exchange: "{MERGE_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{MERGE_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class Top:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}TOPPER:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}topExchange:')
        lines.append(f'{" " * 8}name: "{TOP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{TOP_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}resultExchange:')
        lines.append(f'{" " * 8}name: "{RESULT_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{RESULT_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}eofExchange:')
        lines.append(f'{" " * 8}name: "{EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}parkingEofExchange:')
        lines.append(f'{" " * 8}name: "{PARKING_EOF_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{PARKING_EOF_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}topQueue:')
        lines.append(f'{" " * 8}name: "{TOP_QUEUE["name"]}"')
        lines.append("")
        lines.append(f'{" " * 4}binds:')
        lines.append(f'{" " * 6}topQueue:')
        lines.append(f'{" " * 8}exchange: "{TOP_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}queue: "{TOP_QUEUE["name"]}"')

        return "\n".join(lines) + "\n"


class HealthChecker:
    def __str__(self) -> str:
        lines: list[str] = []
        lines.append(f'{" " * 2}HEALTH:')
        lines.append(f'{" " * 4}exchanges:')
        lines.append(f'{" " * 6}controlExchange:')
        lines.append(f'{" " * 8}name: "{CONTROL_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{CONTROL_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 6}healthExchange:')
        lines.append(f'{" " * 8}name: "{HEALTH_EXCHANGE["name"]}"')
        lines.append(f'{" " * 8}kind: "{HEALTH_EXCHANGE["kind"]}"')
        lines.append("")
        lines.append(f'{" " * 4}queues:')
        lines.append(f'{" " * 6}leaderQueue:')
        lines.append(f'{" " * 8}name: "{LEADER_QUEUE["name"]}"')
        lines.append("")

        return "\n".join(lines) + "\n"


class RabbitConfig:
    exchanges: dict[str, dict[str, str]]
    queues: dict[str, dict[str, str]]

    def __init__(self):
        self.exchanges = {
            "filterExchange": FILTER_EXCHANGE,
            "overviewExchange": OVERVIEW_EXCHANGE,
            "mapExchange": MAP_EXCHANGE,
            "joinExchange": JOIN_EXCHANGE,
            "reduceExchange": REDUCE_EXCHANGE,
            "mergeExchange": MERGE_EXCHANGE,
            "topExchange": TOP_EXCHANGE,
            "resultExchange": RESULT_EXCHANGE,
            "eofExchange": EOF_EXCHANGE,
            "parkingEofExchange": PARKING_EOF_EXCHANGE,
            "controlExchange": CONTROL_EXCHANGE,
            "healthExchange": HEALTH_EXCHANGE,
        }

    def __str__(self):
        lines: list[str] = []
        # define consts
        lines.append("consts:")
        for k, v in self.exchanges.items():
            lines.append(f'{" " * 2}{k}: "{v["name"]}"')

        lines.append(f'{" " * 2}broadcastId: "{BROADCAST_ID}"\n')
        lines.append(
            f'{" " * 2}eofBroadcastRK: "{BROADCAST_EOF_ROUTING_KEY}"\n'
        )
        lines.append(
            f'{" " * 2}controlBroadcastRK: "{PING_ROUTING_KEY}"\n'
        )
        lines.append(
            f'{" " * 2}leaderRK: "{LEADER_ROUTING_KEY}"\n'
        )
        lines.append(
            f'{" " * 2}clientQueueTTL: "{CLIENT_QUEUE_TTL}"\n'
        )

        lines.append("rabbitmq:")
        lines.append(str(Server()))
        lines.append(str(Filter()))
        lines.append(str(Overview()))
        lines.append(str(Map()))
        lines.append(str(Join()))
        lines.append(str(Reduce()))
        lines.append(str(Merge()))
        lines.append(str(Top()))
        lines.append(str(HealthChecker()))

        return "\n".join(lines) + "\n"


def write_to_file(output_file: str, rabbit_config: RabbitConfig) -> None:
    with open(output_file, "w") as file:
        file.write(str(rabbit_config))


def get_output_file() -> str:
    try:
        return sys.argv[1]
    except IndexError:
        print(
            "Usage: python3 generate_rabbit_config.py <path_to_output_yaml_config_file>"
        )
        sys.exit(1)


if __name__ == "__main__":
    output_file_path = get_output_file()
    write_to_file(output_file_path, RabbitConfig())
