from __future__ import annotations

from confluent_kafka import libversion, version

EXPECTED_CONFLUENT_KAFKA_VERSION = "2.16.0"
EXPECTED_LIBRDKAFKA_VERSION = "2.16.0"


def main() -> None:
    client_version = version()
    library_version = libversion()[0]

    if client_version != EXPECTED_CONFLUENT_KAFKA_VERSION:
        raise SystemExit(
            "unexpected confluent-kafka version: "
            f"expected {EXPECTED_CONFLUENT_KAFKA_VERSION}, got {client_version}"
        )
    if library_version != EXPECTED_LIBRDKAFKA_VERSION:
        raise SystemExit(
            "unexpected librdkafka version: "
            f"expected {EXPECTED_LIBRDKAFKA_VERSION}, got {library_version}"
        )

    print(f"confluent-kafka {client_version}; librdkafka {library_version}")


if __name__ == "__main__":
    main()
