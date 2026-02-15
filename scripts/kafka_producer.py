#!/usr/bin/env python3
"""
Kafka Producer — OpenSky Network Flight Data

Fetches live flight data from the OpenSky Network REST API and publishes
JSON messages to a Kafka topic. Designed to run standalone or inside Docker.

Usage:
    # Run locally (host machine)
    python scripts/kafka_producer.py --bootstrap localhost:9093 --topic flight-data --interval 60

    # Run inside Docker network
    python scripts/kafka_producer.py --bootstrap kafka:9092 --topic flight-data --interval 60

    # One-shot mode (fetch once and exit)
    python scripts/kafka_producer.py --bootstrap localhost:9093 --topic flight-data --once

OpenSky REST API (no auth required for anonymous access):
    https://opensky-network.org/api/states/all
    Returns all current airborne flights with position, velocity, altitude, etc.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

# OpenSky state vector field names (positional in the response array)
STATE_FIELDS = [
    "icao24",            # 0  - Unique ICAO 24-bit transponder address
    "callsign",          # 1  - Callsign (flight number)
    "origin_country",    # 2  - Country of origin
    "time_position",     # 3  - Unix timestamp of last position update
    "last_contact",      # 4  - Unix timestamp of last contact
    "longitude",         # 5  - Longitude in degrees
    "latitude",          # 6  - Latitude in degrees
    "baro_altitude",     # 7  - Barometric altitude in meters
    "on_ground",         # 8  - Whether the aircraft is on ground
    "velocity",          # 9  - Ground speed in m/s
    "true_track",        # 10 - Track angle in degrees (clockwise from North)
    "vertical_rate",     # 11 - Vertical rate in m/s
    "sensors",           # 12 - Sensor IDs (array)
    "geo_altitude",      # 13 - Geometric altitude in meters
    "squawk",            # 14 - Transponder squawk code
    "spi",               # 15 - Special purpose indicator
    "position_source",   # 16 - Origin of position (0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM)
    "category",          # 17 - Aircraft category (if available)
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka-producer")


# ---------------------------------------------------------------------------
# OpenSky data fetcher
# ---------------------------------------------------------------------------
def fetch_opensky_flights(
    bbox: dict | None = None,
    username: str | None = None,
    password: str | None = None,
) -> list[dict]:
    """
    Fetch current flight states from OpenSky Network API.

    Args:
        bbox: Optional bounding box {lamin, lamax, lomin, lomax} to filter by area.
        username/password: Optional OpenSky credentials for higher rate limits.

    Returns:
        List of flight dictionaries with named fields.
    """
    params = {}
    if bbox:
        params.update(bbox)

    auth = None
    if username and password:
        auth = (username, password)

    try:
        resp = requests.get(OPENSKY_API_URL, params=params, auth=auth, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("Failed to fetch OpenSky data: %s", e)
        return []

    states = data.get("states", [])
    if not states:
        logger.warning("OpenSky returned 0 flight states")
        return []

    # Convert positional arrays to named dictionaries
    flights = []
    for state in states:
        flight = {}
        for i, field_name in enumerate(STATE_FIELDS):
            if i < len(state):
                flight[field_name] = state[i]
            else:
                flight[field_name] = None

        # Clean up
        if flight.get("callsign"):
            flight["callsign"] = flight["callsign"].strip()

        # Add ingestion metadata
        flight["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        flight["_source"] = "opensky"

        flights.append(flight)

    return flights


# ---------------------------------------------------------------------------
# Kafka publisher
# ---------------------------------------------------------------------------
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=100,
        batch_size=32768,
    )


def publish_flights(
    producer: KafkaProducer,
    topic: str,
    flights: list[dict],
) -> int:
    """
    Publish flight records to Kafka topic.
    Uses icao24 (transponder ID) as the message key for partition routing.

    Returns:
        Number of messages successfully sent.
    """
    sent = 0
    for flight in flights:
        key = flight.get("icao24", "unknown")
        try:
            producer.send(topic, key=key, value=flight)
            sent += 1
        except Exception as e:
            logger.error("Failed to send message for %s: %s", key, e)

    producer.flush()
    return sent


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run_producer(
    bootstrap_servers: str,
    topic: str,
    interval: int = 60,
    once: bool = False,
    bbox: dict | None = None,
    username: str | None = None,
    password: str | None = None,
):
    """
    Main producer loop. Fetches from OpenSky and publishes to Kafka.

    Args:
        bootstrap_servers: Kafka broker addresses.
        topic: Target Kafka topic.
        interval: Seconds between fetches (OpenSky updates every ~10s, but rate-limited).
        once: If True, fetch once and exit.
        bbox: Optional geographic bounding box filter.
        username/password: Optional OpenSky credentials.
    """
    logger.info("=" * 60)
    logger.info("🛫 OpenSky → Kafka Producer")
    logger.info("   Broker : %s", bootstrap_servers)
    logger.info("   Topic  : %s", topic)
    logger.info("   Mode   : %s", "one-shot" if once else f"every {interval}s")
    if bbox:
        logger.info("   BBox   : %s", bbox)
    logger.info("=" * 60)

    producer = create_producer(bootstrap_servers)
    run_count = 0

    try:
        while True:
            run_count += 1
            logger.info("--- Fetch #%d ---", run_count)

            flights = fetch_opensky_flights(bbox=bbox, username=username, password=password)

            if flights:
                sent = publish_flights(producer, topic, flights)
                logger.info(
                    "✅ Published %d/%d flights to topic '%s'",
                    sent, len(flights), topic,
                )
            else:
                logger.warning("No flights to publish")

            if once:
                break

            logger.info("Sleeping %ds until next fetch...", interval)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("\n🛑 Producer stopped by user")
    finally:
        producer.close()
        logger.info("Producer closed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="OpenSky Network → Kafka Flight Data Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Continuous mode — fetch every 60 seconds
  python kafka_producer.py --bootstrap localhost:9093 --topic flight-data

  # One-shot mode — fetch once and exit
  python kafka_producer.py --bootstrap localhost:9093 --topic flight-data --once

  # Filter by geographic area (e.g., India)
  python kafka_producer.py --bootstrap localhost:9093 --topic flight-data \\
      --lamin 6.5 --lamax 35.5 --lomin 68.0 --lomax 97.5

  # With OpenSky credentials (higher rate limits)
  python kafka_producer.py --bootstrap localhost:9093 --topic flight-data \\
      --username myuser --password mypass
        """,
    )

    parser.add_argument("--bootstrap", default="localhost:9093",
                        help="Kafka bootstrap servers (default: localhost:9093)")
    parser.add_argument("--topic", default="flight-data",
                        help="Kafka topic name (default: flight-data)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Seconds between fetches (default: 60)")
    parser.add_argument("--once", action="store_true",
                        help="Fetch once and exit (no loop)")

    # Geographic bounding box
    parser.add_argument("--lamin", type=float, help="Min latitude")
    parser.add_argument("--lamax", type=float, help="Max latitude")
    parser.add_argument("--lomin", type=float, help="Min longitude")
    parser.add_argument("--lomax", type=float, help="Max longitude")

    # OpenSky credentials
    parser.add_argument("--username", help="OpenSky username (optional)")
    parser.add_argument("--password", help="OpenSky password (optional)")

    args = parser.parse_args()

    bbox = None
    if all(v is not None for v in [args.lamin, args.lamax, args.lomin, args.lomax]):
        bbox = {
            "lamin": args.lamin,
            "lamax": args.lamax,
            "lomin": args.lomin,
            "lomax": args.lomax,
        }

    run_producer(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        interval=args.interval,
        once=args.once,
        bbox=bbox,
        username=args.username,
        password=args.password,
    )


if __name__ == "__main__":
    main()
