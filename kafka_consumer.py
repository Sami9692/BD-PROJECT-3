"""
Kafka consumers for receiving processed results and heartbeats
"""

from confluent_kafka import Consumer, KafkaError
import json
import logging
from typing import Dict, Callable
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResultConsumer:
    """Consumes processed image tiles from workers"""

    def __init__(
        self,
        bootstrap_servers: str = "172.28.218.152:9092",  # Kafka broker IP
        topic: str = "results",
        group_id: str = "master-group"
    ):
        """
        Initialize the result consumer

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to consume results from
            group_id: Consumer group ID
        """
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000
        })
        self.consumer.subscribe([topic])
        self.running = False
        self.callback = None

    def set_callback(self, callback: Callable[[Dict], None]):
        """Attach a callback function for received messages"""
        self.callback = callback

    def start_consuming(self):
        """Start consuming messages in a separate background thread"""
        self.running = True
        consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        consumer_thread.start()
        logger.info(f"✅ Started consuming results from topic: {self.topic}")

    def _consume_loop(self):
        """Internal loop for consuming Kafka messages"""
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"❌ Consumer error: {msg.error()}")
                    continue

                # Decode JSON message
                result = json.loads(msg.value().decode('utf-8'))

                logger.info(
                    f"📦 Received result for job {result.get('job_id')} | "
                    f"tile {result.get('tile_index')}"
                )

                if self.callback:
                    self.callback(result)

            except Exception as e:
                logger.error(f"Error consuming result message: {e}")

    def stop(self):
        """Stop consuming and close consumer connection"""
        self.running = False
        self.consumer.close()
        logger.info("🛑 Result consumer stopped.")


class HeartbeatConsumer:
    """Consumes worker heartbeat messages"""

    def __init__(
        self,
        bootstrap_servers: str = "172.28.218.152:9092",  # Kafka broker IP
        topic: str = "heartbeats",
        group_id: str = "heartbeat-monitor"
    ):
        """
        Initialize the heartbeat consumer

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to consume heartbeats from
            group_id: Consumer group ID
        """
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',  # Only consume fresh heartbeats
            'enable.auto.commit': True
        })
        self.consumer.subscribe([topic])
        self.running = False
        self.callback = None

    def set_callback(self, callback: Callable[[Dict], None]):
        """Attach a callback function for heartbeat events"""
        self.callback = callback

    def start_consuming(self):
        """Start consuming heartbeats"""
        self.running = True
        consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        consumer_thread.start()
        logger.info(f"❤️ Started monitoring heartbeats from topic: {self.topic}")

    def _consume_loop(self):
        """Internal loop for consuming heartbeats"""
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"❌ Heartbeat consumer error: {msg.error()}")
                    continue

                heartbeat = json.loads(msg.value().decode('utf-8'))
                logger.debug(f"💓 Heartbeat from worker {heartbeat.get('worker_id')}")

                if self.callback:
                    self.callback(heartbeat)

            except Exception as e:
                logger.error(f"Error consuming heartbeat message: {e}")

    def stop(self):
        """Stop consuming heartbeats"""
        self.running = False
        self.consumer.close()
        logger.info("🛑 Heartbeat consumer stopped.")

