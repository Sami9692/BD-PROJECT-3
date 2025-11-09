"""
Kafka producer for distributing image processing tasks to worker nodes
"""

from confluent_kafka import Producer
import json
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskProducer:
    """Produces image processing tasks to Kafka"""

    def __init__(self, bootstrap_servers: str = "172.28.218.152:9092", topic: str = "tasks"):
        """
        Initialize the task producer

        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., '172.28.218.152:9092')
            topic: Topic to publish tasks to
        """
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'master-producer',
            'compression.type': 'lz4',     # Efficient for image tiles
            'linger.ms': 10,               # Batch up to 10ms for efficiency
            'batch.size': 32768,           # Slightly increased batch size (32 KB)
            'acks': 'all'                  # Ensure message durability
        })

    def delivery_report(self, err, msg):
        """Callback for Kafka message delivery status"""
        if err is not None:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            logger.info(
                f"✅ Task delivered to topic='{msg.topic()}' "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def publish_tasks(self, tiles: List[Dict]) -> int:
        """
        Publish image tiles as tasks to Kafka

        Args:
            tiles: List of tile metadata dictionaries

        Returns:
            Number of tasks successfully published
        """
        published = 0

        for tile in tiles:
            try:
                # Serialize tile metadata into JSON
                message = json.dumps(tile).encode("utf-8")

                # Use job_id as key for ordered delivery within each job
                key = str(tile.get("job_id", "unknown")).encode("utf-8")

                # Send to Kafka
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=message,
                    callback=self.delivery_report
                )

                published += 1

            except BufferError:
                logger.warning("⚠️ Local producer queue is full, flushing...")
                self.producer.flush()
            except Exception as e:
                logger.error(f"❌ Failed to publish task: {e}")

        # Wait until all messages are sent
        self.producer.flush()
        logger.info(f"📤 Published {published} tasks to topic '{self.topic}'")
        return published

    def close(self):
        """Flush and close the producer safely"""
        try:
            self.producer.flush()
            logger.info("🛑 Task producer closed cleanly.")
        except Exception as e:
            logger.error(f"Error while closing producer: {e}")

