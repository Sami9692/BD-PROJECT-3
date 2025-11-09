#!/usr/bin/env python3
"""
Worker Node for Distributed Image Processing
Consumes tasks from Kafka, processes image tiles, and publishes results
"""

from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import os
import time
import threading
import signal
import sys

from image_processor import ImageProcessor

# Configuration
# Prefer explicit envs but default to your provided IPs
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS',
                                    os.getenv('KAFKA_BROKER', '172.28.218.152:9092'))
KAFKA_TASKS_TOPIC = os.getenv('KAFKA_TASKS_TOPIC', 'tasks')
KAFKA_RESULTS_TOPIC = os.getenv('KAFKA_RESULTS_TOPIC', 'results')
KAFKA_HEARTBEATS_TOPIC = os.getenv('KAFKA_HEARTBEATS_TOPIC', 'heartbeats')
WORKER_ID = os.getenv('WORKER_ID', 'worker-unknown')
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 5))
PROCESSING_EFFECT = os.getenv('PROCESSING_EFFECT', 'grayscale')

# NEW: Redis defaults (master node)
REDIS_HOST = os.getenv('REDIS_HOST', '172.28.103.33')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))

# Make Redis config visible to any downstream component that reads env
os.environ['REDIS_HOST'] = REDIS_HOST
os.environ['REDIS_PORT'] = str(REDIS_PORT)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(f'Worker-{WORKER_ID}')


class Worker:
    """Distributed image processing worker node"""

    def _init_(self, worker_id: str, bootstrap_servers: str):
        """
        Initialize worker

        Args:
            worker_id: Unique worker identifier
            bootstrap_servers: Kafka bootstrap servers
        """
        self.worker_id = worker_id
        self.bootstrap_servers = bootstrap_servers
        self.running = False
        self.processed_count = 0

        # Initialize image processor
        self.processor = ImageProcessor()

        # Initialize Kafka consumer for tasks
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000  # 5 minutes
        })
        self.consumer.subscribe([KAFKA_TASKS_TOPIC])

        # Initialize Kafka producer for results and heartbeats
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': worker_id,
            'compression.type': 'lz4',
            'linger.ms': 10
        })

        logger.info(f"Worker {worker_id} initialized")

    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_heartbeat(self):
        """Send heartbeat message to Kafka"""
        heartbeat = {
            'worker_id': self.worker_id,
            'timestamp': time.time(),
            'status': 'active',
            'processed_count': self.processed_count
        }

        try:
            self.producer.produce(
                topic=KAFKA_HEARTBEATS_TOPIC,
                key=self.worker_id.encode('utf-8'),
                value=json.dumps(heartbeat).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger callbacks
            logger.debug(f"Heartbeat sent")
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")

    def heartbeat_loop(self):
        """Periodic heartbeat sender"""
        while self.running:
            self.send_heartbeat()
            time.sleep(HEARTBEAT_INTERVAL)

    def process_task(self, task: dict) -> dict:
        """
        Process a single tile task

        Args:
            task: Task metadata including image data

        Returns:
            Processed task result
        """
        job_id = task['job_id']
        tile_index = task['tile_index']

        logger.info(f"Processing job {job_id}, tile {tile_index}")

        try:
            # Process the tile
            processed_image_data = self.processor.process_tile(
                task['image_data'],
                effect=PROCESSING_EFFECT
            )

            # Create result
            result = {
                'job_id': job_id,
                'tile_index': tile_index,
                'row': task['row'],
                'col': task['col'],
                'x1': task['x1'],
                'y1': task['y1'],
                'x2': task['x2'],
                'y2': task['y2'],
                'width': task['width'],
                'height': task['height'],
                'image_data': processed_image_data,
                'original_width': task['original_width'],
                'original_height': task['original_height'],
                'total_rows': task['total_rows'],
                'total_cols': task['total_cols'],
                'worker_id': self.worker_id,
                'processed_at': time.time(),
                'effect_applied': PROCESSING_EFFECT
            }

            self.processed_count += 1
            logger.info(f"Processed tile {tile_index} for job {job_id}")

            return result

        except Exception as e:
            logger.error(f"Error processing tile {tile_index}: {e}")
            raise

    def publish_result(self, result: dict):
        """
        Publish processed result to Kafka

        Args:
            result: Processed tile result
        """
        try:
            message = json.dumps(result).encode('utf-8')
            key = result['job_id'].encode('utf-8')

            self.producer.produce(
                topic=KAFKA_RESULTS_TOPIC,
                key=key,
                value=message,
                callback=self.delivery_report
            )

            self.producer.poll(0)  # Trigger callbacks

        except Exception as e:
            logger.error(f"Failed to publish result: {e}")

    def start(self):
        """Start the worker"""
        self.running = True

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            daemon=True
        )
        heartbeat_thread.start()

        logger.info(f"Worker {self.worker_id} started, waiting for tasks...")

        # Main processing loop
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse task
                    task = json.loads(msg.value().decode('utf-8'))

                    # Process task
                    result = self.process_task(task)

                    # Publish result
                    self.publish_result(result)

                except json.JSONDecodeError as e:
                    logger.error(f"Invalid task format: {e}")
                except Exception as e:
                    logger.error(f"Task processing failed: {e}")

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the worker gracefully"""
        logger.info("Stopping worker...")
        self.running = False

        # Send final heartbeat
        self.send_heartbeat()

        # Flush producer
        self.producer.flush()

        # Close consumer
        self.consumer.close()

        logger.info(f"Worker stopped. Processed {self.processed_count} tiles total.")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received")
    sys.exit(0)


if _name_ == '_main_':
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 60)
    logger.info(f"Starting Worker Node: {WORKER_ID}")
    logger.info(f"Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Processing Effect: {PROCESSING_EFFECT}")
    logger.info(f"Redis: host={REDIS_HOST} port={REDIS_PORT}")
    logger.info("=" * 60)

    # Create and start worker
    worker = Worker(WORKER_ID, KAFKA_BOOTSTRAP_SERVERS)
    worker.start()