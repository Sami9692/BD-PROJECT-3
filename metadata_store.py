"""
Metadata storage using Redis for job tracking and worker monitoring
"""

import redis
import json
import time
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataStore:
    """Redis-based metadata storage for jobs and worker health"""

    def __init__(self, host: str = '172.28.103.33', port: int = 6379):
        """
        Initialize Redis connection

        Args:
            host: Redis host IP (e.g., 172.28.103.33)
            port: Redis port (default: 6379)
        """
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                decode_responses=True
            )
            # Simple ping to verify connection
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {host}:{port}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis at {host}:{port} → {e}")
            raise

        # Heartbeat timeout in seconds (worker inactive threshold)
        self.heartbeat_timeout = 15

    # ─────────────────────────────
    # Job Management
    # ─────────────────────────────

    def create_job(self, job_id: str, metadata: Dict):
        """Create a new job entry"""
        job_key = f"job:{job_id}"
        metadata['status'] = 'processing'
        metadata['created_at'] = time.time()
        metadata['processed_tiles'] = 0

        self.redis_client.hset(job_key, mapping={
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in metadata.items()
        })
        logger.info(f"🆕 Created job {job_id}")

    def update_job(self, job_id: str, updates: Dict):
        """Update job metadata"""
        job_key = f"job:{job_id}"
        self.redis_client.hset(job_key, mapping={
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in updates.items()
        })

    def get_job(self, job_id: str) -> Optional[Dict]:
        """Retrieve job metadata"""
        job_key = f"job:{job_id}"
        data = self.redis_client.hgetall(job_key)

        if not data:
            return None

        # Parse JSON and numeric fields
        for key in ['created_at', 'processed_tiles', 'total_tiles']:
            if key in data:
                try:
                    if key == 'created_at':
                        data[key] = float(data[key])
                    else:
                        data[key] = int(data[key])
                except ValueError:
                    pass

        return data

    def increment_processed_tiles(self, job_id: str) -> int:
        """Increment count of processed tiles for a job"""
        job_key = f"job:{job_id}"
        return self.redis_client.hincrby(job_key, 'processed_tiles', 1)

    def store_tile_result(self, job_id: str, tile_index: int, result: Dict):
        """Store processed tile result temporarily"""
        tile_key = f"tile:{job_id}:{tile_index}"
        self.redis_client.setex(tile_key, 3600, json.dumps(result))  # 1 hour expiry

    def get_all_tiles(self, job_id: str, total_tiles: int) -> List[Dict]:
        """Retrieve all processed tiles for reconstruction"""
        tiles = []
        for i in range(total_tiles):
            tile_key = f"tile:{job_id}:{i}"
            tile_data = self.redis_client.get(tile_key)
            if tile_data:
                tiles.append(json.loads(tile_data))
            else:
                logger.warning(f"⚠️ Missing tile {i} for job {job_id}")
        return tiles

    def complete_job(self, job_id: str):
        """Mark job as completed"""
        self.update_job(job_id, {
            'status': 'completed',
            'completed_at': time.time()
        })
        logger.info(f"🏁 Job {job_id} completed successfully.")

    # ─────────────────────────────
    # Worker Health Monitoring
    # ─────────────────────────────

    def update_worker_heartbeat(self, worker_id: str, metadata: Dict = None):
        """Update or insert worker heartbeat"""
        worker_key = f"worker:{worker_id}"
        data = {
            'worker_id': worker_id,
            'last_heartbeat': time.time(),
            'status': 'active'
        }

        if metadata:
            data.update(metadata)

        self.redis_client.hset(worker_key, mapping={
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in data.items()
        })

    def get_active_workers(self) -> List[Dict]:
        """Return list of active workers based on heartbeat freshness"""
        active_workers = []
        current_time = time.time()
        worker_keys = self.redis_client.keys("worker:*")

        for key in worker_keys:
            worker_data = self.redis_client.hgetall(key)
            if worker_data:
                try:
                    last_heartbeat = float(worker_data.get('last_heartbeat', 0))
                    elapsed = current_time - last_heartbeat
                    if elapsed < self.heartbeat_timeout:
                        worker_data['last_heartbeat'] = last_heartbeat
                        worker_data['time_since_heartbeat'] = round(elapsed, 2)
                        active_workers.append(worker_data)
                    else:
                        # Mark inactive
                        self.redis_client.hset(key, 'status', 'inactive')
                except Exception:
                    continue

        return active_workers

    def get_worker_count(self) -> int:
        """Return number of active workers"""
        return len(self.get_active_workers())

