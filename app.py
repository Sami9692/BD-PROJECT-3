"""
Master Node Flask Application
Handles image upload, job orchestration, and monitoring dashboard
"""

from flask import Flask, request, render_template, jsonify, send_file
import os
import uuid
import logging
from werkzeug.utils import secure_filename

from tiling import ImageTiler
from kafka_producer import TaskProducer
from kafka_consumer import ResultConsumer, HeartbeatConsumer
from metadata_store import MetadataStore

# ===========================
# Configuration
# ===========================

UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', '/home/pes2ug23cs519/master/uploads')
PROCESSED_FOLDER = os.getenv('PROCESSED_FOLDER', '/home/pes2ug23cs519/master/processed')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '172.28.218.152:9092')
KAFKA_TASKS_TOPIC = os.getenv('KAFKA_TASKS_TOPIC', 'tasks')
KAFKA_RESULTS_TOPIC = os.getenv('KAFKA_RESULTS_TOPIC', 'results')
KAFKA_HEARTBEATS_TOPIC = os.getenv('KAFKA_HEARTBEATS_TOPIC', 'heartbeats')

REDIS_HOST = os.getenv('REDIS_HOST', '172.28.103.33')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

TILE_SIZE = int(os.getenv('TILE_SIZE', 512))
MIN_IMAGE_SIZE = int(os.getenv('MIN_IMAGE_SIZE', 1024))

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# ===========================
# Flask App Setup
# ===========================

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB max file size

# ===========================
# Logging Setup
# ===========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# Initialize Components
# ===========================

tiler = ImageTiler(tile_size=TILE_SIZE)
task_producer = TaskProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TASKS_TOPIC)
result_consumer = ResultConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_RESULTS_TOPIC)
heartbeat_consumer = HeartbeatConsumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_HEARTBEATS_TOPIC)
metadata_store = MetadataStore(REDIS_HOST, REDIS_PORT)

# ===========================
# Callback Handlers
# ===========================

def handle_tile_result(result):
    """Callback for processing tile results"""
    job_id = result['job_id']
    tile_index = result['tile_index']

    # Store tile result in Redis
    metadata_store.store_tile_result(job_id, tile_index, result)

    # Increment processed count
    processed = metadata_store.increment_processed_tiles(job_id)

    # Get job info
    job = metadata_store.get_job(job_id)
    if job:
        total_tiles = int(job.get('total_tiles', 0))
        logger.info(f"Job {job_id}: {processed}/{total_tiles} tiles processed")

        # If all tiles are processed → reconstruct image
        if processed >= total_tiles:
            logger.info(f"All tiles received for job {job_id}, reconstructing image...")
            reconstruct_image(job_id, total_tiles)


def handle_heartbeat(heartbeat):
    """Callback for processing worker heartbeats"""
    worker_id = heartbeat.get('worker_id')
    metadata_store.update_worker_heartbeat(worker_id, heartbeat)


def reconstruct_image(job_id, total_tiles):
    """Reconstruct the final image from processed tiles"""
    try:
        tiles = metadata_store.get_all_tiles(job_id, total_tiles)

        if len(tiles) < total_tiles:
            logger.error(f"Missing tiles for job {job_id}: {len(tiles)}/{total_tiles}")
            return

        output_path = os.path.join(app.config['PROCESSED_FOLDER'], f"{job_id}.png")
        tiler.reconstruct_image(tiles, output_path)

        metadata_store.complete_job(job_id)
        logger.info(f"✅ Image reconstructed successfully: {output_path}")

    except Exception as e:
        logger.error(f"Failed to reconstruct image for job {job_id}: {e}")


# ===========================
# Kafka Consumers
# ===========================

result_consumer.set_callback(handle_tile_result)
heartbeat_consumer.set_callback(handle_heartbeat)
result_consumer.start_consuming()
heartbeat_consumer.start_consuming()

# ===========================
# Routes
# ===========================

@app.route('/')
def index():
    """Main page with upload form"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_image():
    """Handle image upload and start processing"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        job_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{job_id}_{filename}")
        file.save(filepath)

        tiles = tiler.split_image(filepath, job_id)

        metadata_store.create_job(job_id, {
            'filename': filename,
            'total_tiles': len(tiles),
            'tile_size': TILE_SIZE
        })

        published = task_producer.publish_tasks(tiles)
        logger.info(f"📦 Job {job_id} created with {published} tasks")

        return jsonify({
            'job_id': job_id,
            'total_tiles': len(tiles),
            'status': 'processing'
        }), 200

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/status/<job_id>')
def job_status(job_id):
    """Get job status"""
    job = metadata_store.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    total = int(job.get('total_tiles', 0))
    processed = int(job.get('processed_tiles', 0))
    progress = (processed / total * 100) if total > 0 else 0

    return jsonify({
        'job_id': job_id,
        'status': job.get('status'),
        'total_tiles': total,
        'processed_tiles': processed,
        'progress': round(progress, 2)
    })


@app.route('/result/<job_id>')
def get_result(job_id):
    """Download processed image"""
    output_path = os.path.join(app.config['PROCESSED_FOLDER'], f"{job_id}.png")

    if not os.path.exists(output_path):
        return jsonify({'error': 'Result not ready'}), 404

    return send_file(output_path, mimetype='image/png')


@app.route('/dashboard')
def dashboard():
    """Monitoring dashboard"""
    return render_template('dashboard.html')


@app.route('/api/workers')
def get_workers():
    """Get active workers"""
    workers = metadata_store.get_active_workers()
    return jsonify({'workers': workers, 'count': len(workers)})


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'})


# ===========================
# Run Flask App
# ===========================

if __name__ == '__main__':
    logger.info("🚀 Starting Master Node...")
    logger.info(f"Kafka Broker → {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Redis Server → {REDIS_HOST}:{REDIS_PORT}")

    app.run(
        host=os.getenv('MASTER_HOST', '0.0.0.0'),
        port=int(os.getenv('MASTER_PORT', 5000)),
        debug=True
    )

