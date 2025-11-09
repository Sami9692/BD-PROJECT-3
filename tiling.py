from PIL import Image
import io, base64
from typing import List, Dict

class ImageTiler:
    """Handles image tiling and reconstruction"""

    def __init__(self, tile_size: int = 512):
        self.tile_size = tile_size

    def validate_image(self, image: Image.Image, min_size: int = 1024) -> bool:
        width, height = image.size
        return width >= min_size and height >= min_size

    def split_image(self, image_path: str, job_id: str) -> List[Dict]:
        image = Image.open(image_path).convert("RGB")

        if not self.validate_image(image):
            raise ValueError(
                f"Image must be at least {self.tile_size * 2}x{self.tile_size * 2} pixels"
            )

        width, height = image.size
        tiles = []

        cols = (width + self.tile_size - 1) // self.tile_size
        rows = (height + self.tile_size - 1) // self.tile_size
        tile_index = 0

        for row in range(rows):
            for col in range(cols):
                x1, y1 = col * self.tile_size, row * self.tile_size
                x2, y2 = min(x1 + self.tile_size, width), min(y1 + self.tile_size, height)
                tile = image.crop((x1, y1, x2, y2))

                tile_bytes = io.BytesIO()
                tile.save(tile_bytes, format='PNG')
                tile_meta = {
                    'job_id': job_id,
                    'tile_index': tile_index,
                    'row': row,
                    'col': col,
                    'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2,
                    'width': x2 - x1, 'height': y2 - y1,
                    'image_data': base64.b64encode(tile_bytes.getvalue()).decode('utf-8'),
                    'original_width': width, 'original_height': height,
                    'total_rows': rows, 'total_cols': cols
                }
                tiles.append(tile_meta)
                tile_index += 1

        return tiles

    def reconstruct_image(self, tiles: List[Dict], output_path: str) -> str:
        if not tiles:
            raise ValueError("No tiles provided for reconstruction")

        first_tile = tiles[0]
        result_image = Image.new('RGB', (first_tile['original_width'], first_tile['original_height']))

        for t in tiles:
            tile = Image.open(io.BytesIO(base64.b64decode(t['image_data'])))
            result_image.paste(tile, (t['x1'], t['y1']))

        result_image.save(output_path, format='PNG', quality=95)
        return output_path

