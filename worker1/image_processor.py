"""
Image processing module for worker nodes
Applies various transformations to image tiles
"""

import cv2
import numpy as np
from PIL import Image
import io
import base64


class ImageProcessor:
    """Handles image transformations for tiles"""

    @staticmethod
    def decode_image(image_data_base64: str) -> np.ndarray:
        """
        Decode base64 image data to numpy array

        Args:
            image_data_base64: Base64 encoded image data

        Returns:
            Image as numpy array
        """
        image_bytes = base64.b64decode(image_data_base64)
        image = Image.open(io.BytesIO(image_bytes))
        return np.array(image)

    @staticmethod
    def encode_image(image_array: np.ndarray) -> str:
        """
        Encode numpy array to base64 string

        Args:
            image_array: Image as numpy array

        Returns:
            Base64 encoded image data
        """
        image = Image.fromarray(image_array)
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        return base64.b64encode(buffer.getvalue()).decode('utf-8')

    @staticmethod
    def apply_grayscale(image: np.ndarray) -> np.ndarray:
        """Convert image to grayscale"""
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
            return cv2.cvtColor(gray, cv2.COLOR_GRAY2RGB)
        return image

    @staticmethod
    def apply_blur(image: np.ndarray, kernel_size: int = 5) -> np.ndarray:
        """Apply Gaussian blur"""
        return cv2.GaussianBlur(image, (kernel_size, kernel_size), 0)

    @staticmethod
    def apply_edge_detection(image: np.ndarray) -> np.ndarray:
        """Apply Canny edge detection"""
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        else:
            gray = image

        edges = cv2.Canny(gray, 100, 200)
        return cv2.cvtColor(edges, cv2.COLOR_GRAY2RGB)

    @staticmethod
    def apply_sharpen(image: np.ndarray) -> np.ndarray:
        """Apply sharpening filter"""
        kernel = np.array([[-1, -1, -1],
                          [-1,  9, -1],
                          [-1, -1, -1]])
        return cv2.filter2D(image, -1, kernel)

    @staticmethod
    def apply_brightness(image: np.ndarray, beta: int = 30) -> np.ndarray:
        """Increase brightness"""
        return cv2.convertScaleAbs(image, alpha=1, beta=beta)

    @staticmethod
    def apply_contrast(image: np.ndarray, alpha: float = 1.5) -> np.ndarray:
        """Adjust contrast"""
        return cv2.convertScaleAbs(image, alpha=alpha, beta=0)

    @staticmethod
    def apply_invert(image: np.ndarray) -> np.ndarray:
        """Invert colors"""
        return cv2.bitwise_not(image)

    @staticmethod
    def apply_sepia(image: np.ndarray) -> np.ndarray:
        """Apply sepia tone effect"""
        kernel = np.array([[0.272, 0.534, 0.131],
                          [0.349, 0.686, 0.168],
                          [0.393, 0.769, 0.189]])

        sepia = cv2.transform(image, kernel)
        sepia = np.clip(sepia, 0, 255)
        return sepia.astype(np.uint8)

    def process_tile(self, image_data_base64: str, effect: str = 'grayscale') -> str:
        """
        Process a single tile with the specified effect

        Args:
            image_data_base64: Base64 encoded image data
            effect: Effect to apply (grayscale, blur, edge, sharpen, etc.)

        Returns:
            Base64 encoded processed image
        """
        # Decode image
        image = self.decode_image(image_data_base64)

        # Apply transformation based on effect
        effect_map = {
            'grayscale': self.apply_grayscale,
            'blur': self.apply_blur,
            'edge': self.apply_edge_detection,
            'sharpen': self.apply_sharpen,
            'brighten': self.apply_brightness,
            'contrast': self.apply_contrast,
            'invert': self.apply_invert,
            'sepia': self.apply_sepia
        }

        processor = effect_map.get(effect, self.apply_grayscale)
        processed = processor(image)

        # Encode back to base64
        return self.encode_image(processed)