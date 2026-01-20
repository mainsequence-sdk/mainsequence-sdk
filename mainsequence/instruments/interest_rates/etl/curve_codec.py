import base64
import gzip
import json
from typing import Any


def compress_curve_to_string(curve_dict: dict[Any, Any]) -> str:
    """
    Serializes, compresses, and encodes a curve dictionary into a single,
    transport-safe text string.

    Pipeline: Dict -> JSON -> Gzip (binary) -> Base64 (text)

    Args:
        curve_dict: The Python dictionary representing the curve.

    Returns:
        A Base64-encoded string of the Gzipped JSON.
    """
    # 1. Serialize the dictionary to a compact JSON string, then encode to bytes
    json_bytes = json.dumps(curve_dict, separators=(',', ':')).encode('utf-8')

    # 2. Compress the JSON bytes using the universal Gzip standard
    compressed_bytes = gzip.compress(json_bytes)

    # 3. Encode the compressed binary data into a text-safe Base64 string
    base64_bytes = base64.b64encode(compressed_bytes)

    # 4. Decode the Base64 bytes into a final ASCII string for storage/transport
    return base64_bytes.decode('ascii')


def decompress_string_to_curve(b64_string: str) -> dict[Any, Any]:
    """
    Decodes, decompresses, and deserializes a string back into a curve dictionary.

    Pipeline: Base64 (text) -> Gzip (binary) -> JSON -> Dict

    Args:
        b64_string: The Base64-encoded string from the database or API.

    Returns:
        The reconstructed Python dictionary.
    """
    # 1. Encode the ASCII string back into Base64 bytes
    base64_bytes = b64_string.encode('ascii')

    # 2. Decode the Base64 to get the compressed Gzip bytes
    compressed_bytes = base64.b64decode(base64_bytes)

    # 3. Decompress the Gzip bytes to get the original JSON bytes
    json_bytes = gzip.decompress(compressed_bytes)

    # 4. Decode the JSON bytes to a string and parse back into a dictionary
    return json.loads(json_bytes.decode('utf-8'))


