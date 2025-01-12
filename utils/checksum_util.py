import hashlib

from config.logger_config import configure_logger

# Configure logger
logger = configure_logger("checksum.log")

def calculate_checksum(filepath):
    """
    Calculate the SHA-256 checksum of a file.

    :param filepath: Path to the file.
    :return: SHA-256 checksum as a hexadecimal string, or an error message.
    """
    try:
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        checksum = hash_sha256.hexdigest()
        logger.info(f"Checksum calculated successfully for file: {filepath}")
        return checksum
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return "File not found"
    except Exception as e:
        logger.error(f"Error calculating checksum for file {filepath}: {e}")
        return f"Error: {e}"
