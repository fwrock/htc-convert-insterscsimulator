import json
import gzip
import logging
import os
from pathlib import Path
from typing import Any, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_output_dir(output_path: Path):
    """Creates the output directory if it does not exist."""
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory created/confirmed: {output_path}")
    except OSError as e:
        logger.error(f"Error creating output directory {output_path}: {e}")
        raise

def save_json(data: Any, filepath: Path, pretty: bool, use_gzip: bool):
    """Saves data to a JSON file, optionally pretty-formatted and/or compressed."""
    final_path = filepath.with_suffix('.json.gz' if use_gzip else '.json')
    logger.debug(f"Saving file to: {final_path}")
    try:
        if use_gzip:
            with gzip.open(final_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f, indent=4 if pretty else None, ensure_ascii=False)
        else:
            with open(final_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4 if pretty else None, ensure_ascii=False)
        logger.debug(f"File successfully saved: {final_path}")
    except (IOError, TypeError) as e:
        logger.error(f"Error saving file {final_path}: {e}")
        raise

def generate_resource_id(prefix: str, index: int) -> str:
    """Generates a resource ID in DTMI format."""
    return f"{prefix}{index}"

def generate_actor_id(prefix: str, original_id: str) -> str:
    """Generates an actor ID in DTMI format."""
    # Ensures the original ID does not contain characters that could break DTMI (optional but safe)
    safe_original_id = str(original_id).replace(";", "_").replace(":", "_")
    return f"{prefix}{safe_original_id}"

def chunk_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """Splits a list into chunks with a maximum size of chunk_size."""
    if chunk_size <= 0:
        return [data]  # Returns the entire list if the chunk size is invalid
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
