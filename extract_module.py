"""
Extract Module - ETL Pipeline
"""

import requests
import wget
import subprocess
import os
from urllib.parse import urlparse


def extract_with_requests(url, destination_folder):
    """
    Method 1: Extract data using requests library.

    Security:
    - HTTPS support built-in
    - No command-line injection risk (pure Python library)

    Robustness:
    - Streaming download in chunks to handle large files
    - Timeout set to prevent hanging
    """
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    destination_path = os.path.join(destination_folder, filename)

    os.makedirs(destination_folder, exist_ok=True)

    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    with open(destination_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"[OK] Downloaded with requests: {destination_path}")
    return destination_path


def extract_with_wget(url, destination_folder):
    """
    Method 2: Extract data using wget library.

    Security:
    - HTTPS support automatic
    - No command-line injection (Python library)

    Robustness:
    - Built-in retry mechanisms
    - Handles chunked downloads
    """
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)

    os.makedirs(destination_folder, exist_ok=True)

    original_dir = os.getcwd()
    os.chdir(destination_folder)

    try:
        downloaded_file = wget.download(url)
        print(f"\n[OK] Downloaded with wget: {downloaded_file}")
        destination_path = os.path.join(destination_folder, filename)
    finally:
        os.chdir(original_dir)

    return destination_path


def extract_with_subprocess_curl(url, destination_folder):
    """
    Method 3: Extract data using subprocess with curl.

    Security:
    - HTTPS support via curl
    - Command injection prevention: Uses list form subprocess.run(['curl', url])
      instead of string form which would be vulnerable

    Robustness:
    - Progress bar enabled
    - Can handle interrupted downloads
    """
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    destination_path = os.path.join(destination_folder, filename)

    os.makedirs(destination_folder, exist_ok=True)

    # Use list form to prevent command injection
    command = [
        'curl',
        '-L',
        '-o', destination_path,
        '--progress-bar',
        url
    ]

    # shell=False prevents shell injection
    result = subprocess.run(command, shell=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"curl failed: {result.stderr}")

    print(f"[OK] Downloaded with curl: {destination_path}")
    return destination_path
