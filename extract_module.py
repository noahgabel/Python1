"""
Extract Module - ETL Pipeline
This module provides three methods to download data from remote sources.
"""

import requests
import wget
import subprocess
import os
from urllib.parse import urlparse


def extract_with_requests(url, destination_folder):
    """
    Method 1: Extract data using the requests library.

    Security considerations:
    - HTTPS: Uses requests library which automatically handles HTTPS connections securely
    - Command-line injection: Not applicable - no shell commands are executed
    - URL validation: Uses urlparse to parse URL components safely

    Robustness:
    - Streaming download: Downloads data in chunks (stream=True) to handle large files
    - Error handling: Raises exceptions on HTTP errors using raise_for_status()
    - Timeout: Could add timeout parameter to prevent hanging connections
    """

    # Extract filename from URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    destination_path = os.path.join(destination_folder, filename)

    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    # Download with streaming to handle large files robustly
    # stream=True downloads in chunks rather than loading entire file into memory
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()  # Raise exception for HTTP errors

    # Write in chunks for robustness
    with open(destination_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # Filter out keep-alive chunks
                f.write(chunk)

    print(f"[OK] Downloaded with requests: {destination_path}")
    return destination_path


def extract_with_wget(url, destination_folder):
    """
    Method 2: Extract data using the wget Python library.

    Security considerations:
    - HTTPS: wget library supports HTTPS connections automatically
    - Command-line injection: Not applicable - uses Python library, not shell commands
    - URL validation: wget library validates URLs internally

    Robustness:
    - Built-in retry: wget has built-in retry mechanisms
    - Resume capability: Can resume interrupted downloads (not enabled here)
    - Error handling: Returns filename or None on failure
    """

    # Extract filename from URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)

    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    # Change to destination folder for download
    original_dir = os.getcwd()
    os.chdir(destination_folder)

    try:
        # wget.download() handles chunked downloads automatically
        downloaded_file = wget.download(url)
        print(f"\n[OK] Downloaded with wget: {downloaded_file}")
        destination_path = os.path.join(destination_folder, filename)
    finally:
        os.chdir(original_dir)

    return destination_path


def extract_with_subprocess_curl(url, destination_folder):
    """
    Method 3: Extract data using subprocess to execute curl command.

    Security considerations:
    - HTTPS: curl supports HTTPS by default
    - Command-line injection: CRITICAL - Uses list form of subprocess.run() to prevent injection
      * subprocess.run(['curl', url]) is safe - arguments are passed as list, not string
      * subprocess.run('curl ' + url, shell=True) would be UNSAFE - vulnerable to injection
    - URL validation: Limited - relies on curl's validation

    Robustness:
    - Progress bar: curl can show progress with --progress-bar
    - Retry: Could add --retry flag for automatic retries
    - Resume: Could add -C - flag to resume interrupted downloads
    - Timeout: --max-time flag could be added to prevent hanging

    Cross-platform note:
    - curl may not be available on all Windows systems by default
    - More reliable on Linux/macOS systems
    """

    # Extract filename from URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    destination_path = os.path.join(destination_folder, filename)

    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    # SECURITY: Use list form to prevent command injection
    # This ensures URL is treated as a single argument, not interpreted as shell commands
    command = [
        'curl',
        '-L',  # Follow redirects
        '-o', destination_path,  # Output file
        '--progress-bar',  # Show progress
        url
    ]

    # Execute curl command safely
    # shell=False is critical for security - prevents shell injection
    result = subprocess.run(command, shell=False, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"curl failed: {result.stderr}")

    print(f"[OK] Downloaded with curl: {destination_path}")
    return destination_path
