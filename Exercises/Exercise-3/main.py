from typing import Optional, List
import boto3
import logging
import gzip
import io

# Create and configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
stream_handler = logging.StreamHandler()

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s %(message)s')
stream_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("log.log")
logger.addHandler(file_handler)

# Vars
bucket_name = "commoncrawl"
key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
client = boto3.client('s3')


# Made by ChatGPT
def main():
    # Step 1: Download the initial gzip file, read in memory, and extract the URI
    file_content = download_file_from_s3()
    first_uri = pull_uri(file_content.split('\n'))

    # Step 2: Stream the second file from the extracted URI, printing line by line
    if first_uri:
        stream_file_from_uri(first_uri)


# Made by ChatGPT
def download_file_from_s3(_key: str = key) -> str:
    obj = client.get_object(Key=key, Bucket=bucket_name)
    compressed_file = obj['Body'].read()
    with gzip.open(io.BytesIO(compressed_file), 'rb') as f:
        file_content = f.read()
    return file_content.decode("utf-8")


def stream_file_from_uri(uri: str):
    logger.info(f"Streaming file from S3 with URI: {uri}")
    response = client.get_object(Bucket=bucket_name, Key=uri)
    compressed_file = response['Body'].read()  # Read the compressed file into memory
    with gzip.open(io.BytesIO(compressed_file), 'rb') as f:
        for line in f:
            try:
                logger.info(line.decode('utf-8'))  # Decode each line after decompression
            except UnicodeDecodeError as e:
                logger.error(f"Unicode decode error: {e}. Skipping problematic line.")
    logger.info("Finished streaming file from S3.")


def download_or_stream(param: str, file_downloaded: Optional[str]) -> Optional[List[str]]:
    if param == "download":
        try:
            return download_file_from_s3().split('\n')
        except Exception as e:
            logger.error(f"Error: {e}")
    elif param == "stream":
        try:
            return download_file_from_s3(file_downloaded).split('\r\n')
        except Exception as e:
            logger.error(f"Error: {e}")
    return None


def pull_uri(file_downloaded: list) -> Optional[str]:  # splits the string list, then returns the first one
    if not file_downloaded:
        return None
    return file_downloaded[0]


def log_file(param: list[str]):
    for line in param:
        logger.info(line)


if __name__ == "__main__":
    main()
