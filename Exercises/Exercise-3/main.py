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


# Made by Yaar
def download_file_from_s3(_key: str = key, file_name:str = "file_name.gz") -> str:
    filename = file_name
    client.download_file(Bucket=bucket_name, Key=_key, Filename=filename)
    with gzip.open(filename, 'rb') as f:
        file_content = f.read()
    return file_content.decode("utf-8")


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


# Made by Yaar:
def main():
    file_dwd = pull_uri(download_or_stream("download", None))
    log_file(download_or_stream("stream", file_dwd))


if __name__ == "__main__":
    main()
