import requests
import os
import json
import logging
import time
import boto3
import argparse

from datetime import datetime, timezone
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
url = os.getenv("DEMAND_API_URL")
api_key = os.getenv("API_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)




def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=["manual", "incremental"],
        default="manual"
    )

    parser.add_argument("--start-time")
    parser.add_argument("--end-time")

    args = parser.parse_args()

    def convert_time(value):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            raise ValueError("Timestamp must include a timezone")

        dt = dt.astimezone(timezone.utc)

        if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
            raise ValueError("Only whole-hour timestamps are supported")

        return dt.strftime("%Y-%m-%dT%H")

    if args.mode == "manual":
        if not args.start_time or not args.end_time:
            parser.error("Manual mode requires start and end times")

        start_time = convert_time(args.start_time)
        end_time = convert_time(args.end_time)

    else:
        # Earliest date for incremental ingestion
        start_time = "2025-01-01T00"

        end_time = (
            convert_time(args.end_time)
            if args.end_time
            else (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).strftime("%Y-%m-%dT%H")
        )

    if start_time >= end_time:
        raise ValueError("Start time must be earlier than end time")

    return args.mode, start_time, end_time



def get_data(offset,start_time, end_time):
    max_retries = 3
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]":"value",
        "facets[respondent][]": "PJM",
        "facets[type][]": "D",
        "start": start_time,
        "end": end_time,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": offset,
        "length": 10
    }

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching demand data - attempt {attempt}")
            response = requests.get(
                url,
                params=params,
                timeout=30
            )

            # Success
            if response.status_code == 200:
                logger.info("Demand data retrieved successfully.")
                return response.json()

            # Retryable HTTP errors
            elif response.status_code == 429 or response.status_code >= 500:
                logger.warning(
                    f"API returned {response.status_code}."
                )
                if attempt < max_retries:
                    wait_time = 2 ** (attempt - 1)
                    logger.info(
                        f"Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)

            # Non-retryable HTTP errors
            else:
                logger.error(f"HTTP error: {response.status_code}")
                logger.error(f"API response: {response.text[:1000]}")
                return None

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:

            logger.warning(
                f"Network error: {e}"
            )

            if attempt < max_retries:
                wait_time = 2 ** (attempt - 1)
                logger.info(
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None

    logger.error("API failed after maximum retries.")
    return None

def save_to_s3(data, start_time, end_time):
    start_date = start_time.split("T")[0]
    end_date = end_time.split("T")[0]
    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION
        )

        file_path = (
            f"bronze/eia/demand/"
            f"start_date={start_date}/"
            f"end_date={end_date}/"
            f"demand_{start_time}_{end_time}.json"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_path,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        logger.info(
            f"Uploaded {file_path} to {BUCKET_NAME}"
        )

    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed: {e}")
        raise


def get_watermark(key="metadata/watermark.json"):
    try:
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION
        )
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=key
        )
        # Read S3 file content
        body = response["Body"].read()
        # JSON → Python dictionary
        watermark = json.loads(body)
        return watermark["last_retrieved_time"]

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No watermark found. First run.")
            return None

        logger.error(f"Failed to read watermark: {e}")
        raise

def save_watermark(time, key="metadata/watermark.json"):
    file_name = key

    data = {
        "last_retrieved_time": time
    }

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    logger.info(f"Watermark saved: {time}")

def validate_data(all_records, start_time, end_time):
    """Validate that every expected hourly period appears exactly once."""

    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H")
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H")

    # 1. Generate all expected hourly periods
    expected_periods = set()

    current = start_dt
    while current <= end_dt:
        expected_periods.add(current.strftime("%Y-%m-%dT%H"))
        current += timedelta(hours=1)

    # 2. Extract actual periods from API records
    actual_periods = [record["period"] for record in all_records]
    actual_period_set = set(actual_periods)

    # 3. Check for missing, unexpected, or duplicate periods
    missing_periods = expected_periods - actual_period_set
    unexpected_periods = actual_period_set - expected_periods
    duplicate_count = len(actual_periods) - len(actual_period_set)

    if missing_periods or unexpected_periods or duplicate_count:
        raise RuntimeError(
            f"Missing periods: {sorted(missing_periods)}; "
            f"Unexpected periods: {sorted(unexpected_periods)}; "
            f"Duplicate records: {duplicate_count}"
        )

    logger.info(
        f"Validation passed: {len(actual_periods)} hourly periods"
    )


def main(mode, start_time, end_time):
    offset = 0
    all_records = []
    total = None

    if mode == "incremental":
        watermark_key = "metadata/daily_watermark.json"

        # 1. Read the daily watermark
        watermark = get_watermark(watermark_key)

        if watermark is None:
            # First daily run: ingest the latest 24-hour window
            end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H")

            start_time = (
                end_dt - timedelta(hours=24)
            ).strftime("%Y-%m-%dT%H")

            logger.info(
                f"First daily ingestion: {start_time} to {end_time}"
            )

        else:
            # 2. Skip if no new eligible data
            if end_time <= watermark:
                logger.info("No new electricity demand data to process.")
                return

            # 3. Apply 24-hour lookback
            lookback_start = (
                datetime.strptime(watermark, "%Y-%m-%dT%H")
                - timedelta(hours=24)
            )

            start_time = lookback_start.strftime("%Y-%m-%dT%H")

            logger.info(
                f"Daily incremental ingestion: {start_time} to {end_time}"
            )

    else:
        logger.info(
            "Manual backfill: using selected dates; "
            "watermark will remain unchanged."
        )

    # 4. Validate time range
    if start_time >= end_time:
        raise ValueError("Invalid ingestion date range.")

    # 5. Retrieve all API pages
    while total is None or offset < total:
        result = get_data(offset, start_time, end_time)

        if result is None:
            raise RuntimeError("API extraction failed")

        records = result["response"]["data"]
        total = int(result["response"]["total"])

        if not records and offset < total:
            raise RuntimeError(
                "Empty page before extraction completed"
            )

        all_records.extend(records)
        offset += len(records)

        logger.info(f"Retrieved {offset}/{total} records")

    logger.info(
        f"Extraction completed: {len(all_records)} records"
    )

    # 6. Stop if no observations returned
    if not all_records:
        logger.info("No records returned. Watermark unchanged.")
        return

    # 7. Validate hourly completeness
    validate_data(all_records, start_time, end_time)

    # 8. Prepare Bronze JSON
    complete_data = {
        "metadata": {
            "source": "EIA",
            "respondent": "PJM",
            "type": "D",
            "start_time": start_time,
            "end_time": end_time,
            "total_records": len(all_records)
        },
        "data": all_records
    }

    # 9. Upload validated data to S3 Bronze
    save_to_s3(complete_data, start_time, end_time)

    # 10. Update watermark only for daily incremental
    if mode == "incremental":
        last_period = all_records[-1]["period"]

        save_watermark(
            last_period,
            key=watermark_key
        )

        logger.info(
            f"Daily watermark updated to {last_period}"
        )

    else:
        logger.info(
            "Manual backfill completed. Watermark unchanged."
        )


if __name__ == "__main__":
    mode, start_time, end_time = parse_arguments()
    main(mode, start_time, end_time)
