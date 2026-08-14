"""
Order Event Consumer — AWS Lambda (Python 3.12)

Consumes records from AWS Kinesis Data Streams and writes JSON files to S3.
Processes records independently; one failed record does not fail the batch.

Environment variables required:
    BUCKET_NAME  — S3 bucket name (e.g. kinesis-learning-lab)
    RAW_FOLDER   — S3 folder prefix (e.g. raw)

S3 Key format:
    {RAW_FOLDER}/year=YYYY/month=MM/day=DD/hour=HH/{event_id}.json

Notes:
    - In AWS Lambda, AWS_REGION is runtime-provided and reserved.
    - Do not set AWS_REGION manually in Lambda environment configuration.
    - Each Kinesis record is processed independently; failures do not cascade.
"""

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


BUCKET_NAME: str = os.environ["BUCKET_NAME"]
RAW_FOLDER: str = os.environ["RAW_FOLDER"]

AWS_REGION: str | None = (
    os.getenv("APP_AWS_REGION")
    or os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
    or boto3.session.Session().region_name
)

if not AWS_REGION:
    raise RuntimeError(
        "Unable to resolve AWS region. Set APP_AWS_REGION or ensure Lambda runtime region variables are available."
    )


def decode_kinesis_record(kinesis_data: str) -> dict:
    """Decode Base64-encoded Kinesis data and parse as JSON."""
    decoded_bytes = base64.b64decode(kinesis_data)
    payload = json.loads(decoded_bytes.decode("utf-8"))
    return payload


def build_s3_key(event_data: dict) -> str:
    """
    Build S3 key with datetime partitioning.
    
    Format: {RAW_FOLDER}/year=YYYY/month=MM/day=DD/hour=HH/{event_id}.json
    """
    event_id = event_data.get("event_id") or str(uuid.uuid4())
    
    ingestion_timestamp = event_data.get("ingestion_timestamp")
    if not ingestion_timestamp:
        now = datetime.now(timezone.utc)
    else:
        try:
            now = datetime.fromisoformat(ingestion_timestamp.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            now = datetime.now(timezone.utc)
    
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")
    
    s3_key = (
        f"{RAW_FOLDER}/year={year}/month={month}/day={day}/hour={hour}/{event_id}.json"
    )
    return s3_key


def write_to_s3(s3_client, s3_key: str, payload: dict) -> None:
    """Write JSON payload to S3."""
    payload_json = json.dumps(payload, default=str, indent=2)
    payload_bytes = payload_json.encode("utf-8")
    
    logger.info(
        "Writing to S3 | bucket=%s key=%s size_bytes=%d",
        BUCKET_NAME,
        s3_key,
        len(payload_bytes),
    )
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=payload_bytes,
        ContentType="application/json",
    )
    
    logger.info("Successfully wrote to S3 | s3://%s/%s", BUCKET_NAME, s3_key)


def process_kinesis_record(s3_client, kinesis_record: dict) -> tuple[bool, str | None]:
    """
    Process a single Kinesis record.
    
    Returns:
        (success: bool, s3_key: str | None)
    """
    try:
        kinesis_data = kinesis_record.get("kinesis", {}).get("data")
        if not kinesis_data:
            raise ValueError("Missing 'kinesis.data' in record")
        
        logger.info("Decoding Kinesis record | data_length=%d", len(kinesis_data))
        payload = decode_kinesis_record(kinesis_data)
        
        logger.info("Decoded payload | event_id=%s correlation_id=%s",
                    payload.get("event_id"), payload.get("correlationId"))
        
        s3_key = build_s3_key(payload)
        write_to_s3(s3_client, s3_key, payload)
        
        return True, s3_key
    
    except (BotoCoreError, ClientError) as exc:
        logger.exception("AWS API error processing record: %s", exc)
        return False, None
    except (ValueError, json.JSONDecodeError, KeyError) as exc:
        logger.exception("Data processing error: %s", exc)
        return False, None
    except Exception as exc:
        logger.exception("Unexpected error processing record: %s", exc)
        return False, None


def lambda_handler(event: dict, context) -> dict:
    """
    Lambda handler for consuming Kinesis records and writing to S3.
    
    Parameters
    ----------
    event   : dict  — Kinesis event with Records array.
    context : LambdaContext — AWS runtime context.
    
    Returns
    -------
    dict — Summary with records_received, records_written, records_failed.
    """
    logger.info(
        "Lambda invoked | function=%s request_id=%s",
        getattr(context, "function_name", "unknown"),
        getattr(context, "aws_request_id", "unknown"),
    )
    
    records = event.get("Records", [])
    records_received = len(records)
    records_written = 0
    records_failed = 0
    
    logger.info("Received event | records_count=%d", records_received)
    
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    
    for idx, kinesis_record in enumerate(records):
        logger.info("Processing record | index=%d", idx)
        success, s3_key = process_kinesis_record(s3_client, kinesis_record)
        
        if success:
            records_written += 1
            logger.info("Record processed successfully | s3_key=%s", s3_key)
        else:
            records_failed += 1
            logger.warning("Record processing failed | index=%d", idx)
    
    logger.info(
        "Lambda execution complete | received=%d written=%d failed=%d",
        records_received,
        records_written,
        records_failed,
    )
    
    return {
        "records_received": records_received,
        "records_written": records_written,
        "records_failed": records_failed,
    }
