"""
Order Event Producer — AWS Lambda (Python 3.12)

Publishes a single order event to an AWS Kinesis Data Stream.
Invoke manually from the AWS Console for testing.

Environment variables required:
    KINESIS_STREAM_NAME  — Name of the target Kinesis Data Stream

Optional environment variable:
    APP_AWS_REGION       — Explicit region override (e.g. ap-southeast-2)

Notes:
    - In AWS Lambda, AWS_REGION is a reserved/runtime-provided variable.
    - Do not set AWS_REGION manually in Lambda environment configuration.
    - For this single-file deployment, set Handler to lambda_function.lambda_handler.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
KINESIS_STREAM_NAME: str = os.environ["KINESIS_STREAM_NAME"]
AWS_REGION: str = (
    os.getenv("APP_AWS_REGION")
    or os.getenv("AWS_REGION")
    or os.getenv("AWS_DEFAULT_REGION")
    or boto3.session.Session().region_name
)

if not AWS_REGION:
    raise RuntimeError(
        "Unable to resolve AWS region. Set APP_AWS_REGION or ensure Lambda runtime region variables are available."
    )

# ---------------------------------------------------------------------------
# Mandatory top-level fields that must be present in the incoming event
# ---------------------------------------------------------------------------
MANDATORY_FIELDS: list[str] = [
    "hook",
    "eventDateTimeUTC",
    "correlationId",
    "CustomerId",
    "partnerId",
    "data",
]

# Mandatory fields inside data
MANDATORY_DATA_FIELDS: list[str] = [
    "orderId",
    "operationType",
    "orderStatus",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def validate_payload(event: dict) -> None:
    """Raise ValueError if any mandatory field is missing or empty."""
    missing = [f for f in MANDATORY_FIELDS if not event.get(f)]
    if missing:
        raise ValueError(f"Missing or empty mandatory top-level fields: {missing}")

    data = event.get("data", {})
    if not isinstance(data, dict):
        raise ValueError("'data' must be a JSON object.")

    missing_data = [f for f in MANDATORY_DATA_FIELDS if not data.get(f)]
    if missing_data:
        raise ValueError(f"Missing or empty mandatory data fields: {missing_data}")


def build_kinesis_record(event: dict) -> dict:
    """
    Enrich the incoming event with metadata and return the record
    that will be written to Kinesis.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "hook": event["hook"],
        "eventDateTimeUTC": event["eventDateTimeUTC"],
        "correlationId": event["correlationId"],
        "CustomerId": event["CustomerId"],
        "partnerId": event["partnerId"],
        "data": event["data"],
    }


def publish_to_kinesis(client, record: dict, partition_key: str) -> dict:
    """Put a single record onto the Kinesis stream and return the raw response."""
    payload_bytes = json.dumps(record, default=str).encode("utf-8")

    logger.info(
        "Publishing record to Kinesis | stream=%s partition_key=%s payload_size_bytes=%d",
        KINESIS_STREAM_NAME,
        partition_key,
        len(payload_bytes),
    )

    response = client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=payload_bytes,
        PartitionKey=partition_key,
    )
    return response


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point for the Lambda function.

    Parameters
    ----------
    event   : dict  — The JSON payload passed when invoking the Lambda.
    context : LambdaContext — AWS runtime context (not used directly).

    Returns
    -------
    dict — Structured JSON response with statusCode, message, and metadata.
    """
    logger.info(
        "Lambda invoked | function=%s request_id=%s",
        getattr(context, "function_name", "unknown"),
        getattr(context, "aws_request_id", "unknown"),
    )
    logger.info("Received event: %s", json.dumps(event, default=str))

    # -- Validation ----------------------------------------------------------
    try:
        validate_payload(event)
    except ValueError as exc:
        logger.error("Payload validation failed: %s", exc)
        return {
            "statusCode": 400,
            "message": "Payload validation failed.",
            "error": str(exc),
        }

    # -- Build enriched record -----------------------------------------------
    record = build_kinesis_record(event)
    partition_key: str = str(event["CustomerId"])

    logger.info(
        "Enriched record built | event_id=%s correlation_id=%s order_id=%s",
        record["event_id"],
        record["correlationId"],
        record["data"].get("orderId"),
    )

    # -- Publish to Kinesis --------------------------------------------------
    try:
        kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
        response = publish_to_kinesis(kinesis_client, record, partition_key)
    except (BotoCoreError, ClientError) as exc:
        logger.exception("Failed to publish record to Kinesis: %s", exc)
        return {
            "statusCode": 500,
            "message": "Failed to publish event to Kinesis.",
            "error": str(exc),
        }

    shard_id = response.get("ShardId", "unknown")
    sequence_number = response.get("SequenceNumber", "unknown")

    logger.info(
        "Record published successfully | event_id=%s shard_id=%s sequence_number=%s",
        record["event_id"],
        shard_id,
        sequence_number,
    )

    return {
        "statusCode": 200,
        "message": "Order event published successfully.",
        "event_id": record["event_id"],
        "ingestion_timestamp": record["ingestion_timestamp"],
        "correlationId": record["correlationId"],
        "orderId": record["data"].get("orderId"),
        "kinesis": {
            "stream": KINESIS_STREAM_NAME,
            "shard_id": shard_id,
            "sequence_number": sequence_number,
            "partition_key": partition_key,
        },
    }
