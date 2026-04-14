import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

ERROR_CATALOG = {
    "MAPPING_FILE_MISSING": {
        "summary": "Attribute mapping file is missing",
        "details": "The workflow could not locate the required attribute mapping file used to determine default values.",
        "severity": "high",
    },
    "MAPPING_FILE_INVALID": {
        "summary": "Attribute mapping file is invalid",
        "details": "The attribute mapping file exists but could not be parsed because the JSON structure is invalid.",
        "severity": "high",
    },
    "MAPPING_ITEM_NOT_FOUND": {
        "summary": "Mapping item does not exist",
        "details": "The requested item key was not found in the attribute mapping file.",
        "severity": "medium",
    },
    "SOURCE_RECORD_NOT_FOUND": {
        "summary": "Source record was not found",
        "details": "The source item required for default attribute assignment could not be found in the upstream data source.",
        "severity": "high",
    },
    "ATTRIBUTE_UPDATE_REJECTED": {
        "summary": "Default attribute update was rejected",
        "details": "The downstream service rejected the default attribute update because the payload failed validation.",
        "severity": "high",
    },
    "DOWNSTREAM_API_UNAVAILABLE": {
        "summary": "Downstream attribute API unavailable",
        "details": "The downstream service required for applying the default attribute was unavailable or returned an unexpected error.",
        "severity": "high",
    },
    "WRITE_TIMEOUT": {
        "summary": "Attribute update timed out",
        "details": "The update operation exceeded the allowed execution time before the default attribute could be written.",
        "severity": "high",
    },
    "MISSING_REQUIRED_FIELD": {
        "summary": "Required source field missing",
        "details": "The source record did not contain a required field needed to derive the default attribute.",
        "severity": "medium",
    },
    "OBJECT_NOT_FOUND": {
        "summary": "Required object does not exist",
        "details": "The workflow could not find the required object in storage.",
        "severity": "HIGH",
    },
    "OUTPUT_WRITE_FAILED": {
        "summary": "Output write failed",
        "details": "The workflow failed while writing the output artifact.",
        "severity": "HIGH",
    },
    "INVALID_INPUT": {
        "summary": "Input validation failed",
        "details": "The workflow received invalid or incomplete input data.",
        "severity": "MEDIUM",
    },
    "DOWNSTREAM_UNAVAILABLE": {
        "summary": "Downstream service unavailable",
        "details": "A required downstream dependency did not respond successfully.",
        "severity": "HIGH",
    },
    "PROCESSING_TIMEOUT": {
        "summary": "Processing timed out",
        "details": "The workflow exceeded the allowed processing time.",
        "severity": "HIGH",
    },
}


def write_result(payload: dict) -> None:
    with open("job_result.json", "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle)


def main() -> int:
    mode = os.getenv("SIM_MODE", "success").strip().lower()
    requested_error_code = (
        os.getenv("SIM_ERROR_CODE", "OBJECT_NOT_FOUND").strip().upper()
    )

    print("Starting simulated operational job...")

    if mode == "success":
        payload = {"result": "success"}
        write_result(payload)
        print("Job completed successfully.")
        return 0

    if mode == "random":
        error_code = random.choice(list(ERROR_CATALOG.keys()))
    elif mode == "failure":
        error_code = requested_error_code
    else:
        print(f"Unknown SIM_MODE: {mode}", file=sys.stderr)
        return 2

    error = ERROR_CATALOG.get(error_code)
    if error is None:
        print(f"Unknown SIM_ERROR_CODE: {error_code}", file=sys.stderr)
        return 2

    payload = {
        "result": "failure",
        "summary": error["summary"],
        "details": error["details"],
        "severity": error["severity"],
    }
    write_result(payload)

    print(f"Job failed with {error_code}.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
