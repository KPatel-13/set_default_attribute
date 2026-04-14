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
}


def write_json_file(path: str, payload: dict) -> None:
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_failure_payload(error_code: str, dataset: str) -> dict:
    error = ERROR_CATALOG[error_code]
    return {
        "result": "failure",
        "errorCode": error_code,
        "summary": error["summary"],
        "details": error["details"],
        "severity": error["severity"],
        "dataset": dataset,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
    }


def resolve_error_code(mode: str, requested_error_code: str) -> str:
    if mode == "random":
        return random.choice(list(ERROR_CATALOG.keys()))
    return requested_error_code


def main() -> int:
    mode = os.getenv("SIM_MODE", "failure").strip().lower()
    requested_error_code = (
        os.getenv("SIM_ERROR_CODE", "MAPPING_FILE_MISSING").strip().upper()
    )
    dataset = os.getenv("SIM_DATASET", "customer-preferences").strip()

    print("Starting set_default_attribute job...")
    print(f"Dataset: {dataset}")

    if mode not in {"failure", "random"}:
        print(f"Unsupported SIM_MODE: {mode}", file=sys.stderr)
        return 2

    if mode == "failure" and requested_error_code not in ERROR_CATALOG:
        print(f"Unsupported SIM_ERROR_CODE: {requested_error_code}", file=sys.stderr)
        return 2

    error_code = resolve_error_code(mode, requested_error_code)
    payload = build_failure_payload(error_code, dataset)
    write_json_file("job_result.json", payload)

    print(f"Job failed with {error_code}.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
