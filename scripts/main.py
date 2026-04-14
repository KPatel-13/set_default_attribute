import json
import os
import random
import sys

# Merged Catalog containing all your specific error scenarios
ERROR_CATALOG = {
    "MAPPING_FILE_MISSING": {
        "summary": "Mapping file does not exist",
        "details": "The job could not find the expected mapping file in the configured input location.",
        "severity": "HIGH",
    },
    "MAPPING_VALUE_MISSING": {
        "summary": "Value does not exist within mapping file",
        "details": "The job found the mapping file, but the expected key/value mapping was missing.",
        "severity": "MEDIUM",
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
    """Writes the job execution result to a JSON file."""
    with open("job_result.json", "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=4)


def main() -> int:
    # Read environment variables
    mode = os.getenv("SIM_MODE", "success").strip().lower()
    requested_error_code = (
        os.getenv("SIM_ERROR_CODE", "OBJECT_NOT_FOUND").strip().upper()
    )
    job_name = os.getenv("SIM_JOB_NAME", "set-default-attribute").strip()

    print(f"Starting simulated operational job: {job_name}...")

    # Success Path
    if mode == "success":
        payload = {
            "result": "success",
            "summary": f"{job_name} completed successfully",
            "details": "The simulated upstream job completed successfully and did not raise a failure ticket.",
            "severity": "LOW",
        }
        write_result(payload)
        print("Job completed successfully.")
        return 0

    # Failure Path Logic
    if mode == "random":
        error_code = random.choice(list(ERROR_CATALOG.keys()))
    elif mode == "failure":
        error_code = requested_error_code
    else:
        print(f"Unknown SIM_MODE: {mode}", file=sys.stderr)
        return 2

    # Fetch error details from catalog
    error = ERROR_CATALOG.get(error_code)
    if error is None:
        print(f"Unknown SIM_ERROR_CODE: {error_code}", file=sys.stderr)
        return 2

    # Build Failure Payload
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
    # Using SystemExit for clean exit code handling
    raise SystemExit(main())
