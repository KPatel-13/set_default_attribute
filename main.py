import json
import os
import random
import sys


ERROR_CATALOG = {
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
