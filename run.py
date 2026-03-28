import argparse
import pandas as pd
import numpy as np
import yaml
import logging
import json
import time
import sys
from pathlib import Path


# ---------------------------
# Logging Setup
# ---------------------------
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# ---------------------------
# Load & Validate Config
# ---------------------------
def load_config(config_path):
    if not Path(config_path).exists():
        raise ValueError("Config file not found")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception:
        raise ValueError("Invalid YAML format")

    required_keys = ["seed", "window", "version"]

    if not isinstance(config, dict):
        raise ValueError("Invalid config structure")

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    return config


# ---------------------------
# Load & Validate Data
# ---------------------------
def load_data(input_path):
    if not Path(input_path).exists():
        raise ValueError("Input CSV file not found")

    try:
        df = pd.read_csv(input_path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV file is empty")
    
    df.columns = df.columns.str.strip().str.lower()
    if "close" not in df.columns:
        raise ValueError(f"Missing required column: close. Found columns: {list(df.columns)}")

    return df


# ---------------------------
# Main Pipeline
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="MLOps Batch Signal Pipeline")

    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file)

    start_time = time.time()

    # Default error metrics
    metrics = {
        "version": "v1",
        "status": "error"
    }

    try:
        logging.info("Job started")

        # ---------------------------
        # Load Config
        # ---------------------------
        config = load_config(args.config)
        logging.info(f"Config loaded: {config}")

        np.random.seed(config["seed"])

        # ---------------------------
        # Load Data
        # ---------------------------
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # ---------------------------
        # Rolling Mean
        # ---------------------------
        window = int(config["window"])
        logging.info(f"Computing rolling mean with window={window}")

        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # Drop NaN rows
        df = df.dropna()

        # ---------------------------
        # Signal Generation
        # ---------------------------
        logging.info("Generating signal")

        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        # ---------------------------
        # Metrics
        # ---------------------------
        rows_processed = int(len(df))
        signal_rate = float(df["signal"].mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": config["version"],
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        # Write metrics
        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        # Print to stdout (important for Docker)
        print(json.dumps(metrics, indent=2))

        sys.exit(0)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")

        metrics["error_message"] = str(e)

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        print(json.dumps(metrics, indent=2))

        sys.exit(1)


# ---------------------------
# Entry Point
# ---------------------------
if __name__ == "__main__":
    main()