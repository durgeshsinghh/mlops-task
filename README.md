# 🚀 MLOps Batch Signal Pipeline

## 📌 Overview

This project implements a **deterministic MLOps-style batch pipeline** that processes OHLCV market data, computes a rolling mean on the `close` price, and generates a binary trading signal.

The system is designed to demonstrate:

* ✅ Reproducibility (config + seed)
* ✅ Observability (logs + metrics)
* ✅ Deployment readiness (Docker)

---

## ⚙️ Features

* YAML-based configuration
* Deterministic execution using seed
* Rolling mean-based signal generation
* Binary signal (1 = buy, 0 = no buy)
* Structured metrics output (`metrics.json`)
* Detailed execution logging (`run.log`)
* Fully Dockerized (one-command run)

---

## 📂 Project Structure

```bash
mlops-task/
│
├── run.py
├── config.yaml
├── data.csv
├── requirements.txt
├── Dockerfile
├── README.md
├── metrics.json
├── run.log
```

---

## 📥 Input Files

### config.yaml

```yaml
seed: 42
window: 5
version: "v1"
```

### data.csv

* Contains OHLCV data
* Only `close` column is used

---

## 🧠 Processing Logic

1. Load and validate configuration
2. Load and validate dataset
3. Compute rolling mean on `close`
4. Generate signal:

   * `1` if close > rolling_mean
   * `0` otherwise
5. Compute metrics:

   * rows_processed
   * signal_rate
   * latency_ms

---

## ▶️ Run Locally

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## 🐳 Run with Docker

```bash
docker build -t mlops-task .
docker run --rm mlops-task
```

---

## 📊 Example Output (metrics.json)

```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 35,
  "seed": 42,
  "status": "success"
}
```

---

## 📝 Logging (run.log)

Logs include:

* Job start timestamp
* Config validation
* Data loading
* Processing steps
* Metrics summary
* Errors (if any)
* Job completion status

---

## 🧪 Error Handling

The pipeline handles:

* Missing input file
* Invalid CSV format
* Empty dataset
* Missing `close` column
* Invalid config structure

In all cases, a valid `metrics.json` is generated.

---

## 🎯 Key Concepts Demonstrated

* **Reproducibility** → Config-driven execution + seed
* **Observability** → Structured logs + metrics JSON
* **Reliability** → Input validation + error handling
* **Deployment** → Docker containerization

---

## 🚀 Conclusion

This project demonstrates how to build a **production-ready batch data pipeline** with strong engineering practices, aligned with real-world MLOps workflows.

---
