# Real-Time Flight Delay Analysis with Apache Flink

[![Language](https://img.shields.io/badge/language-Python-blue)]()
[![Course](https://img.shields.io/badge/course-Big%20Data%20Systems-red)]()

Can a streaming system process 2.23 million flight events and answer real-time delay queries over tumbling windows? A PyFlink pipeline deployed on a 3-container Docker Compose cluster, processing US BTS flight data for January–April 2025.

**Author:** Daniel Garoz Vazquez (Erasmus exchange)

---

## Problem

Given a continuous stream of US flight events (~2.23 M records, Jan–Apr 2025), answer two queries in real time:

- **Q1** — hourly per-carrier statistics over event-time tumbling windows (streaming, PyFlink DataStream API).
- **Q2** — top-10 airports by severe departure delays over 1h, 6h and full-dataset windows.

## Key results

- **Q1 sustained throughput: ~10,000 events/second** with parallelism = 1; full replay in ~3 minutes at acceleration factor α = 60,000.
- **Parallelism scaling studied** across {1, 2, 4} — results in `benchmarks/q1_benchmark_results.csv`.
- **Q2 hybrid design:** four PyFlink streaming iterations hit Beam Python runtime ceilings (OOM, throughput collapse); documented in Report §IV.A. Final design uses pandas batch on the same event-time-sorted input, preserving semantics.
- **All results bit-identical** across benchmark runs (verified with `diff`).

## Architecture

```text
     events.csv (pre-sorted by event time)
    /                       \
[Feeder]                [pandas batch]
TCP :9999                     |
   |                          v
[Flink JobManager]        Q2 CSVs
   |
[Flink TaskManager]
   |
 Q1 CSV
```

## Tech stack

- **Language:** Python 3
- **Streaming:** Apache Flink / PyFlink (DataStream API)
- **Containerisation:** Docker Compose (3 services: feeder + JobManager + TaskManager)
- **Batch fallback:** pandas
- **Data:** US BTS On-Time Reporting (~2.23 M records, 400 MB)
- **Docs:** LaTeX (IEEE format + Beamer)

## Repository structure

```text
├── src/            Application code (preprocess, Q1 streaming, Q2 batch)
├── feeder/         TCP stream simulator (replays events.csv)
├── docker/         Docker Compose cluster definition
├── benchmarks/     Q1 throughput benchmarks (parallelism 1/2/4)
├── Results/        Query outputs (Q1: 9,765 rows; Q2: 1h/6h/global)
├── Report/         IEEE-format report (~6 pages) + AI declaration
└── slides/         Beamer slides (12 frames, 15-min talk)
```

## How to reproduce

Requirements: Docker Desktop, Python 3.10+, 8 GB RAM minimum.

```bash
# 1. Pre-process (place monthly CSVs under data/ first)
pip install -r requirements.txt
python src/preprocess.py --input data --output data/events.csv

# 2. Start the cluster
docker-compose -f docker/docker-compose.yml up -d --build
sleep 15

# 3. Run Q1 (streaming)
mkdir -p Results/q1 && chmod 777 Results/q1
docker exec sabd2_jobmanager flink run -d \
    -py /opt/flink/app/src/query1.py \
    -pyexec /opt/venv/bin/python \
    -pyclientexec /opt/venv/bin/python

# 4. Run Q2 (batch)
python src/query2_batch.py --events-csv data/events.csv --results-root Results

# 5. Q1 parallelism benchmarks
bash benchmarks/run_q1_benchmarks.sh
```

Full Q1 replay: ~3 min. Q2 batch: ~30 sec. Benchmarks: ~15 min total.

## Note on the use of AI

The implementation was developed with AI-assisted coding. All architectural decisions, experimental design, interpretation of results and written content are the author's own work. See `Report/ai_declaration.pdf` for details.

## Context

Project 2 for *Sistemi e Architetture per Big Data*, Università degli Studi di Roma Tor Vergata, A.A. 2025–26 (Erasmus exchange). Dataset: US BTS On-Time Reporting, Jan–Apr 2025.
