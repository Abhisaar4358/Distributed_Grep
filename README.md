# Distributed Grep

A small C++ implementation of distributed grep using the MapReduce execution
model. The project has a master process, multiple worker processes, a simple
TCP protocol, generated input splits, intermediate map output, and final reduce
output.

This is based on the example described in:

> Jeffrey Dean and Sanjay Ghemawat, "MapReduce: Simplified Data Processing on
> Large Clusters", OSDI 2004.

Paper: https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/

The goal is not to rebuild Google's production system. It is a compact version
that makes the core ideas visible: split the input, send map tasks to workers,
partition intermediate results, run reduce tasks, and tolerate simple worker
failure by reassigning timed-out work.

## What It Does

The program searches a collection of text files for a pattern. Each input file
is treated as one map task. A worker reads its assigned file, writes matching
lines into partitioned intermediate files, and reports completion to the master.
Once all map tasks finish, the master assigns reduce tasks. Each reduce task
collects one partition of the intermediate files and writes a final output file.

Example output layout:

```text
data/input-000.txt       -> map task 0
data/input-001.txt       -> map task 1

intermediate/mr-0-0.txt  -> map 0, reduce partition 0
intermediate/mr-0-1.txt  -> map 0, reduce partition 1

output/output-0.txt      -> final reduce output for partition 0
output/output-1.txt      -> final reduce output for partition 1
```

## Project Layout

```text
.
|-- include/
|   `-- mapreduce.h        shared types, constants, and declarations
|-- src/
|   |-- master.cpp         master/coordinator process
|   |-- worker.cpp         worker process
|   |-- mapreduce.cpp      map, reduce, logging, and socket helpers
|   `-- generate_data.cpp  random test data generator
|-- Makefile
`-- README.md
```

Generated directories such as `build/`, `data/`, `output/`, and
`intermediate/` should not be committed.

## Requirements

- Linux, WSL, or another POSIX-like environment
- `g++` with C++17 support
- `make`

This project uses POSIX sockets: `sys/socket.h`, `netinet/in.h`, `arpa/inet.h`,
and `unistd.h`. Native Windows MinGW/MSVC usually show errors for those
headers unless we run the project through WSL.

## Build

Build everything with the Makefile:

```bash
make
```

This builds:

```text
build/bin/master
build/bin/worker
build/bin/generate_data
```

To remove build output and generated runtime data:

```bash
make clean
```

## Run Manually

Start from a clean runtime state:

```bash
rm -rf data output intermediate
mkdir -p data output intermediate
```

Generate test data:

```bash
./build/bin/generate_data data 10 5000 XQZ
```

Arguments:

```text
generate_data <output_dir> [num_files] [lines_per_file] [rare_pattern]
```

The generator creates random English-like text and injects the rare pattern in
about 1 out of every 500 lines. With 10 files and 5000 lines per file, that is
about 100 matching lines.

Start the master in one terminal:

```bash
./build/bin/master XQZ data output intermediate 3
```

Arguments:

```text
master <pattern> <input_dir> <output_dir> <intermediate_dir> [reduce_count] [--expected-workers N]
```

If you plan to start several workers manually, use `--expected-workers` so the
first worker does not begin consuming tasks before the others connect:

```bash
./build/bin/master XQZ data output intermediate 3 --expected-workers 3
```

The master will send `WAIT` responses until three workers are connected, then it
starts assigning map and reduce tasks normally.

Start workers in other terminals:

```bash
./build/bin/worker 127.0.0.1
```

You can start one worker or several. Workers connect to the master, ask for
tasks, run map or reduce work, and exit when the master sends `DONE`.

## One-Command Demo

```bash
make run_demo
```

The demo builds the project, generates input data, starts one master and three
workers in the background, waits for all three workers before assigning tasks,
then prints a small verification summary.

For the cleanest demo output, run:

```bash
make clean
make run_demo
```

The clean step matters because map and reduce outputs are currently written in
append mode.

## Verify Output

Count lines produced by reduce:

```bash
cat output/output-*.txt | wc -l
```

Check that every output line contains the pattern:

```bash
cat output/output-*.txt | grep -v "XQZ" | wc -l
```

Cross-check against the original input:

```bash
grep -c "XQZ" data/*.txt | awk -F: '{sum += $2} END {print sum}'
```

The reduce output count should match the input match count.

## How It Maps to the MapReduce Paper

The paper describes a user-defined `Map` function, a user-defined `Reduce`
function, a master that coordinates work, and workers that execute tasks. This
project follows that structure directly.

| Paper idea | In this project |
| --- | --- |
| Input splits | Files in `data/` |
| Map task | One input file assigned to a worker |
| Map output | `intermediate/mr-<map_id>-<reduce_id>.txt` |
| Partitioning | FNV-1a hash of the matched line modulo `R` |
| Reduce task | One reduce partition assigned to a worker |
| Final output | `output/output-<reduce_id>.txt` |
| Master | `src/master.cpp` |
| Worker | `src/worker.cpp` |

For distributed grep, the map function emits a line when it matches the search
pattern. The reduce function is intentionally simple: it copies matching lines
from intermediate files into the final output file for its partition.

## Protocol

Master and workers communicate over TCP on port `9000`. Messages are plain text
and end with `END`.

Worker ready:

```text
WORKER_READY
END
```

Map assignment:

```text
TASK_TYPE MAP
TASK_ID 0
INPUT data/input-000.txt
PATTERN XQZ
R_COUNT 3
INTER_DIR intermediate
END
```

Reduce assignment:

```text
TASK_TYPE REDUCE
TASK_ID 0
PARTITION 0
INTER_DIR intermediate
OUTPUT output/output-0.txt
END
```

Task completion:

```text
TASK_DONE 0 MAP
END
```

## Fault Tolerance

The master marks assigned tasks as `IN_PROGRESS` and stores the assignment time.
If a task does not complete within the timeout window, the master can reassign
that task to another worker.

This demonstrates the re-execution idea from the MapReduce paper, but it is not
a production-grade fault-tolerance layer yet. In particular:

- timed-out tasks are retried, but worker disconnects are not immediately
  requeued;
- map and reduce files are written in append mode, so a retry can duplicate
  partial output;
- stale task completions are not fully guarded by worker identity.

## Notes

- Data generation is random by default. Re-running `generate_data` creates new
  sentences and new pattern positions.
- Fast workers may complete more tasks than slower workers. Use
  `--expected-workers N` when you want the master to wait for a group of workers
  before starting the job.
- New workers can join while the master is still running, as long as there is
  work left to assign.
