# Hands-on L10: Report

**Name:** Soumyanil Ain
**Student ID:** 801488534
**Email:** sain@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: **801488534**

```
seed 801488534: 12 files, 306 events (24 late by 1-2 minutes, 4 late by 8-10 minutes)
```

The commands I ran, in order (same as the README, no deviations):

```bash
python datagen.py 801488534
docker compose up -d

docker cp main.py spark-master:/opt/spark/work-dir/

# Task 1
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared

# Task 2, append (files) and then update (console)
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared update

# Question 3: task 1 again, without deleting its checkpoint
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared

# Looking at the results
cat shared-folder/output/task1/*.csv | grep -vc trip_id
cat shared-folder/output/task2/*.csv | grep -v window_start | sort
ls shared-folder/checkpoint/task1 shared-folder/checkpoint/task2

docker compose down
```

---

## Results

### Task 1: ingest and parse

```
=== task1: micro-batch summary ===
batch input rows            watermark state rows
    0         26                    -          -
    1         22                    -          -
    2         28                    -          -
    3         29                    -          -
    4         24                    -          -
    5         29                    -          -
    6         21                    -          -
    7         20                    -          -
    8         28                    -          -
    9         30                    -          -
   10         22                    -          -
   11         27                    -          -

$ cat shared-folder/output/task1/*.csv | grep -vc trip_id
306
```

Sample rows:

```
trip_id,driver_id,distance_km,fare_amount,fare_per_km,event_time
T00280,D01,26.1,71.96,2.76,2026-10-27 08:10:50
T00281,D02,20.8,34.39,1.65,2026-10-27 08:11:49
```

**How many micro-batches ran, and why that number?** 12 batches (0 to 11), one per file.
There are 12 files, `maxFilesPerTrigger = 1` makes each file its own batch, and the
`availableNow` trigger stops the query once everything that was there at start-up has been
read. The input rows add up to 306, the same as the generator's total and the rows in the
output folder, so nothing was lost: task 1 is stateless, so there is no watermark, no state
and no reason to drop anything. Each row is final the moment it arrives.

### Task 2: trips and fares per window

Windows written by the stream (append mode):

```
$ cat shared-folder/output/task2/*.csv | grep -v window_start | sort
2026-10-27 07:58:00,2026-10-27 08:00:00,7,304.73
2026-10-27 08:00:00,2026-10-27 08:02:00,48,2085.71
2026-10-27 08:02:00,2026-10-27 08:04:00,54,2263.51
2026-10-27 08:04:00,2026-10-27 08:06:00,56,2299.53
2026-10-27 08:06:00,2026-10-27 08:08:00,41,1597.03
```

Micro-batch summary:

```
=== task2: micro-batch summary ===
batch input rows            watermark state rows
    0         26                    -          2
    1         22  2026-10-27 07:58:52          2
    2         28  2026-10-27 07:59:53          3
    3         29  2026-10-27 08:00:55          2
    4         24  2026-10-27 08:01:55          3
    5         29  2026-10-27 08:02:49          2
    6         21  2026-10-27 08:03:57          3
    7         20  2026-10-27 08:04:56          2
    8         28  2026-10-27 08:05:50          3
    9         30  2026-10-27 08:06:49          2
   10         22  2026-10-27 08:07:43          3
   11         27  2026-10-27 08:08:53          2
   12          0  2026-10-27 08:09:49          2
```

Batch table printed at the end:

```
=== task2: the same windows computed in batch over all files ===
+-------------------+-------------------+-----+----------+
|window_start       |window_end         |trips|total_fare|
+-------------------+-------------------+-----+----------+
|2026-10-27 07:58:00|2026-10-27 08:00:00|7    |304.73    |
|2026-10-27 08:00:00|2026-10-27 08:02:00|52   |2214.34   |
|2026-10-27 08:02:00|2026-10-27 08:04:00|54   |2263.51   |
|2026-10-27 08:04:00|2026-10-27 08:06:00|56   |2299.53   |
|2026-10-27 08:06:00|2026-10-27 08:08:00|41   |1597.03   |
|2026-10-27 08:08:00|2026-10-27 08:10:00|53   |2417.0    |
|2026-10-27 08:10:00|2026-10-27 08:12:00|43   |1944.44   |
+-------------------+-------------------+-----+----------+
```

Task 2 ran 13 batches, not 12. Batch 12 has no input: it is the extra "no-data" batch Spark
runs at the end of an `availableNow` stream so the watermark can move forward one last time
(to 08:09:49) and any window it has passed can be written and dropped from state. The state
never holds more than 3 windows, because once the watermark passes a window's end Spark
writes it and evicts it.

---

## Questions

**1. Append versus update.**

| window        | first shows up in update mode | last change in update mode | written in append mode |
| ------------- | ----------------------------- | -------------------------- | ---------------------- |
| 07:58 - 08:00 | batch 0 (7 trips)             | batch 0 (7)                | batch 3                |
| 08:00 - 08:02 | batch 0 (19 trips)            | batch 2 (48)               | batch 5                |
| 08:02 - 08:04 | batch 2 (21 trips)            | batch 5 (54)               | batch 7                |
| 08:04 - 08:06 | batch 4 (22 trips)            | batch 6 (56)               | batch 9                |
| 08:06 - 08:08 | batch 6 (14 trips)            | batch 8 (41)               | batch 11               |
| 08:08 - 08:10 | batch 8 (21 trips)            | batch 11 (53)              | never                  |
| 08:10 - 08:12 | batch 10 (18 trips)           | batch 11 (43)              | never                  |

Update mode prints a window as soon as it gets its first row, and prints it again every time
its count changes, so you see partial numbers (19, then 41, then 48 for 08:00-08:02). Append
mode only writes a window once Spark is sure no more rows can change it, which is when the
watermark passes the window's end. The watermark is the latest event time seen in the
previous batches minus 2 minutes. For 07:58-08:00 the watermark had to reach 08:00:00: in
batch 2 it was 07:59:53 (not yet), in batch 3 it was 08:00:55, so the window was written in
batch 3. The same thing happens for every window: 08:00-08:02 in batch 5 (watermark
08:02:49, batch 4 was still 08:01:55), 08:02-08:04 in batch 7 (08:04:56), 08:04-08:06 in
batch 9 (08:06:49), 08:06-08:08 in batch 11 (08:08:53). So append lags update by about
three batches: two for the window itself plus the 2-minute watermark delay. In exchange, each
window is written exactly once with its final value, which is what a file sink needs, since
it can't go back and edit a file it already committed.

**2. Where did the rows go?**

Only one written window has fewer trips in the stream than in the batch table: **08:00-08:02
has 48 instead of 52, so 4 trips are missing** (2085.71 vs 2214.34, a difference of 128.63 in
fare). Those are exactly the 4 "very late" events that `datagen.py` made by subtracting 8 to
10 minutes from the arrival time:

```
file rides_09 (batch 9):  T00233 08:00:29 59.30, T00247 08:01:00 9.63, T00251 08:01:02 32.07
file rides_10 (batch 10): T00278 08:01:08 27.63
```

They arrived in batches 9 and 10, when the watermark was already 08:06:49 and 08:07:43. Their
event times are way below the watermark, and their window had been written and removed from
state back in batch 5, so Spark dropped them. The 24 events that were only 1 to 2 minutes
late all made it in, because a 2-minute watermark is wide enough for them. (In update mode
you can see the same thing: 08:00-08:02 stops at 48 in batch 2 and never changes again.)

Two windows are **missing from the output altogether: 08:08-08:10 (53 trips) and
08:10-08:12 (43 trips)**. They weren't dropped, they're still sitting in the state store
(the "2 state rows" at the end of the summary). The stream ran out of files before the
watermark got past their end. The last event time seen was 08:11:49, so the final watermark
was 08:09:49, 11 seconds short of 08:10:00. To get them written, the stream has to keep
going: an event with event_time at or after 08:12:00 would push the watermark past 08:10 and
release 08:08-08:10, and one at or after 08:14:00 would release 08:10-08:12. In a real
stream that's simply the next couple of minutes of data. Update mode does show them (it
printed 53 and 43 in batch 11), it just never marks them as final.

Accounting check: 206 trips written + 4 dropped + 96 still in state = 306.

To keep the very late events, the watermark would need to be about 10 minutes instead of 2,
at the cost of holding more state and delaying every append write by ~10 minutes.

**3. Checkpoint.**

Running task 1 again without deleting `shared-folder/checkpoint/task1`, the query starts,
reads its checkpoint, sees that batches 0-11 are committed and that all 12 files are already
in its list of processed files, finds nothing new, and stops right away. The summary shows a
single empty trigger:

```
=== task1: micro-batch summary ===
batch input rows            watermark state rows
   12          0                    -          -
```

Nothing new is written; the output still has 306 rows. That's the point of the checkpoint:
it's how a streaming query knows where it left off, so after a restart it continues instead
of reprocessing (and duplicating) everything.

What's inside the checkpoint:

```
$ ls shared-folder/checkpoint/task1
commits  metadata  offsets  sources
$ ls shared-folder/checkpoint/task2
commits  metadata  offsets  sources  state
```

- `metadata`: one small JSON file with the query's id, so a restarted query knows it's the same query.
- `offsets/`: the write-ahead log, one file per batch (0, 1, ..., 11), written _before_ the batch runs. It records which source offset the batch covers (`{"logOffset":11}`), the batch's watermark and timestamp, and the Spark settings the query depends on.
- `commits/`: one file per batch, written _after_ the batch's output is done. A batch with an offsets entry but no commits entry was interrupted and gets re-run. For task 2 it also stores the next batch's watermark (`nextBatchWatermarkMs`).
- `sources/0/`: the file source's own log of which files were read in which batch (path, modification time, batch id), compacted every 10 batches (`9.compact`). This is what makes the re-run above skip all 12 files.
- `state/` (task 2 only): the state store for the window aggregation, one folder per shuffle partition (0 and 1), with `.delta` files holding the partial count and fare sum of each open window. Task 1 has no state, so it has no `state/` folder.

---

## Problems and fixes

Nothing went wrong; both tasks ran in both modes without errors. The only thing to keep in
mind is the checkpoint: to re-run a task from the start, its checkpoint has to be deleted
first (`rm -rf shared-folder/checkpoint/task1`), otherwise the query finds nothing new and
stops after an empty batch, as in question 3.
