# Spark-Sorting-Local-Sort-vs-Global-Sort

This guide explains the difference between partition-wise sorting (sortWithinPartitions) and global sorting (orderBy) using PySpark.


# Spark-Sorting-Local-Sort-vs-Global-Sort

This guide explains the difference between **partition-wise sorting** (`sortWithinPartitions`) and **global sorting** (`orderBy`) using PySpark.

---

## 🔧 Setup

Assume you have a CSV file `events.csv`:

```csv
user_id,device_id,referrer,host,url,event_time
u1,d1,google.com,a.com,/home,2024-06-01 12:01:00
u2,d2,bing.com,a.com,/about,2024-06-01 12:02:00
u3,d3,yahoo.com,b.com,/home,2024-06-02 11:00:00
u4,d4,duck.com,b.com,/contact,2024-06-02 11:05:00
```

---

## 🛠 Step-by-step PySpark Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc

# Start Spark
spark = SparkSession.builder.appName("SortExample").getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv("events.csv")

# Create an event_date column
df = df.withColumn("event_time_ts", col("event_time").cast("timestamp"))
df = df.withColumn("event_date", date_trunc("day", col("event_time_ts")))
```

---

## 📌 1. Partition-wise Sort (Local Sort)

```python
df_repartitioned = df.repartition(10, "event_date")
df_local_sorted = df_repartitioned.sortWithinPartitions("event_date", "host")
df_local_sorted.explain()
```

✅ **Behavior**:
- Hash partitions by `event_date`
- Sorts **within** each partition
- Faster, but **not globally ordered**

---

## 📌 2. Global Sort (Strict Sort Across Partitions)

```python
df_global_sorted = df.orderBy("event_date", "host")
df_global_sorted.explain()
```

✅ **Behavior**:
- Spark shuffles data using **range partitioning**
- Ensures **globally sorted** output
- Slower, but necessary for full ordering or operations like `collect()` or `limit()`

---

## 📊 Compare the Plans

```python
df_local_sorted.explain(True)     # shows hash partition + local sort
df_global_sorted.explain(True)    # shows range partition + global sort
```

---

## 🔍 Summary Table

| Feature             | `sortWithinPartitions()`       | `orderBy()` (Global Sort)       |
|---------------------|-------------------------------|---------------------------------|
| Scope               | Per partition                  | Whole dataset                   |
| Partitioning        | You must explicitly repartition | Spark handles repartitioning    |
| Shuffle             | Only once (if at all)          | At least twice (hash + range)   |
| Performance         | Faster                         | Slower                          |
| Use case            | If exact order isn’t critical  | When full ordering is required  |

---

## ✅ When to Use What?

- Use `sortWithinPartitions()` when:
  - Writing to partitioned files
  - Ordering within groups is sufficient

- Use `orderBy()` when:
  - You need strict global order (e.g., for `collect()`, `limit()`, or window functions`)


