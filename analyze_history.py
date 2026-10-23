import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


BASE_DIR = Path(__file__).resolve().parent
if not (BASE_DIR / "history").exists():
    BASE_DIR = BASE_DIR.parents[2]

HISTORY_DIR = BASE_DIR / "history"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

HISTORY_FILES = [
    ("Job 1\n(Unoptimized)", HISTORY_DIR / "history-remote-ethereumJob1"),
    ("Job 2\n(Pre-Agg)", HISTORY_DIR / "history-remote-ethereumJob2"),
    ("Job 3\n(Broadcast)", HISTORY_DIR / "history-remote-ethereumJob3"),
]

summary_rows = []
stage_rows = []

for label, fpath in HISTORY_FILES:
    if not fpath.exists():
        raise FileNotFoundError(f"File not found: {fpath}")

    app_start, app_end, job_start, job_end = None, None, None, None
    total_tasks, s3_input_bytes, sw_bytes, sr_bytes = 0, 0, 0, 0
    sw_records, run_time_ms, cpu_time_ns, gc_time_ms = 0, 0, 0, 0

    with open(fpath, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            ev = json.loads(line)
            etype = ev.get("Event")

            if etype == "SparkListenerApplicationStart":
                app_start = ev["Timestamp"]
            elif etype == "SparkListenerApplicationEnd":
                app_end = ev["Timestamp"]
            elif etype == "SparkListenerJobStart":
                t = ev["Submission Time"]
                job_start = t if job_start is None else min(job_start, t)
            elif etype == "SparkListenerJobEnd":
                t = ev["Completion Time"]
                job_end = t if job_end is None else max(job_end, t)
            elif etype == "SparkListenerTaskEnd":
                total_tasks += 1
                tm = ev.get("Task Metrics", {})
                if tm:
                    run_time_ms += tm.get("Executor Run Time", 0)
                    cpu_time_ns += tm.get("Executor CPU Time", 0)
                    gc_time_ms += tm.get("JVM GC Time", 0)
                    sw_bytes += tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    sw_records += tm.get("Shuffle Write Metrics", {}).get("Shuffle Records Written", 0)
                    sr = tm.get("Shuffle Read Metrics", {})
                    sr_bytes += sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0)
            elif etype == "SparkListenerStageCompleted":
                si = ev["Stage Info"]
                accs = {a["Name"]: a["Value"] for a in si.get("Accumulables", []) if "Name" in a and "Value" in a}
                in_b = accs.get("internal.metrics.input.bytesRead", 0)
                sr_b = (
                    accs.get("internal.metrics.shuffle.read.remoteBytesRead", 0)
                    + accs.get("internal.metrics.shuffle.read.localBytesRead", 0)
                )
                if sr_b == 0:
                    s3_input_bytes += in_b

                stage_rows.append({
                    "Job": label.replace("\n", " "),
                    "Stage ID": si["Stage ID"],
                    "Stage Name": si["Stage Name"],
                    "Tasks": si["Number of Tasks"],
                    "Duration (s)": round((si["Completion Time"] - si["Submission Time"]) / 1000.0, 2),
                    "Input/Cache (MB)": round(in_b / (1024**2), 2),
                    "Shuffle Read (MB)": round(sr_b / (1024**2), 2),
                    "Shuffle Write (MB)": round(accs.get("internal.metrics.shuffle.write.bytesWritten", 0) / (1024**2), 2),
                    "Shuffle Write Recs": accs.get("internal.metrics.shuffle.write.recordsWritten", 0),
                })

    summary_rows.append({
        "Job": label.replace("\n", " "),
        "App Duration (s)": round((app_end - app_start) / 1000.0, 2) if app_start and app_end else None,
        "Wall Time (s)": round((job_end - job_start) / 1000.0, 2) if job_start and job_end else None,
        "Executor Time (s)": round(run_time_ms / 1000.0, 2),
        "CPU Time (s)": round(cpu_time_ns / 1e9, 2),
        "GC Time (s)": round(gc_time_ms / 1000.0, 2),
        "Tasks": total_tasks,
        "S3 Read (MB)": round(s3_input_bytes / (1024**2), 2),
        "Shuffle Write (MB)": round(sw_bytes / (1024**2), 2),
        "Shuffle Recs (M)": round(sw_records / 1e6, 2),
    })

df_sum = pd.DataFrame(summary_rows)
df_stages = pd.DataFrame(stage_rows).sort_values(["Job", "Stage ID"])

print("\n=== AWS EMR EXECUTION GLOBAL SUMMARY ===")
print(df_sum.to_string(index=False))
print("\n=== METRIC DETAILS PER STAGE ===")
print(df_stages.to_string(index=False))


labels = [r[0] for r in HISTORY_FILES]
x = np.arange(len(labels))
width = 0.35

fig, axes = plt.subplots(1, 3, figsize=(15, 4.8), dpi=120)

b1 = axes[0].bar(x - width/2, df_sum["Executor Time (s)"], width, label="Executor Run Time (s)", color="#3b82f6")
b2 = axes[0].bar(x + width/2, df_sum["Wall Time (s)"], width, label="Wall-Clock Time (s)", color="#10b981")
axes[0].set_title("Execution Times (Seconds)", fontweight="bold")
axes[0].set_xticks(x)
axes[0].set_xticklabels(labels)
axes[0].legend()
axes[0].grid(axis="y", linestyle="--", alpha=0.5)
for bar in list(b1) + list(b2):
    axes[0].annotate(f"{bar.get_height():.0f}s", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                     ha="center", va="bottom", fontsize=9)

ax2_twin = axes[1].twinx()
b3 = axes[1].bar(x - width/2, df_sum["Shuffle Write (MB)"], width, label="Shuffle Write (MB)", color="#f59e0b")
b4 = ax2_twin.bar(x + width/2, df_sum["Shuffle Recs (M)"], width, label="Shuffle Records (M)", color="#ef4444")
axes[1].set_title("Shuffle Volume (MB and Million Records)", fontweight="bold")
axes[1].set_xticks(x)
axes[1].set_xticklabels(labels)
axes[1].set_ylabel("MB")
ax2_twin.set_ylabel("Million Records")
axes[1].grid(axis="y", linestyle="--", alpha=0.5)
l1, lab1 = axes[1].get_legend_handles_labels()
l2, lab2 = ax2_twin.get_legend_handles_labels()
axes[1].legend(l1 + l2, lab1 + lab2, loc="upper right")
for bar in b3:
    axes[1].annotate(f"{bar.get_height():.1f}MB", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                     ha="center", va="bottom", fontsize=9)
for bar in b4:
    ax2_twin.annotate(f"{bar.get_height():.1f}M", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                      ha="center", va="bottom", fontsize=9)

ax3_twin = axes[2].twinx()
b5 = axes[2].bar(x - width/2, df_sum["S3 Read (MB)"] / 1024.0, width, label="Data Read from S3 (GB)", color="#6366f1")
b6 = ax3_twin.bar(x + width/2, df_sum["Tasks"], width, label="Total Tasks", color="#8b5cf6")
axes[2].set_title("S3 I/O (GB) and Task Count", fontweight="bold")
axes[2].set_xticks(x)
axes[2].set_xticklabels(labels)
axes[2].set_ylabel("GB Read from S3")
ax3_twin.set_ylabel("Total Tasks")
axes[2].grid(axis="y", linestyle="--", alpha=0.5)
l3, lab3 = axes[2].get_legend_handles_labels()
l4, lab4 = ax3_twin.get_legend_handles_labels()
axes[2].legend(l3 + l4, lab3 + lab4, loc="upper right")
for bar in b5:
    axes[2].annotate(f"{bar.get_height():.2f}GB", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                     ha="center", va="bottom", fontsize=9)
for bar in b6:
    ax3_twin.annotate(f"{int(bar.get_height())}", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                      ha="center", va="bottom", fontsize=9)

plt.tight_layout()
out_chart = OUTPUT_DIR / "benchmark_history.png"
plt.savefig(out_chart)
print(f"\n[OK] Benchmark chart saved to: {out_chart}")