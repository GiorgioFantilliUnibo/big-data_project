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

JOBS = {
    "Job 1": "history-remote-ethereumJob1",
    "Job 2": "history-remote-ethereumJob2",
    "Job 3": "history-remote-ethereumJob3",
}


rows = []
for job, file_name in JOBS.items():
    start, end = None, None
    tasks, run_ms, s3_bytes, sw_bytes, sw_recs = 0, 0, 0, 0, 0

    with open(HISTORY_DIR / file_name, encoding="utf-8") as f:
        for line in f:
            ev = json.loads(line)
            event = ev["Event"]

            if event == "SparkListenerJobStart" and start is None:
                start = ev["Submission Time"]
            elif event == "SparkListenerJobEnd":
                end = ev["Completion Time"]
            elif event == "SparkListenerTaskEnd" and "Task Metrics" in ev:
                m = ev["Task Metrics"]
                sr = m["Shuffle Read Metrics"]
                tasks += 1
                run_ms += m["Executor Run Time"]
                sw_bytes += m["Shuffle Write Metrics"]["Shuffle Bytes Written"]
                sw_recs += m["Shuffle Write Metrics"]["Shuffle Records Written"]
                if sr["Remote Bytes Read"] + sr["Local Bytes Read"] == 0:
                    s3_bytes += m["Input Metrics"]["Bytes Read"]

    rows.append({
        "Job": job,
        "Wall Time (s)": (end - start) / 1000,
        "Executor Time (s)": run_ms / 1000,
        "S3 Read (GB)": s3_bytes / 1024**3,
        "Shuffle Write (MB)": sw_bytes / 1024**2,
        "Shuffle Records (M)": sw_recs / 1e6,
        "Tasks": tasks,
    })

df = pd.DataFrame(rows).set_index("Job")
print(df.round(2).to_string())


labels = ["Job 1\n(Unoptimized)", "Job 2\n(Pre-Agg)", "Job 3\n(Broadcast)"]
x = np.arange(len(labels))
width = 0.35

fig, axes = plt.subplots(1, 3, figsize=(15, 4.8), dpi=120)

b1 = axes[0].bar(x - width/2, df["Executor Time (s)"], width, label="Executor Run Time (s)", color="#3b82f6")
b2 = axes[0].bar(x + width/2, df["Wall Time (s)"], width, label="Wall-Clock Time (s)", color="#10b981")
axes[0].set_title("Execution Times (Seconds)", fontweight="bold")
axes[0].set_xticks(x)
axes[0].set_xticklabels(labels)
axes[0].legend()
axes[0].grid(axis="y", linestyle="--", alpha=0.5)
for bar in list(b1) + list(b2):
    axes[0].annotate(f"{bar.get_height():.0f}s", (bar.get_x() + bar.get_width()/2, bar.get_height()),
                     ha="center", va="bottom", fontsize=9)

ax2_twin = axes[1].twinx()
b3 = axes[1].bar(x - width/2, df["Shuffle Write (MB)"], width, label="Shuffle Write (MB)", color="#f59e0b")
b4 = ax2_twin.bar(x + width/2, df["Shuffle Records (M)"], width, label="Shuffle Records (M)", color="#ef4444")
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
b5 = axes[2].bar(x - width/2, df["S3 Read (GB)"], width, label="Data Read from S3 (GB)", color="#6366f1")
b6 = ax3_twin.bar(x + width/2, df["Tasks"], width, label="Total Tasks", color="#8b5cf6")
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