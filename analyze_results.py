from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


BASE_DIR = Path(__file__).resolve().parent
if not (BASE_DIR / "output").exists():
    BASE_DIR = BASE_DIR.parents[2]

OUTPUT_DIR = BASE_DIR / "output"


def load_csv_target(target: Path) -> pd.DataFrame:
    if target.is_file():
        return pd.read_csv(target)
    part_files = sorted(target.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No CSV file found in {target}")
    return pd.concat([pd.read_csv(f) for f in part_files], ignore_index=True)


rem1 = load_csv_target(OUTPUT_DIR / "output-remote-ethereumJob1.csv")
rem2 = load_csv_target(OUTPUT_DIR / "output-remote-ethereumJob2.csv")
rem3 = load_csv_target(OUTPUT_DIR / "output-remote-ethereumJob3.csv")

pd.testing.assert_frame_equal(rem1, rem2, check_like=True)
pd.testing.assert_frame_equal(rem1, rem3, check_like=True)
print(f"[OK] REMOTE outputs (Job 1, 2, 3) verified: identical ({len(rem1)} rows).")


df = rem1.copy()
df["Date"] = pd.to_datetime(df["Date"])
df["WeightedGasGwei"] = (df["TxCount"] * df["AvgGasPrice"]) / 1e9

print("\n=== SUMMARY BY SENDER ACTIVITY TIER ===")
tier_summary = df.groupby("Tier").agg(
    TotalTx=("TxCount", "sum"),
    TotalValueEth=("TotalValueGwei", lambda s: round(s.sum() / 1e9, 2)),
    WeightedAvgGasGwei=("WeightedGasGwei", "sum"),
)
tier_summary["WeightedAvgGasGwei"] = (tier_summary["WeightedAvgGasGwei"] / tier_summary["TotalTx"]).round(2)
print(tier_summary.to_string())


fig, axes = plt.subplots(1, 3, figsize=(16, 4.8), dpi=120)

tier_pivot = df.groupby(["Tier", "IsContractCall"])["TxCount"].sum().unstack(fill_value=0)
tier_pivot.columns = ["Standard Transfer", "Smart Contract Call"]
tier_pivot.plot(kind="bar", stacked=True, ax=axes[0], color=["#3b82f6", "#f59e0b"])
axes[0].set_title("Transaction Volume by Tier and Type", fontweight="bold")
axes[0].set_xlabel("Sender Tier (1: <=1, 2: <=10, 3: <=100, 4: >100 tx)")
axes[0].set_ylabel("Number of Transactions")
axes[0].tick_params(axis="x", rotation=0)
axes[0].grid(axis="y", linestyle="--", alpha=0.5)

daily_tx = df.groupby(["Date", "IsContractCall"])["TxCount"].sum().unstack(fill_value=0)
daily_tx.columns = ["Standard Transfer", "Smart Contract Call"]
daily_tx.plot(ax=axes[1], linewidth=2, color=["#3b82f6", "#ef4444"])
axes[1].set_title("Daily Transaction Trends", fontweight="bold")
axes[1].set_xlabel("Date")
axes[1].set_ylabel("Transactions / Day")
axes[1].grid(True, linestyle="--", alpha=0.5)

grouped_gas = df.groupby(["Tier", "IsContractCall"])[["WeightedGasGwei", "TxCount"]].sum()
gas_pivot = (grouped_gas["WeightedGasGwei"] / grouped_gas["TxCount"]).unstack(fill_value=0)
gas_pivot.columns = ["Standard Transfer", "Smart Contract Call"]
gas_pivot.plot(kind="bar", ax=axes[2], color=["#10b981", "#8b5cf6"])
axes[2].set_title("Weighted Average Gas Price (Gwei)", fontweight="bold")
axes[2].set_xlabel("Sender Tier")
axes[2].set_ylabel("Avg Gas Price (Gwei)")
axes[2].tick_params(axis="x", rotation=0)
axes[2].grid(axis="y", linestyle="--", alpha=0.5)

plt.tight_layout()
out_chart = OUTPUT_DIR / "ethereum_analytics.png"
plt.savefig(out_chart)
print(f"\n[OK] Analytics chart saved to: {out_chart}")