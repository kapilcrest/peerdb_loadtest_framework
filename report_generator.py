import pandas as pd
import matplotlib.pyplot as plt

def generate_report(csv_path="metrics_log.csv"):
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # CDC lag plot
    lag_df = df[df["metric"] == "max_cdc_lag_sec"]
    if lag_df.empty:
        print("⚠️ No CDC lag data found.")
        return

    print(f"✅ Found {len(lag_df)} CDC lag datapoints")

    for schema in lag_df["schema"].unique():
        subset = lag_df[lag_df["schema"] == schema]
        plt.plot(subset["timestamp"], subset["value"], label=schema)
        print(f"📊 {schema} lag: min={subset['value'].min()}s, max={subset['value'].max()}s, avg={subset['value'].mean():.2f}s")

    plt.legend()
    plt.title("CDC Lag Over Time")
    plt.xlabel("Time")
    plt.ylabel("Lag (s)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("cdc_lag_report.png")
    print("✅ Report generated: cdc_lag_report.png")