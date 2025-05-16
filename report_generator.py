import pandas as pd
import matplotlib.pyplot as plt

def generate_report(csv_path="metrics_log.csv"):
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # CDC lag plot
    lag_df = df[df["metric"] == "cdc_lag_sec"]
    if not lag_df.empty:
        for schema in lag_df["schema"].unique():
            subset = lag_df[lag_df["schema"] == schema]
            plt.plot(subset["timestamp"], subset["value"], label=schema)
        plt.legend()
        plt.title("CDC Lag Over Time")
        plt.xlabel("Time")
        plt.ylabel("Lag (s)")
        plt.savefig("cdc_lag_report.png")

    print("✅ Report and plot generated: cdc_lag_report.png")