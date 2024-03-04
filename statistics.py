import os

import pandas as pd
import numpy as np

def main(dir_):
    found_files = get_csv_files(dir_)
    for csv_file in found_files:
        try:
            calculate_stats(csv_file)
        except ValueError:
            raise


def get_csv_files(dir):
    """
    Recursively walks a dir and returns a list of all the csv files 
    in all children directiories.
    """
    found_files = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".csv") and file.startswith("joined"):
                found_files.append(os.path.join(root, file))
        else:
            for dir in dirs:
                dir = os.path.join(root, dir)
                get_csv_files(dir)
    return found_files


def calculate_stats(csv_file):
    df = pd.read_csv(csv_file, index_col=0)
    df = df.T
    df.columns = ["month", "predicted", "actual", "Error"]

    mean_actual = df["actual"].mean()
    mean_predicted = df["predicted"].mean()
    std_actual = df['actual'].std()
    std_predicted = df['predicted'].std()
    corr_coefficient = df['actual'].corr(df['predicted'])
    mae = np.mean(np.abs(df['predicted'] - df['actual']))
    smape = 100 / len(df) * np.sum(2 * np.abs(df['predicted'] - df['actual']) / (np.abs(df['predicted']) + np.abs(df['actual'])))
    mape = np.mean(np.abs((df['actual'] - df['predicted']) / df['actual'])) * 100
    mase = mae / np.mean(np.abs(np.diff(df['actual'])))
    rmse = np.sqrt(np.mean((df['predicted'] - df['actual'])**2))
    statistics_df = pd.DataFrame({
        'Statistics': 'Value',
        'Mean Actual': mean_actual,
        'Mean Predicted': mean_predicted,
        'MAE': mae,
        'RMSE': rmse,
        'SMAPE': smape,
        'MAPE': mape,
        'MASE': mase,
        'Pearson Correlation Coefficient': corr_coefficient, 
        'rrmse' : rmse/mean_actual,
        'cv actual': std_actual/mean_actual,
        'cv predicted': std_predicted/mean_predicted},
        index=[0]
    )
    file_location = "\\".join(csv_file.split(".")[0].split("\\")[:-1])
    statistics_df.to_csv(os.path.join(file_location, "stats.csv"))


if __name__ == "__main__":
    dir_ = os.path.join(os.path.dirname(__file__), "output_dir")
    main(dir_)