import os
from pathlib import Path
import logging
from math import floor

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from .process_json import main as json_main
from .pvgis_script import main as pvgis_main
from .pvoutput_script import pvoutput_main
from .statistics import calculate_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

PARENT_FOLDER = os.path.join(os.path.dirname(__file__), "output_dir")


def main():
    json_main()
    pvoutput_main()
    pvgis_main()
    error_list = []
    df_list = join_dfs()
    for item in df_list:
        plot(item[0], item[1], item[2], item[3], item[4]) 
        error_list.append(item[4])
    mean_error = sum(error_list) / len(error_list)
    sorted_error = sorted(error_list)
    median_error = sorted_error[(floor(len(error_list) / 2))]

    print(sorted_error)
    print("Mean error", mean_error, len(error_list))
    print("Median Error", median_error)
    calculate_stats()


def get_folder_names():
    """Returns all folder names in output directory"""
    folder_list = []
    
    for folder in os.listdir(PARENT_FOLDER):
        folder_list.append(folder)
    
    return folder_list

def truncate(num):
    return round(num, 2)

def join_dfs():
    """
    Creates a csv file with generated power from processed PVGIS and 
    PVOUTPUT files as columns, using month as index
    """
    folder_list = get_folder_names()

    df_list = []
    for folder in folder_list:
        pvoutput_file = os.path.join(
            PARENT_FOLDER, folder, "pvoutput", f"{folder}.csv"
            )
        logging.info(pvoutput_file)
        pvoutput_df = pd.read_csv(pvoutput_file)

        pvgis_file = os.path.join(
            PARENT_FOLDER, folder, "pvgis", f"{folder}.csv"
            )
        pvgis_df = pd.read_csv(pvgis_file)

        month = pd.Series(pvgis_df["Month"],)

        pvgis_generated = pd.Series(pvgis_df["Avg Monthly Energy Production"],)

        pvoutput_generated = pd.Series(pvoutput_df["Generated (KWh)"],)

        frame = {
            "Month": month, "PVGIS Generated": pvgis_generated, 
            "PVOUTPUT Generated": pvoutput_generated
            }
        joined_df = pd.DataFrame(frame)
        joined_df["Error"] = (
            (joined_df["PVOUTPUT Generated"] - joined_df["PVGIS Generated"]) 
            / joined_df["PVOUTPUT Generated"]
            )

        
        joined_df.T.round(2).to_csv(
            os.path.join(PARENT_FOLDER, folder, (f"joined_{folder}.csv")),
        )
        std = joined_df.std().astype("float32")
        pvgis_std = std[1]
        pvoutput_std = std[2]
        standard_deviation = pvgis_std, pvoutput_std

        mean = joined_df.mean().astype("float32")
        pvgis_mean = mean[1]
        pvoutput_mean = mean[2]
        p_error = round(abs((pvoutput_mean - pvgis_mean) / pvoutput_mean), 3)
        mean = pvgis_mean, pvoutput_mean

        df_list.append((joined_df, folder, standard_deviation, mean, p_error))
    return df_list


def plot(df, folder, std, mean, p_error):
    """
    Plots 'Generated' in KWh VS 'Month' and saves plot 
	as '*.png' file.
    Adds information on the pv system on the plots.

	Parameters
	----------
	df: pandas.DataFrame 
    folder: str
        folder containing df to be plotted
    """
    txt_file = os.path.join(PARENT_FOLDER, folder, "info.txt")

    with open (txt_file, "r") as txt_file:
        pv_info = (txt_file.readlines())
        
        lon = str(pv_info[0].split("\\")[0]) + "N"
        lat = str(pv_info[1].split("\\")[0]) + "E"

        if lon.startswith("-"):
            lon = lon[:-1] + "S"
        
        if lat.startswith("-"):
            lat = lat[:-1] + "W"
        
        power =  pv_info[2].split("\\")[0]
        losses = pv_info[3].split("\\")[0]
        angle = pv_info[4].split("\\")[0]

    fig_text = (f"PVGIS Mean:{round(float(mean[0]),3)}kWh\nPVOUTPUT Mean:"
        f"{round(float(mean[1]),3)}KWh\n"
        f"Mean Error:{round(float(p_error),3)}\n"
        f"PVGIS STD:{round(float(std[0]),3)}kWh\nPVOUTPUT STD:"
        f"{round(float(std[1]),3)}KWh\nLocation:({lon},{lat})\n"
        f"System Power:{power}KW\nInclination Angle(deg):{angle}")

    color = "red"
    df_columns = list(df)
    label = [
        "Month", "PVGIS Generated", "PVOUTPUT Generated"
        ]
    filename = f"{folder}_joined"

    figure_title = f"{folder} Month VS Generated"
    fig = plt.figure(figure_title)

    fig.set_size_inches(13.6, 7.06)
    plot_no = df.shape

    months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
        "Aug", "Sept", "Oct", "Nov", "Dec",
        ]
    x_axis = np.arange(len(months))
    
    color="blue"
    plt.xticks(x_axis, months)
    plt.xlabel("Month")
    plt.ylabel("Generated (KWh)")
    plt.grid()
    plt.margins(x=0)
    plt.ticklabel_format(useOffset=False, axis="y", style="plain")

    width=0.3
    plt.bar(
        x=df[label[0]] - width/2, height=df[label[1]], 
        width=width, align='center', label='PVGIS Generated'
        )
    plt.bar(
        x=df[label[0]] + width/2, height=df[label[2]],
        width=width, align='center', label='PVOUTPUT Generated'
        )
    plt.legend()
    plt.figtext(
        .15, .7, fig_text, backgroundcolor="white", fontfamily="monospace",
        fontsize="x-small",
        )

    bar_plot_name = os.path.join(
        PARENT_FOLDER, folder, f"{filename}_bar.png"
        )
    plt.savefig(bar_plot_name)
    #plt.show()
    plt.close()

    figure_title = f"{folder} Month VS Generated"
    fig = plt.figure(figure_title)
    fig.set_size_inches(13.6, 7.06)	
    plot_no = df.shape
    color="blue"

    plt.xticks(x_axis, months)
    plt.xlabel("Month")
    plt.ylabel("Energy Generated (KWh)")
    plt.grid()
    plt.margins(x=0)
    plt.ticklabel_format(useOffset=False, axis="y", style="plain")

    plt.plot(
        df[label[0]], df[label[1]], color=color, linewidth=1, 
        label="PVGIS Generated"
    )
    plt.plot(
    df[label[0]], df[label[2]], color="orange", linewidth=1, 
    label="PVOUTPUT Generated"
    )
    plt.legend()
    plt.figtext(
        .15, .7, fig_text, backgroundcolor="white", fontfamily="monospace",
        fontsize="x-small",
        )

    line_plot_name = bar_plot_name = os.path.join(
        PARENT_FOLDER, folder, f"{filename}_line.png"
        )
    plt.savefig(line_plot_name)
    #plt.show()
    plt.close()

if __name__=="__main__":
    main()