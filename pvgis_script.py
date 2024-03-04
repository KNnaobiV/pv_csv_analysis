"""For handling data downloaded from pvgis"""
import os
import logging
from pathlib import Path

import pandas as pd
import numpy as np
import flat_table
from matplotlib import pyplot as plt
from process_json import get_lon_lat_from_filename


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


PWD = os.path.dirname(__file__)

if not os.path.join(PWD, "pvgis_data"):
    os.mkdir(os.path.join(PWD, "pvgis_data"))

CSV_DIR = os.path.join(PWD, "pvgis_data")

NEW_DF_COLUMNS = {
	"SD_m": "Monthly Avg Standard Deviation", 
	"H(i)_m": "Avg Monthly Sum Of Global Irradiation", 
	"H(i)_d": "Avg Daily Sum Of Global Irradiation", 
	"E_m": "Avg Monthly Energy Production", 
	"E_d": "Avg Daily Energy Production", 
	"outputs.vertical_axis.month": "Month"
	}


def main():
	processed_list = open_csv_files()
	for processed_file in processed_list:
		df_dropped = process_df(processed_file[0])
		pv_info = get_pv_info(processed_file[0])[1]
		renamed_df = rename_columns(df_dropped).astype("float32")

		filename = processed_file[2].split(".")[0]
		
		if not os.path.exists(os.path.join(PWD, "output_dir")):
			os.mkdir(os.path.join(PWD, "output_dir"))
		
		if not os.path.exists(os.path.join(PWD, "output_dir", f"{filename}")):
			os.mkdir(os.path.join(PWD, "output_dir", f"{filename}"))

		if not os.path.exists(os.path.join(PWD, "output_dir", f"{filename}", "pvgis")):
			os.mkdir(os.path.join(PWD, "output_dir", f"{filename}", "pvgis"))

		
		csv_file = os.path.join(
			PWD, "output_dir", f"{filename}", "pvgis",  f"{filename}.csv"
			)
		
		if os.path.exists(os.path.join(PWD, "output_dir", f"{filename}", "info.txt")):
			os.remove(os.path.join(PWD, "output_dir", f"{filename}", "info.txt"))
		
		txt_file = os.path.join(PWD, "output_dir", f"{filename}", "info.txt")
		with open(txt_file, "w") as text_file:
			text_file.write("\n".join(pv_info))
			text_file.close()
			
		renamed_df.to_csv((csv_file))
		plot(renamed_df, processed_file)
		
		logging.info(f"file {csv_file} saved to "
		f"{os.path.join(PWD, 'csv_files',)}")
	logging.info("done")



def get_csv_files():
	"""
	Gets all csv files in the csv directory in the present working 
	directory.

	Returns
	-------
	csv_files: list
		List of all csv files in the csv directory of the current 
		working directory
	"""
	csv_files = []

	for file in os.listdir(CSV_DIR):
		if file.endswith(".csv"):
			csv_files.append(file)
	return csv_files


def get_pv_info(df):
	"""Gets information on the pv system"""
	lon = df[1][0]
	
	lat = df[1][1]

	power = df[1][3]

	losses = df[1][4]

	angle = df[1][5]
	return df, (lon, lat, power, losses, angle)


def open_csv_files():
	"""
	Takes csv files in the current directory and opens them as a Data Frame

	Returns
	-------
	df: pandas.DataFrame

	df_columns: list
		list of the data frame columns as string
	"""
	csv_files = get_csv_files()
	processed_list = []

	for filename in csv_files:
		file = os.path.join(PWD, "pvgis_data", filename)
		df = pd.read_csv(file, delimiter="\t", names=list(range(11)))
		df_columns = list(df)

		processed_list.append((df, df_columns, filename))
	return processed_list


def process_df(df):
    """
    Process df by dropping NaNs, input information and meta data and 
    changes the header.

    Parameters
    ----------
    df: pandas.DataFrame

    Returns
    -------
    df: pandas.DataFrame
        Processed data frame
    """
    df = df.iloc[8:21]

    header = df.iloc[0]
    df = df[1:]
    
    df.columns = header
    df.reset_index(drop=True)
    df = df.dropna(axis=1, how="all")
	
    return df


def rename_columns(df):
	df.rename(columns=NEW_DF_COLUMNS, inplace=True)
	return df


def plot(df, processed_file):
	"""
	Plots 'Average Monthly Energy Production' VS 'Month' and saves plot 
	as '*.png' file.

	Parameters
	----------
	df: pandas.DataFrame 
	"""
	filename = processed_file[2].split(".")[0]

	if len(processed_file[2].split("_")) == 5:
		filename = get_lon_lat_from_filename(processed_file[2])
	
	try:
		if not os.path.exists(
		os.path.join(PWD,  "output_dir", f"{str(filename)}", "pvgis")):
			os.mkdir(
				(os.path.join(PWD,  "output_dir", f"{str(filename)}", "pvgis")
				))
			
	except Exception as err:
		print(f"{err} occured")

	label = ["Month", "Avg Monthly Energy Production"]

	figure_title = f"{plt.xlabel} VS {plt.ylabel}"
	fig = plt.figure(figure_title)

	fig.set_size_inches(13.6, 7.06)	
	plot_no = df.shape

	color="orange"
	plt.xlabel(label[0])
	plt.ylabel(label[1] + ("KWh"))
	plt.grid()
	plt.margins(x=0)
	plt.ticklabel_format(useOffset=False, axis="y", style="plain")

	plt.bar(df[label[0]], df["Avg Monthly Energy Production"], label=label)
	plt.legend()

	bar_plot_name = os.path.join(
		PWD,  "output_dir", f"{str(filename)}", "pvgis", f"{filename}_bar.png"
		)
	plt.savefig(bar_plot_name)
	plt.close()

	figure_title = f"{plt.xlabel} VS {plt.ylabel}"
	fig = plt.figure(figure_title)

	fig.set_size_inches(13.6, 7.06)	
	plot_no = df.shape
	
	color="orange"
	plt.xlabel(label[0])
	plt.ylabel(label[1] + ("KWh"))
	plt.grid()
	plt.margins(x=0)
	plt.ticklabel_format(useOffset=False, axis="y", style="plain")
	plt.plot(
		df[label[0]], df[label[1]], color=color, linewidth=1, label=label
		)
	plt.legend()

	line_plot_name = os.path.join(
		PWD,  "output_dir", f"{str(filename)}", "pvgis", f"{filename}_line.png"
		)
	plt.savefig(line_plot_name)
	plt.close()


if __name__=="__main__":
	main()