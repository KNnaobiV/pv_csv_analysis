"""Process csv data downloaded from PVOUTPUT"""
import os
import re
import glob
import logging
import datetime

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from process_json import get_lon_lat_from_filename

PWD = os.path.dirname(__file__)

CSV_DIR = os.path.join(PWD, "csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

def main():
	processed_list = process_csv_files()
	for processed_file in processed_list:

		df_to_string = to_str(processed_file[0])
		df_to_float = str_to_float(df_to_string)

		filename = processed_file[2].split(".")[0]

		if not os.path.exists(os.path.join(PWD, "output_dir")):
			os.mkdir(os.path.join(PWD, "output_dir"))
		
		if not os.path.exists(os.path.join(PWD, "output_dir", f"{filename}")):
			os.mkdir(os.path.join(PWD, "output_dir", f"{filename}"))
		
		if not os.path.exists(os.path.join(PWD, "output_dir", f"{filename}", "pvoutput")):
			os.mkdir(os.path.join(PWD, "output_dir", f"{filename}", "pvoutput"))


		csv_file = os.path.join(
			PWD, "output_dir", f"{filename}", "pvoutput",  f"{filename}.csv"
			)
		df_to_float = df_to_float[::-1]
		df_to_float.to_csv(csv_file)
		plot(df_to_float, processed_file)
		
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


def process_csv_files():
	"""
	Takes csv files in the current directory and converts them to a
	dataframe.

	Returns
	-------
	df: pandas.DataFrame

	df_columns: list
		list of the data frame columns as string
	"""
	csv_files = get_csv_files()
	processed_list = []

	for filename in csv_files:
		file = os.path.join(PWD, "csv", filename)
		df = pd.read_csv(file)
		df_columns = list(df)

		processed_list.append((df, df_columns, filename))
	return processed_list


def to_str(df):
	"""
	Converts dtype of data frame columns to string.

	Parameters
	----------
	df: pandas.DataFrame

	Returns
	-------
	df. pandas.DataFrame
		Pandas dataframe with column as string.
	"""
	columns = list(df)

	for column in columns:
		df[column] = df[column].astype("string")
	df = df.drop(0)
	return df


def str_to_float(df):
	"""
	Converts df values of df items from string to floats

	Parameters
	----------
	df: pandas.DataFrame

	Returns
	-------
	df: pandas.DataFrame
		Pandas dataframe with column as string.
	"""
	columns = ["Generated", 'Efficiency', 'Low', 'High', 'Average']
	for column in columns:
		df_list = [column, ]

		for value in df[column]:

			if type(value) == "NA": pass

			else: value = strip_string(value)


			if value.lower().endswith("mwh"):
				value = pd.to_numeric(value[:-3],).astype("float32")
				value = value * 1000

				df_list[0] = ((df_list[0].split(" "))[0] + " (KWh)")
				df_list.append(value)


			elif value.lower().endswith("kwh"):
				value = pd.to_numeric(value[:-3],).astype("float32")

				df_list[0] = ((df_list[0].split(" "))[0] + " (KWh)")
				df_list.append(value)

			elif value.lower().endswith("kwh/kw"):
				value = pd.to_numeric(value[:-6],).astype("float32")
				value = value * 1000

				df_list[0] = ((df_list[0].split(" "))[0] + " (KWh/KW)")
				df_list.append(value)


		df[column] = pd.Series(df_list,)
		df.rename(columns={column: df_list[0]}, inplace=True)
	return df


def strip_string(string):
	"""
	Strip strings of commas and white space

	Parameters
	----------
	string: str

	Returns
	-------
	string: str
		String with commas and white space stripped.
	"""
	try:
		string_list = string.split(",")
		string = string_list[0]

		if len(string_list) > 1:
			new_string = ""

			for i in range(len(string_list)):
				new_string += string_list[i].replace(" ", "")

			string = new_string
		return string

	except Exception as err:
		print(err)


def plot(df, processed_file):
	"""
	Plots 'Generated' in KWh VS 'Month' and saves plot 
	as '*.png' file.

	Parameters
	----------
	df: pandas.DataFrame 
	"""
	filename = processed_file[2].split(".")[0]
	
	try:
		if not os.path.exists(
		os.path.join(PWD,  "output_dir", f"{str(filename)}", "pvoutput")):
			os.mkdir(
				(os.path.join(PWD,  "output_dir", f"{str(filename)}", "pvoutput")
				))
			
	except Exception as err:

		print(f"{err} occured")

	color = "red"
	df_columns = list(df)
	label = [df_columns[0], df_columns[1]]

	figure_title = f"{plt.xlabel} VS {plt.ylabel}"
	fig = plt.figure(figure_title)

	fig.set_size_inches(13.6, 7.06)	
	plot_no = df.shape

	color="blue"
	plt.xlabel(label[0])
	plt.ylabel(label[1])
	plt.grid()
	plt.margins(x=0)
	plt.ticklabel_format(useOffset=False, axis="y", style="plain")

	plt.bar(df[label[0]], df[label[1]], label=label)
	plt.legend()

	bar_plot_name = os.path.join(
		PWD,  "output_dir", f"{str(filename)}", "pvoutput", f"{filename}_bar.png"
		)
	plt.savefig(bar_plot_name)
	plt.close()

	figure_title = f"{plt.xlabel} VS {plt.ylabel}"
	fig = plt.figure(figure_title)
	fig.set_size_inches(13.6, 7.06)	
	plot_no = df.shape
	color="blue"
	plt.xlabel(label[0])
	plt.ylabel(label[1] )
	plt.grid()
	plt.margins(x=0)
	plt.ticklabel_format(useOffset=False, axis="y", style="plain")
	plt.plot(
		df[label[0]], df[label[1]], color=color, linewidth=1, label=label
		)
	plt.legend()

	line_plot_name = os.path.join(
		PWD,  "output_dir", f"{str(filename)}", "pvoutput", f"{filename}_line.png"
		)
	plt.savefig(line_plot_name)
	plt.close()


if __name__=="__main__":
	main()


