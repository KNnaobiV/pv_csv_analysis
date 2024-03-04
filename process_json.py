import pandas as pd
import numpy as np
import os
import flat_table
import json
import re
import glob
import logging
import datetime
import matplotlib.pyplot as plt

PWD = os.path.dirname(__file__)
JSON_DIR = os.path.join(PWD, "json")

DF_COLUMN_NAMES = [
		"outputs.vertical_axis.SD_m_y", "outputs.vertical_axis.H(i)_m_y", 
		"outputs.vertical_axis.H(i)_d_y", "outputs.vertical_axis.E_m_y",
		"outputs.vertical_axis.E_d_y", "outputs.vertical_axis.month"
		]

NEW_DF_COLUMNS = {
	"outputs.vertical_axis.SD_m_y": "Monthly Avg Standard Deviation", 
	"outputs.vertical_axis.H(i)_m_y": "Avg Monthly Sum Of Global Irradiation", 
	"outputs.vertical_axis.H(i)_d_y": "Avg Daily Sum Of Global Irradiation", 
	"outputs.vertical_axis.E_m_y": "Avg Monthly Energy Production", 
	"outputs.vertical_axis.E_d_y": "Avg Daily Energy Production", 
	"outputs.vertical_axis.month": "Month"
	}


def get_json_files():
	"""
	Gets all json files in the json directory in the present working 
	directory.
	
	Returns
	-------
	json_files: list
		List of all json files in the json directory of the current 
		working directory
	"""
	json_files = []
	for file in os.listdir(JSON_DIR):
		if file.endswith(".json"):
			json_files.append(file)
	return json_files


def process_json_files():
	"""
	Takes json files in the current directory and converts them to a
	dataframe.

	Returns
	-------
	df: pandas.DataFrame

	df_columns: list
		list of the data frame columns as string
	"""
	json_files = get_json_files()
	processed_list = []
	for filename in json_files:
		file = os.path.join(PWD, "json", filename)
		with open(file) as file:
			file = json.load(file)

		df = pd.DataFrame(file)
		df = flat_table.normalize(df)
		df_columns = list(df)
		processed_list.append((df, df_columns, filename))
		
	return processed_list


def aggregate_monthly_data(df, year):
	"""
	Calculates monthly average of all the columns in the dataframe and 
	adds a "Period" column which is a columns for the number of the 
	months in a year.

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame to be aggregated

	year: Year()
		Python object which contains information about a year.
	
	Returns
	-------
	aggregate_df: pandas.DataFrame
		DataFrame with aggregate data calculated
	"""
	days = year.days
	for month in year.months:
		period = year.months[month]
		aggregate_df(df, period)
	return aggregate_df


def add_periods_to_df(year, df):
	"""
	Adds "Periods" (weeks in a year) columns to the data frame

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame for which the periods are to be calculated

	year: Year()
		Python object which contains information about a year.
	
	Returns
	-------
	aggregate_df: pandas.DataFrame
		DataFrame with aggregate data calculated
	"""
	period_list = Year.get_weeks_in_year(year)


def drop_meta(df):
	"""
	Drops all columns that contain meta data

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame whose meta data would be dropped
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with all meta data dropped
	"""
	drop_list = []
	for column_name in df_columns:
		if "meta" in i:
			drop_list.append(column_name)
	df.drop(drop_list, axis=1, inplace=True)
	return df


def drop_input(df):
	"""
	Drops all columns that contain input data

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame whose input data would be dropped
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with all input data dropped
	"""
	drop_list = []
	for column_name in df_columns:
		dont_drop = [
			'inputs.peak_power', 'inputs.technology', 'inputs.longitude'
			]
		if (column_name not in dont_drop) and ("input" in column_name):
			drop_list.append(i)
	df.drop(drop_list, axis=1, inplace=True)
	return df


def drop_output_time(df):
	"""
	Drops column containing output time.

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame whose output time data would be dropped
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with output time data dropped
	"""
	df.drop("outputs.time", axis=1, inplace=True)
	return df


def drop_dummy_columns(df):
	"""
	Drops all columns containing data not needed for calculations, 
	converts all data to float64 and sorts data frame by month values.

	Parameters
	----------
	df: pandas.DataFrame
		DataFrame whose 'dummy' data would be dropped
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with all 'dummy' data dropped
	"""

	column_drop_list = []
	df_columns = list(df)

	for column_name in df_columns:
		if column_name not in DF_COLUMN_NAMES:
			column_drop_list.append(column_name)
	df.drop(column_drop_list, axis=1, inplace=True)

	for column in DF_COLUMN_NAMES:
		df[column] = pd.to_numeric(df[column])
	
	df = df.sort_values("outputs.vertical_axis.month")
			
	return df


def add_month(df):
	"""
	Changes month values from float to int

	Parameters
	----------
	df: pandas.DataFrame
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with month values changed to int
	"""
	month = pd.Series([1, 12, 11, 9, 10, 7, 6, 5, 4, 3, 2, 8])
	df["months"] = month
	return df


def drop_empty_cells(df):
	"""
	Drops all cells that contain empty string or NaN

	Parameters
	----------
	df: pandas.DataFrame
	
	Returns
	-------
	df: pandas.DataFrame
		DataFrame with all rows with NaN dropped
	"""
	df_columns = list(df)
	for name in df_columns:
		df[name].replace("", np.nan, inplace=True)
		df.dropna(subset=[name], inplace=True)
	return df
  

def aggregate_df(df, period=168):
	"""
	Calculates average data for a set number of rows.

	Parameters
	---------
	df: pandas.DataFrame

	period: int
		period for which the data should be calculated, defaults to 168 
		for converting hourly data to weekly data
	"""
	df = df.groupby(np.arange(len(df))//period).mean()
	df = df[::-1]
	return df


def rename_columns(df):
	df.rename(columns=NEW_DF_COLUMNS, inplace=True)
	return df


def get_lon_lat_from_filename(processed_file):
	"""
	Get longitude and latitude information of the currently processed
	file and return corresponding filename.
	
	Parameters
	----------
	processed_file: str
		name of the file without preceeding path information
	
	Returns
	-------
	filename: str
		file name gotten from the longitude and latitude information of 
		the currently processed file
	"""
	filename = ""
	try:
		if len(processed_file[2].split("_")) == 5:
			lon = processed_file[2].split("_")[1]
			lat = processed_file[2].split("_")[2]
			filename = f"lon{lon}_lat{lat}"
			return filename

	except Exception as err:
		print(f"{err} occured.")
		return None


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

	plt.bar(df[label[0]], df["Avg Monthly Energy Production"])
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


def main():
	processed_list = process_json_files()
	for processed_file in processed_list:
		df_dropped = drop_dummy_columns(processed_file[0])
		renamed_df = rename_columns(df_dropped)

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

		drop_empty_cells(renamed_df).to_csv((csv_file))
		plot(drop_empty_cells(renamed_df), processed_file)


if __name__=="__main__":
	main()


class Year():
	"""Class for year object, its attributes and methods"""
	def __init__(self, year=2020):
		self.year = year
		self.is_leap_year = False
		self.days = 365
		self.months = {
		"Jan": 31, "Feb": 28, "Mar": 31, "Apr": 30, "May": 31, "Jun": 30,
		"Jul": 31, "Aug": 31, "Sept": 30, "Oct":31 , "Nov": 30, "Dec": 31,
		}

	"""if is_leap_year == True:
		self.days = 366
		self.months["Feb"] = 29"""

	def check_leap_yr(self, year):	
		if year < 1999: logging.error("Enter a year in the 21st century.")

		if year > datetime.datetime.year:
			logging.error("This year does not exit yet.")

		if year % 4 == 0:
			is_leap_year = True
			self.days = 366
			self.months["Feb"] = 29
		return is_leap_year

	def get_jan_1_weekday(self, year):
		weekday = datetime.datetime.date(year=year, month=1, day=1).isoweekday()
		return weekday

	def get_weeks_in_year(self, year):
		weekday = get_jan_1_weekday(year)
		week_1days = 7 - weekday
		periods = float((days - week_1days) / 7)

		if week_1_day < 7:
			periods += 1
		
		if week_1days > 7:
			logging.error("Week cannot be more than 7 days long")
		
		period_list = []
		
		period_list = [(f"{year}-i") for i in range(period)]
		return period_list
