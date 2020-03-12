from google.cloud import bigquery
import pandas as pd
import requests
import os

# To read excel file
import gcsfs
import xlrd

# setting environment variables
TOKEN = os.environ.get('token')
GS_BUCKET = os.environ.get('bucket')
PROJECT = os.environ.get('project')
DATASET = os.environ.get('dataset')
SEGMENTS_TABLE = os.environ.get('segments_table')
PROGRAMS_TABLE = os.environ.get('programs_table')
REQUEST_URL = 'https://xxxxxxxxxxx.io/api/xxxxxxxxx/'
XXXXXXXXX_IDS = {"XXXXXXX":"XXXXXXXXXX","XXXXXXXXX":"XXXXXXXXX"}

# defining bq client
bq_client = bigquery.Client(project=PROJECT)

def get_as_run_df(event, context):
    
	if context.event_type != 'google.storage.object.finalize':
		print(f"{file_name} did not trigger a 'finalize' event, ignoring.")
		return;
    
    # returning file name trigger
	file = event
	file_name = file['name']
    
    # reading excel file from bucket storage with panda(dataframe as df)
	as_run_df = pd.read_excel('gs://' + GS_BUCKET +'/'+ file_name, encoding = 'utf-8')

    # check to ensure we're processing a excel file
	if ".xls" not in file_name:
		raise Exception(f"{file_name} is not an xls file, ignoring.")
		
    # check that required columns are present
	df_cols = list(as_run_df.columns)
	checklist = ['columns#1', 'columns#2', 'columns#3', 'columns#4', 'columns#5', 'columns#6']
	diffset = set(checklist).difference(df_cols)
	if diffset:
		raise Exception(f'Missing required columns {diffset}')
    
    # dropping unused columns
	as_run_df = as_run_df.drop(["Transfer status", "Machine status", "State", "Resource"], axis=1)
    # renaming columns
	as_run_df.columns = ["columns#1", "columns#2", "columns#3", "columns#4", "columns#5", "columns#6"]
    # convert Duraction to seconds (hh:mm:ss:ff => s). Splitting frame. 
	as_run_df['duration'] = [x[:-3] for x in as_run_df['duration']]
	as_run_df['duration'] = as_run_df['duration'].str.split(':').apply(lambda x : int(x[0]) * 3600 + int(x[1]) * 60 + int(x[2])).astype(int)
    # convert Start time to timestamp
	as_run_df['start_time'] = pd.to_datetime(as_run_df['start_time'],format='%m/%d/%Y %H:%M:%S;%f')
    # computing End time
	as_run_df['end_time'] = as_run_df["start_time"] + pd.to_timedelta(as_run_df['duration'], unit='s')
    # adding file name to dataframe
	as_run_df['file_name'] = file_name
    # capture processing time for debugging purposes
	as_run_df['file_process_timestamp'] = pd.Timestamp.now()
    
	return as_run_df
    
def get_api_data(as_run_df):
	# Set XXXXXXXX based on first entry in CSV
	XXXXXXXX = XXXXXX[as_run_df["XXXXXX"][1]]
	request_url_final = REQUEST_URL + XXXXXXX + '/XXXXXXX'
    
	# Gather unique XXXXXXX where the template is XXXXXXX
	XXXXXXX_ids = set(as_run_df[as_run_df['template'] == "XXXXXXX"]['XXXXXXX'])
	payload = {'import_ids': list(XXXXXXXXXX), 'filter' : str({'limit':200}).replace(' ', '').replace('\'', '\"')}
    
	try:
		response = requests.get(request_url_final,params = payload,headers = {'accept': 'application/json','ClientToken': TOKEN})
	except requests.exceptions.RequestException as e:
		#TODO use GCP error logging
		print(e)
	response_json = response.json()
    
	response_list = list()
    
	for episode in response_json:
		row = dict()
		row["XXXXXXX"] = XXXXX["XXXXX"]
		row["XXXXX"] = XXXXX["XXXXX"]
		row["XXXXX"] = XXXXX["XXXXX"]
		if "XXXXXX" in XXXXXX and "XXXXX" in XXXXX["XXXX"]:
			row["XXXXXX"] = XXXXX["XXXX"]["XXXXX"]
		else: 
			row["XXXXXX"] = None
		if "XXXXXX" in XXXXXX and "XXXXX" in XXXXXX["XXXXXX"]:
			row["XXXXXX"] = XXXXXX["XXXXX"]["XXXXXX"]
		else:
			row["XXXXXX"] = None
		response_list.append(row)
	
	response_df = pd.DataFrame.from_dict(response_list)	
	response_df["XXXXXX"] = pd.to_datetime(response_df["XXXXXXXX"],format='%m/%d/%Y %H:%M:%S;%f')
	return response_df

def get_segments_df(as_run_df, response_df):
	#Join the as-run DF with the response-DF to create master DF with all data
	joined_df = as_run_df.merge(response_df, how="left", left_on="XXXXXX", right_on="import_id")

    # Replace episode title with XXXXXXX if blank 
	mask = (joined_df['XXXXXXX'] == '')
	joined_df.loc[mask, 'XXXXXX'] = joined_df['XXXXXX']
    
    # reorder columns
	joined_df = joined_df[[columns#1,#2,#3,#4,etc...]]
    
    # Fully - qualified ID in standard SQL is required here 
	load_job = bq_client.load_table_from_dataframe(
		joined_df, '.'.join([PROJECT, DATASET, SEGMENTS_TABLE])
	)
	print("Starting job {}".format(load_job.job_id))

	load_job.result()
	print("Segments Job finished.")

def get_programs_df(as_run_df, response_df):
    # potential SettingWithCopyWarning issue here. I've solved it by adding .copy() but I'm not sure this is the best option. 
    # https://stackoverflow.com/questions/49728421/pandas-dataframe-settingwithcopywarning-a-value-is-trying-to-be-set-on-a-copy
    # https://stackoverflow.com/questions/29888341/a-value-is-trying-to-be-set-on-a-copy-of-a-slice-from-a-dataframe-warning-even-a
    # https://stackoverflow.com/questions/38809796/pandas-still-getting-settingwithcopywarning-even-after-using-loc/38810015#38810015
    # https://stackoverflow.com/questions/23486049/force-return-of-view-rather-than-copy-in-pandas/39983664#39983664
	programs_df = as_run_df[as_run_df['template'] == "XXXXXXX"].copy()
    
	programs_df["XXXXXXX"] = (programs_df["as_run_title"].shift(1) != programs_df["XXXXXXX"]).cumsum()

	programs_df = programs_df.groupby(['XXXXX', 'file_process_timestamp', 'as_run_XXXXX', 'XXXXX', 'XXXXX', 'XXXX_id']).agg({'start_time':'min', 'end_time':'max'})[['start_time','end_time']].reset_index()
    
    # Recompute duration based on program groups
	programs_df['XXXXXXX'] = ((programs_df["XXXXX"] - programs_df["XXXXXXXX"]).dt.total_seconds()).astype(int)
    
    #Join the grouped as-run data with the response-DF to create master DF with all program data
	programs_df = programs_df.merge(response_df, how="left", left_on="XXXXXX_id", right_on="import_id")
	
    # Dropping program_group
	programs_df = programs_df.drop(["program_group"], axis=1)
	print(list(programs_df.columns))
    # Reorder columns
	programs_df = programs_df[[Columns list here]]
	print(list(programs_df.columns))
    
	#job_config = bigquery.LoadJobConfig(schema = [
  #bigquery.SchemaField("XXXXXX", "STRING"),
  #bigquery.SchemaField("XXXXX_id", "STRING"), 
  #bigquery.SchemaField("import_id", "STRING"),
  #bigquery.SchemaField("start_time", "TIMESTAMP"),
  #bigquery.SchemaField("end_time", "TIMESTAMP"),
  #bigquery.SchemaField("duration", "INT64"),    
	#bigquery.SchemaField("XXXXXXX", "STRING"),
	#bigquery.SchemaField("file_name", "STRING"),
	#bigquery.SchemaField("file_process_timestamp", "TIMESTAMP"),
	#bigquery.SchemaField("XXXXXXX", "STRING"),
  #bigquery.SchemaField("XXXXX_date", "TIMESTAMP"),
	#bigquery.SchemaField("XXXXX_title", "STRING"),
	#bigquery.SchemaField("XXXXXX", "STRING"),
  #])
    
    
    # Fully - qualified ID in standard SQL is required here 
	load_job = bq_client.load_table_from_dataframe(
		programs_df, '.'.join([PROJECT, DATASET, PROGRAMS_TABLE]),
	)
	print("Starting job {}".format(load_job.job_id))

	load_job.result()
	print("Programs Job finished.")
    
def main(event, context):
	as_run_df = get_as_run_df(event, context)
	response_df = get_api_data(as_run_df)
	segments_df = get_segments_df(as_run_df,response_df)
	programs_df = get_programs_df(as_run_df, response_df)
