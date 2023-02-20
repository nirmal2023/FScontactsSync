""" 
	README for Bulk IMPORT

	This Python3 program uses the bulk upsert API for Contacts. https://developer.freshsales.io/api/#bulk_upsert_contact

	Usage: 
		1. (Line numbers 21 to 23) Modify the Input file, URL and access token according the user's requirement 
		2. (Line numbers 71 to 72) Modify the list of CSV columns, field names and the JSON parameters.  
		3. Run the program :  python3 bulk_import.py
	
	Dependencies: python3 and requests 

	Author: nirmalkumar.sathiamurthi@freshworks.com   Last Updated On: Feb 20 2023
"""
import csv, json
import requests
import time
import logging

																								########## MODIFY the file name, URL and ACCESS_TOKEN ##########
INPUT_FILE = "/Users/nsathiamurthi/Downloads/upsert_nldata_1L.csv"
URL = "https://***.myfreshworks.com/crm/sales/api/contacts/bulk_upsert"
ACCESS_TOKEN = "***"
logfile_name = "bulk_import_log.txt"
logging.basicConfig(filename=logfile_name,filemode='a',level=logging.DEBUG, format='%(levelname)s : %(asctime)s :: %(message)s')
BATCH_SIZE = 100
MAX_RETRIES = 5
processed_count = 0
processed_jobs = []

headers = {
	'Authorization': 'Token '+ACCESS_TOKEN,
	'Content-Type': 'application/json'
}

def process_records(one_batch,retry_count=0):
	if retry_count >= MAX_RETRIES:
		logging.critical('Exceeded maximum retries. Now exiting the program')
		logging.critical('The following are the job status URLs: '+'\n '.join(processed_jobs))
		exit()
	try:
		payload = json.dumps({"contacts": one_batch})
		# logging.info(payload)
		r = requests.request("POST", URL, headers=headers, data=payload)
		logging.info(r)
		logging.debug(r.json())
		if r.status_code !=200:
			logging.debug('wait for 30s')
			time.sleep(30)
			process_records(one_batch,retry_count+1)  
		else:
			processed_jobs.append(r.json()['job_status_url'])
	except:
		logging.exception('Error occurred in process_records function')
		logging.debug('Retrying in 10s')
		time.sleep(10)
		process_records(one_batch,retry_count+1)


with open(INPUT_FILE,'r', encoding="utf8")as f:
	print('processing csv in batches of '+str(BATCH_SIZE))
	print('Logs are written in ',logfile_name)
	csvFile = csv.reader(f, delimiter=',', quotechar='|')
	batch = []
	for i,row in enumerate(csvFile):
		if i==0:#Skip the header
			i+=1
			continue 
		
		# Order of the items in CSV file                            ########## MODIFY THE COLUMNS in the CSV to be read ##########
		email,_,_,ncash = row[:4]
		batch.append({"emails": email, "data":{"custom_field": {"cf_ncash": ncash}}})       ########## MODIFY THE params in the JSON payload ##########
		if i>=(BATCH_SIZE + processed_count):
			process_records(batch)
			processed_count += len(batch)
			batch = []
		i+=1
	if len(batch)>0:
		process_records(batch)
		processed_count += len(batch)

logging.debug('Data inserted through multiple jobs. The following are the job status URLs: '+'\n '.join(processed_jobs))
logging.debug('Total records pushed via above jobs: '+str(processed_count))
print('Data inserted through multiple jobs. The following are the job status URLs: '+'\n '.join(processed_jobs))
print('Total records pushed via above jobs: '+str(processed_count))

try:
	if len(processed_jobs) > 1:
		r = requests.request("GET", processed_jobs[0], headers=headers)
		logging.debug('First Job status: '+r.text)
		r = requests.request("GET", processed_jobs[-1], headers=headers)
		logging.debug('Last Job status: '+r.text)
except Exception as e:
	logging.exception("Exception occurred")