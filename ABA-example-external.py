# ABA-example.py
# tested in python 2.7.12
# implements Phone Number Scrub via ABA and MSS

# uses one non-standard Python Library -> Requests
# see http://docs.python-requests.org/en/master/
# zlib is used to decompress the output file

import time
import zlib
import requests

# this will be your access token
access_token = 'YOUR_ACCESS_TOKEN_HERE'

# this is the name of the file containing the list of phone numbers.
input_file_name = "phonenumbers.txt"
# in this example it contains the following
#+18132633923
#+18135041457
#+18139551760
# i.e. full phone number including leading + and international code
# I have only included 3 numbers, but it can contain up to 5 million
# the only difference to the below code would be that you have to wait
# for over an hour.

print '### Starting Engines ###\n'

## Step 1: We will create the input file in Media Storage
print 'Creating file in Media Storage'

create_file_url = 'https://api.syniverse.com/mediastorage/v1/files'

create_file_payload = {'fileName': '', 'fileTag': '', 'fileFolder': '', 'appName': '', 'expire_time': '',
                       'checksum': '', 'file_fullsize': '2000000'}

create_file_headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}

create_file_response = requests.post(create_file_url, json=create_file_payload, headers=create_file_headers)

print 'create file response status code: ' + str(create_file_response.status_code)
print 'mss create response body: ' + create_file_response.text

## Step 2: We will upload the input file to Media Storage
print '\nUploading input file to Media Storage'

# get the file_id, company id from the create file response
file_id = create_file_response.json()['file_id']
company_id = create_file_response.json()['company-id']

# the URL to use in the request also comes from the create file response
upload_uri = create_file_response.json()['file_uri']

upload_headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/octet-stream',
                  'int-companyid': company_id}

upload_data = open(input_file_name, 'rb').read()

upload_file_response = requests.post(upload_uri, data=upload_data, headers=upload_headers)

print 'upload response status code: ' + str(upload_file_response.status_code)
print 'upload response: ' + upload_file_response.text

## Step 3: Schedule the batch job in Batch Automation
print '\nScheduling the Number Verification batch job in Batch Automation'

schedule_job_url = 'https://api.syniverse.com/aba/v1/schedules'

schedule_job_headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}

schedule_job_payload = {"schedule": { "jobId" : "NIS-Scrub-v1", "name" : "NISScrub", "inputFileId" : file_id,
                                       "fileRetentionDays" : 30, "scheduleRetentionDays" : 30,
                                       "outputFileNamingExpression" : "DS1-NIS-Scrub-output.txt",
                                       "outputFileFolder" : "/opt/apps/aba/output",
                                       "jobRuntimeContext" : {}}}

schedule_job_response = requests.post(schedule_job_url, json=schedule_job_payload,
                                       headers=schedule_job_headers)

print 'Scheduling response status code: ' + str(schedule_job_response.status_code)
print 'Scheduling response: ' + schedule_job_response.text

## Step 4: Wait for job to complete.
# this approach keeps it simple.
# An exercise for the reader would be to either
# 1) implement a loop that checks for when the job is complete
# 2) implement the callback url so details are only received once the job is complete

print '\nWaiting 20s for job to complete'
time.sleep(20)

## Step 5: Get batch job execution details (hoping that job has completed)
print '\nRetrieving batch job execution details'

# we get the schedule id from the response when we scheduled the batch job
# the response is nested json so we need two keys
schedule_id = schedule_job_response.json()['schedule']['id']

# we create the URL to retrieve the batch job execution details
check_execution_url = '/'.join(['https://api.syniverse.com/aba/v1/schedules', schedule_id, 'executions'])

check_execution_headers = {'Authorization': 'Bearer ' + access_token}

check_execution_response = requests.get(check_execution_url, headers=check_execution_headers)

print 'Get batch job details status code: ' + str(check_execution_response.status_code)
print 'Get batch job details response: ' + check_execution_response.text

## Step 6: We download the results from Media Storage
print '\nDownloading the Output file'

# In this simple example we trust that
# 1) the job is complete
# 2) it was successful
# 3) we only download the output file

# We get the output file URI from the execution details response.
# the JSON response include both nested JSON and a list
output_file_uri = check_execution_response.json()['executions'][0]['outputFileURI']

download_output_headers ={'Authorization': 'Bearer ' + access_token, 'int-companyid': company_id}

download_output_response = requests.get(output_file_uri, headers=download_output_headers)

output_data = zlib.decompress(download_output_response.content, zlib.MAX_WBITS|32)

print 'Download output status code: ' + str(download_output_response.status_code)
print 'Download output response: \n' + output_data
