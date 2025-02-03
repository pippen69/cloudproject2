from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 
import csv #import csv

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0];

# Set the project_id with your project ID
project_id="custom-graph-449020-a3";
topic_name = "csvtopic";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

with open('Labels.csv', mode= 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader: 
        message = json.dumps(row).encode('utf-8')
        print("Producing a record: {}".format(message))    
        future = publisher.publish(topic_path, message);
        future.result()
    print("finished pub")
