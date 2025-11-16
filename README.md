# clickstream
clickStream data engineering

### Steps to create.
copy the python code which generates data , chane the project ID.

> make sure pubsub is installed> ```bash pip install google-cloud-pubsub ```

> have a service account with pubsub publisher access, our generator program will use that to publish data.

> create a pubsub topic : ```bash gcloud pubusb topics create cickstream-topic ```
> create a subscription : ```bash gcloud pubsub subscriptions create clicstream-sub --topic=clickstream-topic ```

> Test messages: run the python generator code ```bash python generator.py ``` 

> Check messages: bash```gcloud pubsub subscriptions pull clickstream-sub --auto-ack --limit=10```

> RUN the dataflow JOB to insert into bigquery:
 ```
 $ python streaming.py --runner=DataflowRunner --project=project-id --region=us-central1 --temp_location=gs://project-id-clickstream/temp --staging_location=gs://project-id-421203-clickstream/staging
 ```
> Make sure dataflow service account (or default compute SA) is having access to read from pubsub and write to bigquery.


