# clickstream
clickStream data engineering

### Steps to create.
copy the python code which generates data , chane the project ID.

> make sure pubsub is installed> ```bash pip install google-cloud-pubsub ```

> have a service account with pubsub publisher access, our generator program will use that to publish data.

> create a pubsub topic : ```bash gcloud pubusb topics create cickstream-topic ```
> create a subscription : ```bash gcloud pubsub subscriptions create clicstream-sub --topic=clickstream-topic ```

> Test messages: run the python generator code ```bash python generator.py ``` 




