import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


# ---- BigQuery Schema ----
BQ_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "page", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referrer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}


# ---- PARSE JSON MESSAGES ----
class ParsePubsubMessage(beam.DoFn):
    def process(self, element):
        """
        Pub/Sub delivers `bytes`, so decode + JSON parse.
        """
        try:
            obj = json.loads(element.decode("utf-8"))

            # Ensure all fields exist (optional)
            row = {
                "event_id": obj.get("event_id"),
                "user_id": obj.get("user_id"),
                "session_id": obj.get("session_id"),
                "timestamp": obj.get("timestamp"),
                "page": obj.get("page"),
                "event_type": obj.get("event_type"),
                "device": obj.get("device"),
                "referrer": obj.get("referrer"),
                "price": float(obj["price"]) if obj.get("price") not in (None, "") else None
            }

            yield row

        except Exception as e:
            # For debugging; in production send to dead-letter topic
            print(f"Error parsing message: {e}")
            return


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        default="project-name",
        help="GCP project ID"
    )
    parser.add_argument(
        "--region",
        default="us-central1",
        help="Dataflow region"
    )
    parser.add_argument(
        "--runner",
        default="DataflowRunner",
        help="Runner: DataflowRunner or DirectRunner"
    )
    parser.add_argument(
        "--temp_location",
        required=True,
        help="GCS path for temporary files"
    )
    parser.add_argument(
        "--staging_location",
        required=True,
        help="GCS path for staging files"
    )

    args, pipeline_args = parser.parse_known_args()

    subscription = "projects/project-name/subscriptions/clicstream-sub"
    bq_table = "project-name.clickstream.pubsub_events"

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        runner=args.runner,
    )

    with beam.Pipeline(options=pipeline_options) as p:

        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "ParseJSON" >> beam.ParDo(ParsePubsubMessage())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=bq_table,
                schema=BQ_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
