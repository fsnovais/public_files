import argparse
from itertools import product
import multiprocessing
import time
import snowflake.connector
import json

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Maximum number of processes to spawn.
MAX_PROCESSES = multiprocessing.cpu_count()
# Timeout between retries in seconds.
BACKOFF_FACTOR = 5
# Maximum number of retries for errors.
MAX_RETRIES = 5

# Load credentials from config file
with open('config.json') as config_file:
    config = json.load(config_file)


# Assign credentials to variables
SNOWFLAKE_ACCOUNT = config['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_USER = config['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = config['SNOWFLAKE_PASSWORD']
SNOWFLAKE_WAREHOUSE = config['SNOWFLAKE_WAREHOUSE']
SNOWFLAKE_DATABASE = config['SNOWFLAKE_DATABASE']
SNOWFLAKE_SCHEMA = config['SNOWFLAKE_SCHEMA']


def main(client, customer_ids):
    """The main method that creates all necessary entities for the example.

    Args:
        client: an initialized GoogleAdsClient instance.
        customer_ids: an array of client customer IDs.
    """

    # Define the GAQL query strings to run for each customer ID.
    campaign_query = """
        SELECT campaign.id, metrics.impressions, metrics.clicks
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS"""
    ad_group_query = """
        SELECT campaign.id, ad_group.id, metrics.impressions, metrics.clicks
        FROM ad_group
        WHERE segments.date DURING LAST_30_DAYS"""

    inputs = generate_inputs(
        client, customer_ids, [campaign_query, ad_group_query]
    )
    with multiprocessing.Pool(MAX_PROCESSES) as pool:
        # Call issue_search_request on each input, parallelizing the work
        # across processes in the pool.
        results = pool.starmap(issue_search_request, inputs)

        # Partition our results into successful and failed results.
        successes = []
        failures = []
        for res in results:
            if res[0]:
                successes.append(res[1])
            else:
                failures.append(res[1])

        # Load results to Snowflake
        load_to_snowflake(successes)

        print(
            f"Total successful results: {len(successes)}\n"
            f"Total failed results: {len(failures)}\n"
        )

        print("Failures:") if len(failures) else None
        for failure in failures:
            ex = failure["exception"]
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" for customer_id '
                f'{failure["customer_id"]} and query "{failure["query"]}" and '
                "includes the following errors:"
            )
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for (
                        field_path_element
                    ) in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")


def issue_search_request(client, customer_id, query):
    """Issues a search request using streaming.

    Retries if a GoogleAdsException is caught, until MAX_RETRIES is reached.

    Args:
        client: an initialized GoogleAdsClient instance.
        customer_id: a client customer ID str.
        query: a GAQL query str.
    """
    ga_service = client.get_service("GoogleAdsService")
    retry_count = 0
    # Retry until we've reached MAX_RETRIES or have successfully received a
    # response.
    while True:
        try:
            stream = ga_service.search_stream(
                customer_id=customer_id, query=query
            )
            # Returning a list of GoogleAdsRows will result in a
            # PicklingError, so instead we put the GoogleAdsRow data
            # into a list of str results and return that.
            result_data = []
            for batch in stream:
                for row in batch.results:
                    ad_group_id = (
                        row.ad_group.id if "ad_group.id" in query else None
                    )
                    result = {
                        "campaign_id": row.campaign.id,
                        "ad_group_id": ad_group_id,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                    }
                    result_data.append(result)
            return (True, {"results": result_data})
        except GoogleAdsException as ex:
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                return (
                    False,
                    {
                        "exception": ex,
                        "customer_id": customer_id,
                        "query": query,
                    },
                )

def load_to_snowflake(successes):
    """Loads successful results to Snowflake.

    Args:
        successes: A list of successful results.
    """
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )
    cursor = conn.cursor()
    
    # Create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.campaign_data (
        campaign_id STRING,
        ad_group_id STRING,
        impressions NUMBER,
        clicks NUMBER
    )
    """
    cursor.execute(create_table_query)

    print("Created table for landing data")
    # Use the specified warehouse, database, and schema
    cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # Insert data
    insert_query = """
    INSERT INTO campaign_data (campaign_id, ad_group_id, impressions, clicks)
    VALUES (%s, %s, %s, %s)
    """
    
    for success in successes:
        for result in success["results"]:
            cursor.execute(insert_query, (
                result["campaign_id"],
                result["ad_group_id"],
                result["impressions"],
                result["clicks"]
            ))
    
    print("Data loaded into Snowflake table successfully.")

    cursor.close()
    conn.commit()
    conn.close()

def generate_inputs(client, customer_ids, queries):
    """Generates all inputs to feed into search requests.

    A GoogleAdsService instance cannot be serialized with pickle for parallel
    processing, but a GoogleAdsClient can be, so we pass the client to the
    pool task which will then get the GoogleAdsService instance.

    Args:
        client: An initialized GoogleAdsClient instance.
        customer_ids: A list of str client customer IDs.
        queries: A list of str GAQL queries.
    """
    return product([client], customer_ids, queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download a set of reports in parallel from a list of "
        "accounts."
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_ids",
        nargs="+",
        type=str,
        required=True,
        help="The Google Ads customer IDs.",
    )
    parser.add_argument(
        "-l",
        "--login_customer_id",
        type=str,
        help="The login customer ID (optional).",
    )
    args = parser.parse_args()

    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(version="v16")
    # Override the login_customer_id on the GoogleAdsClient, if specified.
    if args.login_customer_id is not None:
        googleads_client.login_customer_id = args.login_customer_id

    main(googleads_client, args.customer_ids)
