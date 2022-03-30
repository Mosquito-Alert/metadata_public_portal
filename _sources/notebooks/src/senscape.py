import requests, psycopg2, sys, concurrent.futures, json, logging
import pandas as pd
from io import StringIO
from datetime import timezone
from tqdm import tqdm

# Setup the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False  # activate logger
file_formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s.%(funcName)s | %(message)s"
)
stream_formatter = logging.Formatter("%(message)s")
# file_handler = logging.FileHandler('api.log', mode='a')
# file_handler.setFormatter(file_formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(stream_formatter)

# logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def connect(param):
    """Connect to postgres DB"""

    uri = "postgresql://%s:%s@%s:%s/%s" % (
        param["user"],
        param["password"],
        param["host"],
        param["port"],
        param["database"],
    )

    try:
        conn = psycopg2.connect(uri)
    except psycopg2.Error as e:
        print(f"Had problem connecting with error {e}.")
        sys.exit(1)

    return conn, uri


def executeQuery(conn, queries=[]):
    """ Execute a list of queries """

    if not isinstance(queries, list):
        queries = [queries]

    ret = []  # Return select value
    cursor = conn.cursor()
    try:
        for query in queries:
            cursor.execute(query)
            conn.commit()
            if "select" in query.lower():
                ret.append(cursor.fetchall())
            else:
                ret.append(0)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error executeQuery: %s" % error)
        conn.rollback()
        cursor.close()
        return False

    cursor.close()
    return ret


def copyFromStringIO(conn, df, table):
    """ Save the dataframe in memory to copy it to an already created DB table """

    # Save dataframe to an in memory buffer
    buffer = StringIO()
    # Take care of how NaN's are represented it StringIO. By default they are
    # like ',,,,' with comma separation, but for SQL copy_from we need a string
    # to represent then, like ',Null, Null, Null,'
    df.replace({None: "Null"}).to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(
            buffer, table, sep=",", null="Null"
        )  # works if header is False
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error copyFromStringIO: %s" % error)
        conn.rollback()
        cursor.close()
        return False

    cursor.close()
    return True


def dateFormat(date):
    """ Proper formatting of a timezone aware date """

    return (
        date.astimezone(timezone.utc)
        .isoformat("T", "milliseconds")
        .replace("+00:00", "Z")
    )


def getData(url, headers, timeout=1, data_name="samples"):
    """ Request from url and get records only """

    res = requests.get(url, headers=headers, timeout=timeout)
    return pd.DataFrame(res.json()[data_name])


def getCount(headers, startDate=""):
    """ Get total record count from starting with a given date """

    url = urlGen(startDate=startDate)
    res = requests.get(url, headers=headers)
    try:
        return res.json()["count"]
    except:
        logger.error(
            f"Request API Error: status code is <{res.status_code}> with reason: {res.reason}"
        )
        return 0


def urlGen(pageSize=1, pageNumber=0, startDate=""):
    """ Generate the url given the main filtering keys """

    url_filter = "https://senscape.eu/api/data?sortOrder=asc&sortField=record_time"
    url = url_filter + f"&pageSize={pageSize}&pageNumber={pageNumber}"

    if startDate != "":
        # Begin counting pages after startDate
        return url + f"&filterStart={startDate}"
    else:
        # Begin from the first available record
        return url


def getUrls(count, pageSize=10, startDate=None):
    """ Get a list of urls for the multi-treading request """

    if count > 0:
        pageNumberTot = count // pageSize + 1
        return [
            urlGen(pageSize, pageNumber, startDate)
            for pageNumber in range(0, pageNumberTot)
        ]
    else:
        logger.warning(f"Empty list of urls since data count is zero: check startDate!")
        return []


def requestExecutor(
    urls, headers, timeout=1, workers=10, set_sort_index="", df_query=""
):
    """ Execute the multi-treading request given a list of urls and save the response in a csv file if necessary """

    with tqdm(total=len(urls)) as pbar:

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            failed = {
                "created_time": str(pd.Timestamp.now()),
                "url": [],
                "exeption": [],
            }
            data = []
            # Start the load operations and mark each future with its URL
            future_to_url = {
                executor.submit(getData, url, headers, timeout): url for url in urls
            }
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    df_thread = future.result()
                    if df_query != "":
                        df_thread.query(df_query, inplace=True)
                    data.append(df_thread)
                except Exception as exc:
                    failed["url"].append(url)
                    failed["exeption"].append(str(exc))
                    print("%r Generated an exception: %s" % (url, exc))

                pbar.update(1)

    # Write metadata relative to processed files
    with open("./failed_request.json", "w") as outfile:
        json.dump(failed, outfile, indent=4, sort_keys=True)

    # Join and sort the columns alfabeticaly
    df = pd.concat(data, axis=0, sort=True)

    # Sort the rows by a given column index
    if set_sort_index != "":
        df = df.set_index(set_sort_index).sort_index()

    return df


def uploadData(
    headers,
    conn,
    queries,
    startDate="",
    table_name="data",
    columns=[],
    set_sort_index="",
    df_query="",
    save_csv=False,
):
    """ Upload the hole data set from the API and save it on a csv file if necessary"""

    # Set the request range for all the dataset
    count = getCount(headers, startDate=startDate)

    # Alternative startDate if first request attempt fails because of history data limit
    if count == 0:
        # There is a problem with the api and data (probably there is a limit of 112 days history)
        startDate = "2020-08-05T00:00:00.000Z"
        count = getCount(headers, startDate=startDate)
        logger.info(f"Data are available only from {startDate}")

    urls = getUrls(count, pageSize=1000, startDate=startDate)
    df = requestExecutor(
        urls, headers, timeout=60 * 3, set_sort_index=set_sort_index, df_query=df_query
    )
    if save_csv:
        # Save data on a local csv file
        df.to_csv(table_name + ".csv", mode="w", header=True)

    if table_name != "":
        # Drop the table from DB if exists and create new one with downloaded data
        res_make_table = executeQuery(conn, queries)
        res_load_table = copyFromStringIO(conn, df.reset_index()[columns], table_name)
        if res_load_table:
            if startDate != "":
                logger.info(
                    f"Database was created with {len(df)}/{count} new records starting from {startDate}."
                )
            else:
                logger.info(
                    f"Database was created with {len(df)}/{count} new records (overall dataset)."
                )

    return df


def updateData(
    headers,
    conn,
    startDate="",
    table_name="data",
    columns=[],
    set_sort_index="",
    df_query="",
    save_csv=False,
):
    """ Check for new data and update the database """

    # Check for new data
    count = getCount(headers, startDate=startDate)
    if count > 0:
        urls = getUrls(count, pageSize=1000, startDate=startDate)
        # Get the incremental dataset
        df = requestExecutor(
            urls,
            headers,
            timeout=60 * 3,
            set_sort_index=set_sort_index,
            df_query=df_query,
        )
        # Append to the existing csv
        if table_name != "" and df is not None:
            copyFromStringIO(
                conn, df.reset_index()[columns], table_name
            )  # append to DB table
            if save_csv:
                # Append data on a local csv file
                df[columns].to_csv(table_name + ".csv", mode="a", header=False)
        logger.info(
            f"Database was updated with {len(df)}/{count} new records starting from {startDate}."
        )
        return df

    else:
        logger.info(f"Database is up to date with last record at: {startDate}")


def uploadDevices(
    headers,
    conn,
    queries,
    table_name="devices",
    columns=[],
    df_query="",
    save_csv=False,
):
    """ Upload all the devices from the API and save it on a csv file if necessary"""

    # Get the available devices info
    url = "https://senscape.eu/api/devices"
    df = getData(url, headers, timeout=60 * 3, data_name="devices")

    # Hard code information about 'disposed' date-time of devices
    df = df.query(df_query).drop("typ", axis=1)
    df = df.astype({"dev_timestamp": "int", "tags": "str", "description": "str"})
    df.insert(4, "disposed", "9999-01-01T00:00:00.000Z")

    if table_name != "":
        # Drop the table from DB if exists and create new one with downloaded data
        res = executeQuery(conn, queries)
        copyFromStringIO(conn, df.reset_index()[columns], table_name)
        if save_csv:
            # Save data on a local csv file
            df.to_csv(table_name + ".csv", mode="w", header=True, index=False)

    return df
