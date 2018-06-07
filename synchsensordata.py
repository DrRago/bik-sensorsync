"""
Betriebsmodus: Drehzahl -> Bandgeschwindigkeit
1900 U/min -> 0.5 m/s
3800 U/min -> 1.0 m/s

"""
import csv
import logging
import os
import time
import mysql.connector

from os import listdir
from os.path import isfile, join
from mysql.connector import errorcode
from mysql.connector.connection import MySQLConnection

from vorüberlegungen import *

# =================
# = Configuration =
# =================

# basic configuration
file_root_path = "C:/Users/Leonhard.Gahr/Documents/KIPro/2018-05-24-14-15-36-14_daten/"
current_position = 8.0

# mysql connection configuration
mysql_config = {
    'user': 'root',
    'password': '12345',
    'host': '127.0.0.1',
    'database': 'kipro',
    'port': 3306
}

fetch_limit = 500
fetch_limit_increase_rate = 500
max_fetch_limit = 5000

# sensor configuration
kinect = {
    "name": "Kinect",
    "position": 0.0,
    "data": "file",
    "specification": "image",
    "location": file_root_path + "kinect/",
    "file_prefixes": [
        "color",
        "depth"
    ]
}

analyser = {
    "name": "x102",
    "position": 0.6,
    "data": "file",
    "specification": "csv",
    "location": file_root_path + "udp_x102/2018_05_24/",
    "file_templates": [
        "kipro-analyser-data_%(year)s-%(month)s-%(day)s_%(hour)s-%(minute)s-%(second)s.%(millisecond)s.csv"
    ]
}

analyser_x101 = {
    "name": "x101",
    "position": 0.6,
    "data": 'database',
    'specification': 'data_analyser_x101_daten',
    'location': {
        'field_no': {
            'start': 4,  # inclusive
            'end': 19  # exclusive
        }
    },
    'condition': {
        'datediff': {
            'field_name': 'time'
        }
    }
}

bandwaage = {
    "name": "Bandwaage",
    "position": 7.6,
    "data": "database",
    "specification": "opc_data",  # the table
    "location": {
        "field_name": "ITEM_VALUE",
    },
    "condition": {
        'datediff': {
            'field_name': "READ_TIME",
        },
        'field': {
            'field_name': 'ITEM_NAME',
            'value': 'ET 200SP-Station_1.ET200SP.OPC_DATA.Messwerte.BANDWAAGE'
        }
    }
}

sensors = [kinect, analyser, analyser_x101, bandwaage]

# line configuration
knickband = {
    "name": "Knickband",
    "database_name": "Knickband",
    "id": 0,
    "length": 4.2,
    "speed_factor": 1 / 3800

}
zufuehrband = {
    "name": "Zuführband",
    "database_name": "Aufgabebunker",
    "id": 1,
    "length": 6.8,
    "speed_factor": 1 / 3800
}
lines = {
    "selector_template": "ET 200SP-Station_1.ET200SP.OPC_DATA.Foerderbaender.%(line_name)s.%(value_name)s",
    "lines": [knickband, zufuehrband]
}

# structure templates
img_template = {
    'id': 0,
    'datetime': datetime.datetime(1, 1, 1),
    'locus_count': 0,
    'loci': [],
    'recipe': ''
}


def execute_query(connection, query):
    """
    Executes a single mysql query
    :param connection:  the database to execute the query on
    :param query: the actual query
    :type connection: MySQLConnection
    :type query: str

    :return: the query result
    :rtype: list[dict]
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_speeds(connection):
    """
    Get the speeds of the lines from the database
    :param connection: the database
    :type connection: MySQLConnection

    :return: the list with the speeds
    :rtype: list[list[dict]]
    """
    logging.info("Getting line speeds from database for %s" % ', '.join(map(str, [f['name'] for f in lines['lines']])))
    v = []
    for line in lines["lines"]:
        query = "SELECT ITEM_VALUE as value, READ_TIME as time FROM `opc_data` " \
                "WHERE `ITEM_NAME` = '%s' ORDER BY READ_TIME DESC LIMIT %d" % (
                    lines["selector_template"],
                    fetch_limit
                )
        query = query % {'line_name': line["database_name"], 'value_name': 'Istwert_Drehzahl'}
        result = execute_query(connection, query)

        logging.debug("Executed query \"%s\" and got %d results" % (query, len(result)))

        current_v = []
        for row in result:
            current_v.append(
                {'datetime': row["time"],
                 'speed': float(row["value"]) * line['speed_factor']})
        v.append(current_v)

    return v


def reformat_analyser(sensor_analyser):
    """
    Rename analyser csv files
    :param sensor_analyser: the analyser sensor dictionary
    """
    logging.debug("Rename analyser files")
    path = sensor_analyser['location']
    for f in listdir(path):
        f = join(path, f)
        if isfile(f):
            with open(f, 'r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                time_start = datetime.datetime.strptime(next(csv_reader)[1], '%Y-%m-%d %H:%M:%S.%f')
            if (time_start is None):
                logging.info("File \"%f\" is empty. Removing...")
                os.remove(f)
                continue
            new_file = join(sensor_analyser['location'],
                            sensor_analyser["file_templates"][0])

            time_start_settings = {
                'year': time_start.year,
                'month': time_start.month,
                'day': time_start.day,
                'hour': time_start.hour,
                'minute': time_start.minute,
                'second': time_start.second,
                'millisecond': int(time_start.microsecond / 1000)
            }
            new_file = new_file % time_start_settings
            os.rename(f, new_file)


def get_locus_from_row(row):
    """
    Get a locus from a row of the csv file

    :param row: the row
    :return: the locus as dictionary with the following structure:
        {
            'number': int,
            'classification': int,
            'color_r': int,
            'color_g': int,
            'color_b': int,
            'height': int,
            'spectra': list[float]
        }
    """
    locus = {
        'number': int(row[2]),
        'classification': int(row[261]),
        'color_r': int(row[262]),
        'color_g': int(row[263]),
        'color_b': int(row[264]),
        'height': int(row[265]),
        'spectra': row[5:240]
    }
    return locus


def access_csv_data(path, timestamp, following_path):
    """
    Read the data for a specific time from a csv file

    :param path: The path of the file the data should be located in
    :param timestamp: The timestamp of the data to be accessed
        (if no record for that time exists, the method takes the closest one)
    :param following_path: The path of the following file in case the dataset is incomplete in the actual file
    :type path: str
    :type timestamp: datetime.datetime
    :type following_path: str

    :return: A dictionary with the following structure:
        {
            'id': int,
            'datetime': datetime.datetime,
            'locus_count': int,
            'loci': list[
                    {
                        'number': int,
                        'classification': int,
                        'color_r': int,
                        'color_g': int,
                        'color_b': int,
                        'height': int,
                        'spectra': list[float]
                    }
                ],
            'recipe': str
        }
    """
    with open(path, "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        # initialize img to probably avoid key errors
        img = img_template

        previous_img = None

        for row in csv_reader:
            row_datetime = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
            # add the row to the current image, if it's still part of it, otherwise create a new one
            if (img['id'] == int(row[0]) and img['datetime'] == row_datetime):
                img['loci'].append(get_locus_from_row(row))
            else:
                previous_img = img

                # initialize the image
                img = {'id': int(row[0]),
                       'datetime': row_datetime,
                       'locus_count': int(row[3]),
                       'recipe': row[266],
                       'loci': [get_locus_from_row(row), ]}

            # check if the image is complete
            if (len(img['loci']) == img['locus_count']):

                # check if the current image or the previous image is closer to the timestamp
                if (img['datetime'] > timestamp):
                    if (previous_img is not None):
                        current_img_datetime = img['datetime']
                        previous_img_datetime = previous_img['datetime']
                        if (timestamp - previous_img_datetime < current_img_datetime - timestamp):
                            img = previous_img
                    break

        # check if the image is incomplete. Can happen if an image is written over two files
        if (len(img['loci']) != img['locus_count']):
            if (following_path is None):
                logging.error("Image #%d incomplete. Returning anyways" % img['id'])
            else:
                logging.debug("Image #%d incomplete, trying to complete with %s" % (img['id'], following_path))
                with open(following_path, "r") as next_csv_file:
                    next_csv_reader = csv.reader(next_csv_file, delimiter=';')
                    for row in next_csv_reader:
                        current_img = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                        if (current_img == img['datetime']):
                            img['loci'].append(get_locus_from_row(row))
                        else:
                            break
                    if (len(img['loci']) == img['locus_count']):
                        logging.debug("Image #%d completed")
                    else:
                        logging.error("Image #%d still incomplete. Returning anyways")
        return img


def get_sensor_data_from_csv(timestamp, sensor):
    # TODO optimize like kinect

    file_times = []
    for file in os.listdir(sensor['location']):
        if (isfile(join(sensor['location'], file)) and
                file.startswith(sensor['file_templates'][0].split("%", 1)[0])):
            datetime_string = file[len(sensor['file_templates'][0].split("%", 1)[0]):-len(
                file.split(".")[-1]) - 1]
            file_times.append(datetime.datetime.strptime(datetime_string, "%Y-%m-%d_%H-%M-%S.%f"))

    target_file_time = None
    next_file_time = None
    for e in range(0, len(file_times)):
        if (file_times[e] < timestamp):
            target_file_time = file_times[e]
            if (len(file_times) - 1 != e):
                next_file_time = file_times[e + 1]
            else:
                next_file_time = None

    if (target_file_time is None):
        logging.error("There are no records for %s in %s" % (timestamp, sensor['location']))
        return

    string_settings = {
        'year': target_file_time.year,
        'month': target_file_time.month,
        'day': target_file_time.day,
        'hour': target_file_time.hour,
        'minute': target_file_time.minute,
        'second': target_file_time.second,
        'millisecond': int(target_file_time.microsecond / 1000)
    }

    next_datetime_file = None
    if (next_file_time is not None):
        next_date_settings = {
            'year': next_file_time.year,
            'month': next_file_time.month,
            'day': next_file_time.day,
            'hour': next_file_time.hour,
            'minute': next_file_time.minute,
            'second': next_file_time.second,
            'millisecond': int(next_file_time.microsecond / 1000)
        }

        next_datetime_file = join(sensor['location'], sensor['file_templates'][0] % next_date_settings)

    return access_csv_data(join(sensor['location'], sensor['file_templates'][0] % string_settings),
                           timestamp,
                           next_datetime_file)


def get_sensor_data(timestamp, sensor, db=None):
    """
    Get all sensor data at a specific time (or closest to that time)
    :param timestamp: The time the data should be accessed
    :param sensor: The sensor to get the data from
    :param db: If necessary a database the sensordata is saved in
    :type timestamp: datetime.datetime
    :type sensor: dict
    :type db: MySQLConnection

    :return: sensor data
    :rtype: list
    """
    logging.debug("Getting data from sensor '%s' at %s" % (sensor['name'], str(timestamp)))
    start = time.time()

    sensor_data = []
    if (sensor['data'] == 'file'):
        if (sensor['specification'] == 'csv'):
            sensor_data.append(get_sensor_data_from_csv(timestamp, sensor))
        elif (sensor['specification'] == 'image'):
            os.chdir(sensor['location'])
            files = os.listdir(sensor['location'])
            # sort all files by date of creation
            files.sort(key=lambda x: os.path.getmtime(x))

            # split the prefixes of the files
            for file_prefix in sensor["file_prefixes"]:
                filtered = list(filter(lambda f: f.startswith(file_prefix), files))
                closest_file_time = min(filtered, key=lambda d: abs(
                    datetime.datetime.fromtimestamp(os.path.getmtime(d)) - timestamp))

                sensor_data.append(closest_file_time)

    elif (sensor['data'] == 'database'):
        query = "SELECT "
        if ('field_name' in sensor['location']):
            query += "%s" % sensor['location']['field_name']
            if ('datediff' in sensor['condition']):
                query += ", %s " % sensor['condition']['datediff']['field_name']
        else:
            query += "* "

        query += "FROM %s " % sensor['specification']

        if ('field' in sensor['condition']):
            query += "WHERE %s = %a " % (
                sensor['condition']['field']['field_name'], sensor['condition']['field']['value'])

        if ('datediff' in sensor['condition']):
            if ("WHERE" in query):
                query += "AND "
            else:
                query += "WHERE "
            date_field = sensor['condition']['datediff']['field_name']
            date_val = timestamp + datetime.timedelta(0, 2, -timestamp.microsecond)
            query += "%s < '%s' ORDER BY %s DESC" % (date_field, date_val, date_field)

        query += " LIMIT 10"

        previous = None
        logging.debug("Execute query %a" % query)
        for row in execute_query(db, query):
            row_time = list(row.values())[1]
            if (row_time <= timestamp):
                if (previous is not None and abs(timestamp - list(previous.values())[1]).total_seconds() < abs(
                        row_time - timestamp).total_seconds()):
                    row = previous

                if ('field_no' in sensor['location']):
                    sensor_data.append(
                        dict(list(row.items())[
                             sensor['location']['field_no']['start']:sensor['location']['field_no']['end']]))
                else:
                    sensor_data.append(list(row.items())[0][1])
                break
            previous = row

    else:
        logging.critical("Unknown sensor data type %s" % sensor['data'])
        raise ValueError("Unknown sensor data type %s" % sensor['data'])
    logging.info("Got data for sensor '%s' in %s seconds" % (sensor['name'], str(time.time() - start)))

    if len(sensor_data) == 1:
        return sensor_data[0]
    else:
        return sensor_data


# ========
# = Main =
# ========

def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s.%(msecs)d] [%(levelname)s] %(message)s', '%H:%M:%S')

    # set console handler settings
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    if (not os.path.isdir("log")):
        os.mkdir("log")

    # set file handler settings
    fh = logging.FileHandler("log/log.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)


if (__name__ == "__main__"):
    init_logger()
    now = time.time()
    logging.debug("Application started")

    # create mysql connection
    try:
        db = mysql.connector.connect(**mysql_config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.critical("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.critical("Database does not exist")
        else:
            logging.critical(err)
    else:
        logging.info("Connected to mysql at %s@%s:%s/%s" % (
            mysql_config["user"], mysql_config["host"], mysql_config["port"], mysql_config['database']
        ))

        # Get the speeds from the database
        all_v = get_speeds(db)

        # TODO probably adapt lengths in get_time_offset_multiple_lines so we don't need the line underneath?
        lengths = [f["length"] for f in lines["lines"]]

        # optimize file names of analyser
        reformat_analyser(analyser)

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # !! important stuff happens here !!
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        sensor_data = {}
        for sensor in sensors:

            # get time offset
            time_offset = None
            while time_offset is None:
                try:
                    time_offset = get_time_offset_multiple_lines(sensor['position'], current_position, lengths,
                                                                 all_v)
                except IndexError as e:
                    if (fetch_limit == max_fetch_limit):
                        logging.critical("Max fetch limit reached. Not enough speed data")
                        raise ValueError("Max fetch limit reached. Not enough speed data")
                    logging.debug("Not enough data in %d entries, increasing fetch_limit" % fetch_limit)
                    fetch_limit += fetch_limit_increase_rate
                    if (fetch_limit > max_fetch_limit):
                        fetch_limit = max_fetch_limit
                    all_v = get_speeds(db)

            time_of_sensor_capture = all_v[0][0]["datetime"] - time_offset
            sensor_data[sensor['name']] = get_sensor_data(time_of_sensor_capture, sensor, db)
        db.close()

        logging.info("Program finished in %s seconds" % str(time.time() - now))
