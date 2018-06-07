# Sensordata synchronizer

## Configuration
The program needs to be configured in the first place.
The variables are used as described:
```python=
file_root_path  # the path the destinated sensor data is in (optional if the location of the sensor configuration is a full path)
current_position  # the position in meters where the data should be accassed (mostly the end of the last line)

mysql_config  # the configuration of the mysql database (only read accass required)
fetch_limit  # the maximum amount of rows to be fetched (only important to get the speeds of the lines from the database -> shouldn't be too small)
fetch_limit_increase_rate  # in case the speed data gathered from the sql server isn't enough with the set fetch limit this variable is used to increase the fetch rate
max_fetch_limit  # the maximum feth limit. It won't be increased anymore if it has been reached

#######################
# Sensorconfiguration #
#######################

image_sensor = {
    "name": "sensor",  # sensorname
    "position": 0.0,  # position of the sensor on the lines in meters (absolute position, not relative to current line)
    "data": "file",  # the type of the data storage (file|database)
    "specification": "image",  # the specification of the type (image|csv|[database_table name])
    "location": file_root_path + "kinect/",  # the path to the data (path|column_numbers|table_field name)
    "file_prefixes": [  # the prefixes of the files to seperate them (list)
        "color",
        "depth"
    ]
}

csv_sensor = {
    "name": "x102",
    "position": 0.6,
    "data": "file",
    "specification": "csv",
    "location": file_root_path + "udp_x102/2018_05_24/",
    "file_templates": [  # the template in what the files should be renamed to (date parameters are required to directly access the correct file)
        "kipro-analyser-data_%(year)s-%(month)s-%(day)s_%(hour)s-%(minute)s-%(second)s.%(millisecond)s.csv"
    ]
}

database_sensor_1 = {
    "name": "x101",
    "position": 0.6,
    "data": 'database',
    'specification': 'data_analyser_x101_daten',  # the table in the database
    'location': {
        'field_no': {  # take all data from the following rows
            'start': 4,  # start row (inclusive)
            'end': 19  # last row (exclusive)
        }
    },
    'condition': {  # the condition to select the correct data
        'datediff': {  # datediff means the data with the lowest datediff is taken
            'field_name': 'time'  # the name of the field to compare the date with
        }
    }
}

database_sensor_2 = {
    "name": "Bandwaage",
    "position": 7.6,
    "data": "database",
    "specification": "opc_data",
    "location": {
        "field_name": "ITEM_VALUE",  # the field name the data is saved in
    },
    "condition": {
        'datediff': {
            'field_name': "READ_TIME",  # datediff condition
        },
        'field': {  # specific field condition
            'field_name': 'ITEM_NAME',  # the field
            'value': 'ET 200SP-Station_1.ET200SP.OPC_DATA.Messwerte.BANDWAAGE'  # the value the field must have
        }
    }
}

sensors = [image_sensor, csv_sensor, ...]  # list with all sensors

######################
# line configuration #
######################

knickband = {
    "name": "Knickband",
    "database_name": "Knickband",  # the name suffix of the database table value the line has (see 'lines' variable)
    "id": 0,  # the id (should be ascending)
    "length": 4.2,  # the length of the line im meters
    "speed_factor": 1 / 3800 # the speedfactor of the frequency converter number from the database

}
zufuehrband = {
    "name": "Zuführband",
    "database_name": "Aufgabebunker",
    "id": 1,
    "length": 6.8,
    "speed_factor": 1 / 3800
}
lines = {
    "selector_template": "ET 200SP-Station_1.ET200SP.OPC_DATA.Foerderbaender.%(line_name)s.%(value_name)s",  # the database template for data for the lines (parameters are required)
    "lines": [knickband, zufuehrband]  # the list of all lines
}
```

## Usage
```python
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
```

## Functionality
The tool is adjusted to the current sensors. Minor optimizations might be needed in case a new sensor should be added.
In case of an error there is a logfile located in the log directory with additional debug information.