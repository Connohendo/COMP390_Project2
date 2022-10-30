import functions
import requests
import json
import sqlite3

bound_box_dict = {
    'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
    'Europe_Meteorites': (-24.1, 36, 32, 71.1),
    'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
    'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
    'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
    'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
    'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
}


def main():
    # run a GET request
    response = requests.get('https://data.nasa.gov/resource/gh4g-9sfh.json')

    # convert response text to json format (i.e. list of dictionaries)
    # (the json() decoder function only works if the text is formatted correctly)
    json_data = response.json()

    # connect to database
    db_connection = None
    try:
        # connect to a sqlite database - create it if it does not exist
        db_connection = sqlite3.connect('meteorite_db.db')
        # create a cursor object - this cursor object will be used for all operations pertaining to the database
        db_cursor = db_connection.cursor()

        # create a table in the database to store ALL the meteorite data (if it does not already exist)
        # the parentheses following the table name ("meteorite_data") contains a list of column names
        # and the data type of values that will be inserted into those columns
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS meteorite_data(
                                name TEXT,
                                id INTEGER,
                                nametype TEXT,
                                recclass TEXT,
                                mass TEXT,
                                fall TEXT,
                                year TEXT,
                                reclat TEXT,
                                reclong TEXT,
                                geolocation TEXT,
                                states TEXT,
                                counties TEXT);''')

        # clear the 'meteorite_data' table if it already contains data from last time the program was run
        db_cursor.execute('DELETE FROM meteorite_data')

        # read all the data from the specified json URL and insert it into the database:
        # loop through each dictionary entry in the JSON list
        for record in json_data:
            if functions.check_geoloc(record):
                # some keys may not exist because there is no value for that record - use <dict>.get()
                # to INSERT 'None' if the key is not found
                db_cursor.execute('''INSERT INTO meteorite_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                  (record.get('name', None),
                                   int(record.get('id', None)),
                                   record.get('nametype', None),
                                   record.get('recclass', None),
                                   record.get('mass', None),
                                   record.get('fall', None),
                                   record.get('year', None),
                                   record.get('reclat', None),
                                   record.get('reclong', None),
                                   functions.convert_obj_to_string(record, 'geolocation'),  # convert geolocation <dict> to string
                                   record.get(':@computed_region_cbhk_fwbd', None),
                                   record.get(':@computed_region_nnqa_25f4', None)))

            # run a SELECT query on the table that holds all meteorite data
        for key, value in bound_box_dict.items():
            db_cursor.execute(f'''
                                SELECT name, mass, reclat, reclong FROM meteorite_data 
                                WHERE CAST(reclong AS FLOAT) BETWEEN {value[0]} AND {value[2]} 
                                AND CAST(reclat AS FLOAT) BETWEEN {value[1]} AND {value[3]}
                              ''')
        # # get the result of the query as a list of tuples
            q1_result = db_cursor.fetchall()

        # create a table in the database to store the filtered data (if it does not already exist)
        # this table will hold the resulting rows of our SELECT query
            db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {key}(
                                                   name TEXT,
                                                   id INTEGER,
                                                   nametype TEXT,
                                                   recclass TEXT,
                                                   mass TEXT,
                                                   fall TEXT,
                                                   year TEXT,
                                                   reclat TEXT,
                                                   reclong TEXT,
                                                   geolocation TEXT,
                                                   states TEXT,
                                                   counties TEXT);''')

            # clear the 'filtered_data' table if it already contains data from last time the program was run
            db_cursor.execute(f'DELETE FROM {key}')

        # fill the filtered table
            for tuple_entry in q1_result:
                db_cursor.execute(f'''INSERT INTO {key} VALUES(?, ?, ?, ?)''', tuple_entry)

        # functions.create_tables(bound_box_dict, db_cursor)
        # functions.loop_through_jsondata(json_data, bound_box_dict, db_cursor)

        # commit all changes made to the database
        db_connection.commit()
        db_cursor.close()

    # catch any database errors
    except sqlite3.Error as db_error:
        # print the error description
        print(f'A Database Error has occurred: {db_error}')

    # 'finally' blocks are useful when behavior in the try/except blocks is not predictable
    # The 'finally' block will run regardless of what happens in the try/except blocks.
    finally:
        # close the database connection whether an error happened or not (if a connection exists)
        if db_connection:
            db_connection.close()
            print('Database connection closed.')


if __name__ == '__main__':
    main()
