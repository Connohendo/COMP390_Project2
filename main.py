
import requests
import json
import sqlite3


# this function takes two parameters: a dictionary object and a target key
def convert_obj_to_string(dict_record, key):
    # <dict>.get() gets the value associated with a key in a dictionary
    # the second parameter for <dict>.get() tells the get function to return 'None' if the key does not exist
    if dict_record.get(key, None) is None:
        return None
    # return a string version of the <dict>[key] object if a key is present
    return json.dumps(dict_record[key])


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
                               convert_obj_to_string(record, 'geolocation'),    # convert geolocation <dict> to string
                               record.get(':@computed_region_cbhk_fwbd', None),
                               record.get(':@computed_region_nnqa_25f4', None)))

        # run a SELECT query on the table that holds all meteorite data
        db_cursor.execute('SELECT * FROM meteorite_data WHERE id <= 1000')
        # get the result of the query as a list of tuples
        q1_result = db_cursor.fetchall()

        # create a table in the database to store the filtered data (if it does not already exist)
        # this table will hold the resulting rows of our SELECT query
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS filtered_data(
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
        db_cursor.execute('DELETE FROM filtered_data')

        # fill the filtered table
        for tuple_entry in q1_result:
            db_cursor.execute('''INSERT INTO filtered_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple_entry)

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