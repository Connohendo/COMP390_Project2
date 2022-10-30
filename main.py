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

        functions.create_tables(bound_box_dict, db_cursor)
        functions.loop_through_jsondata(json_data, bound_box_dict, db_cursor)

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
