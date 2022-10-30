import json
import sqlite3
import main


def check_geoloc(meteor):
    if meteor.get('reclat') is None:
        return False
    if meteor.get('reclong') is None:
        return False
    return True


def loop_through_jsondata(json_data, bound_box_dict, db_cursor):
    for record in json_data:
        if check_geoloc(record):
            for key, value in bound_box_dict.items():
                if value[1] <= convert_string_to_numerical(record.get('reclat')) <= value[3] and value[
                    0] <= convert_string_to_numerical(record.get('reclong')) \
                        <= value[2]:
                    db_cursor.execute(f'INSERT INTO {key} VALUES(?, ?, ?, ?)',
                                      (record.get('name', None),
                                       record.get('mass', None),
                                       record.get('reclat', None),
                                       record.get('reclong', None)))


def create_tables(bound_box_dict, db_cursor):
    for key in bound_box_dict:
        create_table = f'CREATE TABLE IF NOT EXISTS {key}(name TEXT, mass TEXT, reclat TEXT, reclong TEXT);'
        db_cursor.execute(create_table)
        db_cursor.execute(f'DELETE FROM {key}')

# The following 3 functions were created by Professor Matta at Bridgewater State University


def _string_is_int(in_string):
    """ returns True if the incoming parameter is an int, returns False otherwise """
    try:
        int(in_string)
        return True
    except TypeError:
        return False
    except ValueError:
        return False


def _string_is_float(in_string):
    """ returns True if the incoming parameter is a float, returns False otherwise """
    try:
        float(in_string)
        return True
    except TypeError:
        return False
    except ValueError:
        return False


def convert_string_to_numerical(in_string):
    """ this function converts a string to a numerical value (to either an int or float)
        'None' is returned if the incoming string is not in the form of an int or float """
    if _string_is_int(in_string):
        return int(in_string)
    elif _string_is_float(in_string):
        return float(in_string)
    return None


# this function takes two parameters: a dictionary object and a target key
def convert_obj_to_string(dict_record, key):
    # <dict>.get() gets the value associated with a key in a dictionary
    # the second parameter for <dict>.get() tells the get function to return 'None' if the key does not exist
    if dict_record.get(key, None) is None:
        return None
    # return a string version of the <dict>[key] object if a key is present
    return json.dumps(dict_record[key])
