from src.BaseDataTable import BaseDataTable
from src.DerivedDataTable import DerivedDataTable
from src.RDBKeyInfo import RDBKeyInfo

import pandas as pd
import pymysql

import logging

logger = logging.getLogger()


class RDBDataTable(BaseDataTable):
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    _default_connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuserdbuser',
        'db': 'newbook',
        'port': 3306
    }

    _default_cnx = None

    @classmethod
    def _get_default_cnx(cls):

        if RDBDataTable._default_cnx is None:
            # Create a connection to use inside this object. In general, this is not the right approach.
            # There would be a connection pool shared across many classes and applications.
            cls._default_cnx = pymysql.connect(
                host=cls._default_connect_info['host'],
                user=cls._default_connect_info['user'],
                password=cls._default_connect_info['password'],
                db=cls._default_connect_info['db'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)

        return cls._default_cnx

    def __init__(self, table_name, key_columns=None, connect_info=None):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
            This is for compatibility with other types of data table. Any value other than None is an error.
        """

        # If there is not explicit connect information, use the defaults.
        pass


    def __str__(self):
        """

        :return: String representation of the table's metadata.
        """
        pass

    @classmethod
    def _run_q(cls, q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """

        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = cls._get_default_cnx()

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        #self.debug_message("Query = ", cursor.mogrify(q, args))

        cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            r = None

        if commit:                  # Do not worry about this for now.
            cnx.commit()

        return r

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=True):
        """

        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        try:
            q = "insert into " + table_name + " "

            # If the column list is not None, form the (col1, col2, ...) part of the statement.
            if column_list is not None:
                q += "(" + ",".join(column_list) + ") "

            # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
            # each value to insert.
            values = ["%s"] * len(values_list)

            # Form the values(%s, %s, ...) part of the statement.
            values = " ( " + ",".join(values) + ") "
            values = "values" + values

            # Put all together.
            q += values

            self._run_q(q, args=values_list, fields=None, fetch=False, cnx=cnx, commit=commit)

        except Exception as e:
            print("Got exception = ", e)
            raise e

    @classmethod
    def get_folders(cls):
        pass

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """
        pass

    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        w_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            w_clause = "WHERE " + " AND ".join(terms)
        else:
            w_clause = ""
            args = None

        return w_clause, args

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        pass


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        pass

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    def load(self):
        pass

    def save(self):
        pass

    def query(self, query_statement, args, context=None):
        pass



