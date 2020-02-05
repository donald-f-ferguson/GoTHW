
# Import package to enable defining abstract classes in Python.
# Do not worry about understanding abstract base classes. This is just a class that defines
# some methods that subclasses must implement.
from abc import ABC, abstractmethod


class DataTableException(Exception):
    """
    A simple class that maps underlying implementation exceptions to generic exceptions.
    """

    invalid_method = 1001

    # General
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        result = (
            type(self), {"code": self.code, "message": self.message}
        )
        result = str(result)
        return result


class BaseDataTable(ABC):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """

    def __init__(self, entity_type_name, connect_info, key_columns=None, context=None):
        """

        :param entity_type_name: Name of the logcal entity type. This maps to various abstractions in
            underlying stores, e.g. file names, RDB tables, Neo4j Labels, ...
        :param connect_info: Dictionary of parameters necessary to connect to the data. See examples in subclasses.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
            A primary key is a set of columns whose values are unique and uniquely identify a row. For Appearances,
            the columns are ['playerID', 'teamID', 'yearID']
        :param contex: Holds context and environment information.
        """
        self._table_name = entity_type_name
        self._connect_info = connect_info
        self._key_columns = key_columns
        self._context = context

    @abstractmethod
    def find_by_primary_key(self, key_fields, field_list=None, context=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """
        pass

    @abstractmethod
    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, context=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}. The function will return
            a derived table containing the rows that match the template.
        :param field_list: A list of requested fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A derived table containing the computed rows.
        """
        pass

    @abstractmethod
    def insert(self, new_entity, context=None):
        """

        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        pass

    @abstractmethod
    def delete_by_template(self, template, context=None):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    @abstractmethod
    def delete_by_key(self, key_fields, Context=None):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        pass

    @abstractmethod
    def update_by_template(self, template, new_values, context=None):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    @abstractmethod
    def update_by_key(self, key_fields, new_values, context=None):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        pass

    @abstractmethod
    def query(self, query_statement, args, context=None):
        """
        Passed through/executes a raw query in the native implementation language of the backend.
        :param query_statement: Query statement as a string.
        :param args: Args to insert into query if it is a template
        :param context:
        :return: A JSON object containing the result of the operation.
        """
        pass

    @abstractmethod
    def load(self, rows=None):
        """
        Loads data into the data table.
        :param rows:
        :return: Number of rows loaded.
        """

    @abstractmethod
    def save(self, context):
        """
        Writes any cached data to a backing store.
        :param context:
        :return:
        """
