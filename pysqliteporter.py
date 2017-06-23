"""Python SQLite Porter main definitions
"""

import json
import re


class PySqlitePorter:

    _DEFAULT_BATCH_INSERT_SIZE = 250

    _separator = ';\n'

    _statement_regex = re.compile('(?!\s|;|$)(?:[^;"\']*(?:"(?:\\.|[^\\"])*"|\'(?:\\.|[^\\\'])*\')?)*')

    def _sql_escape(self, value):

        if re.match('[_-]+', value):
            value = '`' + value + '`';

        return value

    def import_json_to_db(self, db, json_content, kwargs={}):

        main_sql = ''
        create_index_sql = ''

        if type(json_content) == 'string':
            json_content = json.load(json_content)

        if 'structure' in json_content.keys():

            for table_name in json_content['structure']['tables']:

                main_sql = ''.join([
                    'DROP TABLE IF EXISTS ',
                    self._sql_escape(table_name),
                    self._separator,
                    'CREATE TABLE ',
                    self._sql_escape(table_name),
                    json['structure']['tables'][table_name],
                    self._separator
                ])

            for other_sql in json_content['structure']['otherSQL']:

                if re.match('CREATE INDEX', other_sql):
                    create_index_sql = ''.join([
                        other_sql,
                        self._separator
                    ])
                else:
                    main_sql = ''.join([
                        main_sql,
                        other_sql,
                        self._separator
                    ])

        if 'batchInsertSize' in kwargs.keys():
            batch_insert_size = kwargs['batchInsertSize']
        else:
            batch_insert_size = self._DEFAULT_BATCH_INSERT_SIZE

        if 'insert' in json_content['data'].keys():

            for table_name in json_content['data']['inserts']:

                count = 0

                for row in json_content['data']['inserts'][table_name]:

                    if count == batch_insert_size:
                        main_sql = ''.join([
                            main_sql,
                            self._separator
                        ])

                    fields = []
                    values = []

                    for column, v_data in row:
                        fields.append(column)
                        values.append(
                            sanitise_for_sql(v_data)
                        )

                    # TODO
                    # implement/rewrite sanitise_for_sql method
                    # continue rewrite JSON export method