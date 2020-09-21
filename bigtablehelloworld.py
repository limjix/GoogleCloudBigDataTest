"""Demonstrates how to connect to Cloud Bigtable and run some basic operations.

Prerequisites:

- Create a Cloud Bigtable cluster.
  https://cloud.google.com/bigtable/docs/creating-cluster
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

import argparse
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.auth.credentials import AnonymousCredentials
import os

os.environ["BIGTABLE_EMULATOR_HOST"] = 'localhost:8086'
def main():
    client = bigtable.Client(project='IGNORE_ENVIRONMENT_PROJECT',
                    credentials=AnonymousCredentials(),
                    admin=True)
    instance = client.instance("fake_instance")
    table_id = "some_table"
    table = instance.table(table_id)

    print('Creating column family cf1 with Max Version GC rule...')
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))

    print('Writing some greetings to the table.')
    greetings = ['Hello World!', 'Hello Cloud Bigtable!', 'Hello Python!']
    rows = []
    column = 'greeting'.encode()
    for i, value in enumerate(greetings):
        row_key = 'greeting{}'.format(i).encode()
        row = table.direct_row(row_key)
        row.set_cell(column_family_id,
                     column,
                     value,
                     timestamp=datetime.datetime.utcnow())
        rows.append(row)
    table.mutate_rows(rows)

    row_filter = row_filters.CellsColumnLimitFilter(1)

    print('Getting a single greeting by row key.')
    key = 'greeting0'.encode()

    row = table.read_row(key, row_filter)
    cell = row.cells[column_family_id][column][0]
    print(cell.value.decode('utf-8'))

    print('Scanning for all greetings:')
    partial_rows = table.read_rows(filter_=row_filter)

    for row in partial_rows:
        cell = row.cells[column_family_id][column][0]
        print(cell.value.decode('utf-8'))

    print('Deleting the {} table.'.format(table_id))
    table.delete()



if __name__ == '__main__':
    main()