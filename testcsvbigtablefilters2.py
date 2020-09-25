### https://cloud.google.com/bigtable/docs/filters
### https://cloud.google.com/bigtable/docs/using-filters

import random
import re
import argparse
import datetime
import time
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.auth.credentials import AnonymousCredentials
import os
import csv
os.environ["BIGTABLE_EMULATOR_HOST"] = 'localhost:8086'
def main():
    client = bigtable.Client(project='IGNORE_ENVIRONMENT_PROJECT',
                    credentials=AnonymousCredentials(),
                    admin=True)
    instance = client.instance("fake_instance")
    table_id = "test_table"
    table = instance.table(table_id)
    print("START")
    #Filter
    start = datetime.datetime(2020,9,22,12)
    end = datetime.datetime(2020,9,22,)
    rows = table.read_rows(
        filter_=row_filters.TimestampRangeFilter(
            row_filters.TimestampRange(start=start, end=end)))
    for row in rows:
        print_row(row)

    # rows = table.read_rows(filter_=row_filters.CellsColumnLimitFilter(1))
    # for row in rows:
    #     print_row(row)

def print_row(row):
    #Example of how to deconstruct 1 row
    print("Reading data for {}:".format(row.row_key.decode('utf-8')))
    for cf, cols in sorted(row.cells.items()): # First deconstruct the row.cells.items() to get CF and cols
        print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()): # Then deconstuct the col.items() to get the cells
            for cell in cells: #For every cell
                labels = " [{}]".format(",".join(cell.labels)) \
                    if len(cell.labels) else ""
                print(
                    "\t{}: {} @{}{}".format(col.decode('utf-8'),
                                            cell.value.decode('utf-8'),
                                            cell.timestamp, labels))
    print("")

if __name__ == '__main__':
    main()