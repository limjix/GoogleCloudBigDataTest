#https://cloud.google.com/bigtable/docs/reading-data
#https://cloud.google.com/bigtable/docs/reads

import argparse
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
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

    #Create Column Family
    print('Creating column family cf1 with Max Version GC rule...')
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))


    #Read the csv & Create row inserts
    bigtablerows = []
    with open('A0852.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for csvrow in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(csvrow)}')
                line_count += 1
            else:
                print(csvrow[0],csvrow[1], csvrow[20])
                #Create the rowkey
                row_key = 'sensor#{}'.format(csvrow[0]+csvrow[1]).encode()
                bigtablerow = table.direct_row(row_key)
                column = "contactforce".encode()
                value = csvrow[20]
                bigtablerow.set_cell(column_family_id,
                             column,
                             value,
                             timestamp=datetime.datetime.utcnow())
                print(bigtablerow.row_key)
                bigtablerows.append(bigtablerow)

    #Insert into the table
    table.mutate_rows(bigtablerows)

    #Read
    ### Single Read - I specify exact row key
    '''
    print('Single Read')
    key = 'sensor#2020-07-2920:13:21'.encode()
    row = table.read_row(key)
    cell = row.cells[column_family_id][column][0]
    print("Cells", cell.value.decode('utf-8'))
    '''

    ### Partial Row - IE USE FILTERS TO FILTER THE COLUMNS
    '''
    print('Partial Row')
    key = 'sensor#2020-07-2920:13:21'.encode()

    col_filter = row_filters.ColumnQualifierRegexFilter(b'contactforce')
    row = table.read_row(key, filter_=col_filter)
    cell = row.cells[column_family_id][column][0]
    print("Cells", cell.value.decode('utf-8'))
    '''
    ### Reading Multiple Rows
    '''
    print("MULTIPLE ROWS")
    row_set = RowSet()
    row_set.add_row_key(b"sensor#2020-07-2920:11:30")
    row_set.add_row_key(b"sensor#2020-07-2920:13:10")

    rows = table.read_rows(row_set=row_set)
    for row in rows:
        print(row)
        print(row.cells[column_family_id][column][0])
    '''

    ### Reading a range of rows
    '''
    print("GETTING RANGE OF ROWS")
    row_set = RowSet()
    row_set.add_row_range_from_keys(
        start_key=b"sensor#2020-07-2920:11:30",
        end_key=b"sensor#2020-07-2920:13:10")

    rows = table.read_rows(row_set=row_set)
    for row in rows:
        print(row.row_key)
    '''

    ### Reading multiple ranges of rows
    '''
    print("GETTING MULTIPLE RANGE OF ROWS")
    row_set = RowSet()
    row_set.add_row_range_from_keys(
        start_key=b"sensor#2020-07-2919:58:13",
        end_key=b"sensor#2020-07-2919:58:55")
    row_set.add_row_range_from_keys(
        start_key=b"sensor#2020-07-2920:11:30",
        end_key=b"sensor#2020-07-2920:11:45")

    rows = table.read_rows(row_set=row_set)
    for row in rows:
        print(row.row_key)
    '''

    ### Reading multiple rows using row key prefix
    '''
    print("READING multiple rows using row key prefix")
    prefix = "phone#"
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    print(end_key)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"),
                                    end_key.encode("utf-8"))

    rows = table.read_rows(row_set=row_set)
    for row in rows:
        print(row)
    '''

    ### Reading With Filters - Go to next filter py file


    #Cleanup - Delete
    print('Deleting the {} table.'.format(table_id))
    table.delete()



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