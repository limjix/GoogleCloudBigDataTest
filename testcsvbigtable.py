import argparse
import datetime

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

    #Read from the table
    print('Getting a single data by row key.')
    key = 'sensor#2020-07-2920:13:21'.encode()

    row_filter = row_filters.CellsColumnLimitFilter(1)
    row = table.read_row(key, row_filter)

    #Breaking down the row object
    ## Print the contents
    print(dir(row))

    ### Changing to dictionary and deconstructing
    rowdict = row.to_dict()
    print("to_dict()", rowdict)
    print("to_dict()", rowdict[b'cf1:contactforce'])
    print("to_dict()", rowdict[b'cf1:contactforce'][0])

    ### cell_values
    print("Cell VALUES", row.cell_values('cf1','contactforce')) #Outputs a generator
    for item in row.cell_values('cf1',b'contactforce'):
        print( "Cell VALUES", item)

    ### find_cells
    print("find_cells", row.find_cells('cf1',b'contactforce'))

    ### row_key
    print("row_key", row.row_key)

    ### cells - example read
    cell = row.cells[column_family_id][column][0]
    print("Cells", cell.value.decode('utf-8'))
    print("Cells items", row.cells.items())

    #Cleanup - Delete
    print('Deleting the {} table.'.format(table_id))
    table.delete()

if __name__ == '__main__':
    main()