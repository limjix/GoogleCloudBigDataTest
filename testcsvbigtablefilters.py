### https://cloud.google.com/bigtable/docs/filters
### https://cloud.google.com/bigtable/docs/using-filters

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
    time.sleep(1)
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
                editedtimestamp = re.sub('[^A-Za-z0-9]+', '', csvrow[0]+csvrow[1])
                row_key = 'sensor#{}'.format(editedtimestamp).encode()
                bigtablerow = table.direct_row(row_key)
                bigtablerow.set_cell(column_family_id,
                             "column1",
                             #str(float(csvrow[20])+i),
                             csvrow[20],
                             timestamp=datetime.datetime.utcnow())
                bigtablerow.set_cell(column_family_id,
                             "column2",
                             #str(float(csvrow[25])+i),
                             csvrow[25],
                             timestamp=datetime.datetime.utcnow())
                bigtablerow.set_cell(column_family_id,
                             "column3",
                             #str(float(csvrow[16])+i),
                             csvrow[16],
                             timestamp=datetime.datetime.utcnow())
                print(bigtablerow.row_key)
                bigtablerows.append(bigtablerow)
                #time.sleep(0.01) #_FOR TIMESTAMPRANGE

    #Insert into the table
    table.mutate_rows(bigtablerows)

    # LIMITING FILTERS
    ## ROW SELECTION FILTERS
    ### Row Sample - retrieve randm sample of rows within a given range , ie downsample the data
    '''
    print("ROW SAMPLE")
    rows = table.read_rows(filter_=row_filters.RowSampleFilter(.01))
    for row in rows:
        print_row(row)
    '''

    ### Row Key Regex - filter cheks whether the input row key matches a regular expression, if match, output row is = to input row. Else Output row empty
    '''
    must use RE2 syntax https://github.com/google/re2/wiki/Syntax
    Data set here https://cloud.google.com/bigtable/docs/using-filters
    Understanding this Regex : .*#20190501$
    the "." means "any character possibly including newline
    the "*" means repetition. So .* means many random characters
    the "$" means at end of text or line, so the date matches that
    
    So use this if you are searching for some types of row keys
    '''
    # print("ROW KEY REGEX")
    # rows = table.read_rows(
    #     filter_=row_filters.RowKeyRegexFilter(".*#.*20:13:21$".encode("utf-8")))
    # for row in rows:
    #     print(row.row_key)


    ### Cell Per Column Limit (Needa check and retest)
    '''
    This filter limits the number of cells in each column that are included in the output row. 
    When this filter is applied, each output row includes the N most recent cells from each 
    column and omits all other cells from that column.

    Doesn't work, don't know why.
    '''
    # rows = table.read_rows(filter_=row_filters.CellsColumnLimitFilter(10))
    # for row in rows:
    #     print_row(row)

    ### Cell per Row Limit
    '''
    This filter limits the number of cells in each output row
    When this filter is applied, each output row includes the first N cells from the input row and omits all of the following cells from that row.
    The first N cells are read regardless of which column they are in, in the order in which they are stored.
    A column in a row can contain multiple cells. Each cell contains a value for the column and a unique timestamp.
    As a result, limiting a row to N cells might differ from retrieving the first N columns from the row. 
    For example, if you use a filter with a cell per row limit of 20 to read a row that has 30 columns, and each column has 10 timestamped cells, 
    the output row returns values from only the first two columns in the row (2 * 10 = 20).
    
    NOTE TO SELF:
    So basically, if i have 3 columns in that row, but i set N to be 1, i will only get 1 column
    
    '''

    # rows = table.read_rows(filter_=row_filters.CellsRowLimitFilter(2))
    # for row in rows:
    #     print_row(row)

    ### Cells Per Row Offset
    '''
    Filter omits the first N cells from each row input    
    '''
    # rows = table.read_rows(filter_=row_filters.CellsRowOffsetFilter(2))
    # for row in rows:
    #     print_row(row)

    ### Column Family REGEX
    '''
    Match according to column family
    '''

    # rows = table.read_rows(
    #     filter_=row_filters.FamilyNameRegexFilter("cf.*".encode("utf-8"))) #Finds all families with cf(smtg)
    # for row in rows:
    #     print_row(row)

    ### Column Qualifier Regex
    '''
    Match according to column name
    '''
    # rows = table.read_rows(
    #     filter_=row_filters.ColumnQualifierRegexFilter(
    #         "column3.*".encode("utf-8")))
    # for row in rows:
    #     print_row(row)

    ### Column Range
    '''
    From which column family. And from what column to what column
    '''
    # rows = table.read_rows(
    #     filter_=row_filters.ColumnRangeFilter("cf1",
    #                                           b"column1",
    #                                           b"column3",
    #                                           inclusive_start=False,
    #                                           inclusive_end=False))
    # for row in rows:
    #     print_row(row)

    ### Value range (Needa check and retest)
    '''
    Cell value from what to what
    Start can be empty to get cells before specified end value
    End can be empty to get cells after specified start value
    '''

    rows = table.read_rows(
        filter_=row_filters.ValueRangeFilter(start_value=b'0',end_value=b'10000'))

    for row in rows:
        print_row(row)

    ### Value Regex
    '''
    REGEX on cell value
    '''
    # rows = table.read_rows(
    #     filter_=row_filters.ValueRegexFilter("13.*".encode("utf-8"))) #Find anything that starts with 13(smtg)
    # for row in rows:
    #     print_row(row)

    ### Timestamp Range
    '''
    Timestamp Range - on the cell timestamp k , not on the rowkey
    Start is inclusive
    End is exclusive
    '''
    # start = datetime.datetime(2020, 9, 20, 1,54,00)
    # end = datetime.datetime(2020, 9, 20, 1,57,00)
    # print(start, end)
    # rows = table.read_rows(
    #     filter_=row_filters.TimestampRangeFilter(
    #         row_filters.TimestampRange(start=start, end=end)))
    # for row in rows:
    #     print_row(row)

    ## Advance single filters
    ### Block all
    '''
    Removes all of the cells from output row
    '''
    # rows = table.read_rows(filter_=row_filters.BlockAllFilter(False)) #If true then ntg comes up
    # for row in rows:
    #     print_row(row)

    ### Pass all
    '''
    This filter includes all of the input row's cells in the output row. It is equivalent to a read with no filter.
    '''
    # rows = table.read_rows(filter_=row_filters.PassAllFilter(True))
    # for row in rows:
    #     print_row(row)

    ## Modifying Filters
    ### Apply Label
    '''
    Add label to help filters decide if to include or not include
    '''
    # rows = table.read_rows(
    #     filter_=row_filters.ApplyLabelFilter(label="labelled"))
    # for row in rows:
    #     print_row(row)

    ### Strip Value
    '''
    Replace value with empty string so use only when need to count number of rows or cells. Saves egress costs.
    '''
    # rows = table.read_rows(
    #     filter_=row_filters.StripValueTransformerFilter(True))
    # for row in rows:
    #     print_row(row)

    ## Composing filters
    ### Chain RowFilterChain
    '''
    This filter applies a series of filters, in order, to each output row. 
    A chain filter is like using a logical AND.
    Each filter in the chain sees only the output of the previous filter.
    For example, if you chain two filters, and the first filter removes half of the cells from the output row, the second filter does not have access to the cells that were removed.
    In other words, the order of the filters is important. If you change the order of chained filters, you might get different data in your output rows.
    '''

    # rows = table.read_rows(filter_=row_filters.RowFilterChain(
    #     filters=[row_filters.CellsColumnLimitFilter(2),
    #              row_filters.FamilyNameRegexFilter("cf.*")]))
    # for row in rows:
    #     print_row(row)

    ### Interleave RowFilterUnion
    '''
    This filter sends the input row through multiple component filters, generating a temporary output row from each component filter. 
    All of the cells from the temporary output rows are then combined into a final output row. 
    An interleave filter is like using a logical OR.
    '''
    # rows = table.read_rows(filter_=row_filters.RowFilterUnion(
    #     filters=[row_filters.ValueRegexFilter("13.*"),
    #              row_filters.ColumnQualifierRegexFilter("column1")]))
    # for row in rows:
    #     print_row(row)

    ### Condition
    '''
    This filter applies either a true filter or a false filter to the input row.
    '''

    # rows = table.read_rows(filter_=row_filters.ConditionalRowFilter(
    #     base_filter=row_filters.RowFilterChain(filters=[
    #         row_filters.ColumnQualifierRegexFilter(
    #             "column1"),
    #         row_filters.ValueRegexFilter(
    #             "14.*")]),
    #     true_filter=row_filters.ApplyLabelFilter(label="passed-filter"),
    #     false_filter=row_filters.ApplyLabelFilter(label="filtered-out")
    #
    # ))
    # for row in rows:
    #     print_row(row)

    # ---------------- Cleanup - Delete -----------------------------------------------
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