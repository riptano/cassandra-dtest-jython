import re

def strip(val):
    # remove spaces and pipes from beginning/end
    return val.strip().strip('|')

def parse_headers_into_list(data):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))
    
    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)
    
    # separate headers from actual data and remove extra spaces from them
    headers = [h.strip() for h in rows.pop(0).split('|')]
    return headers

def get_row_multiplier(row):
    # find prefix like *1234 meaning create 1,234 rows
    row_cells = [l.strip() for l in row.split('|')]
    m = re.findall('\*(\d+)$', row_cells[0])

    if m:
        return int(m[0])

    return None

def row_has_multiplier(row):
    if get_row_multiplier(row) != None:
        return True
        
    return False

def parse_row_into_list(row, format_funcs=None):
    row_cells = [l.strip() for l in row.split('|')]
    
    if row_has_multiplier(row):
        row_multiplier = get_row_multiplier(row)
        row = '|'.join(row_cells[1:]) # cram remainder of row back into foo|bar format
        multirows = []
        
        for i in range(row_multiplier):
            multirows.append(
                parse_row_into_list(row, format_funcs=format_funcs)
                )
        return multirows

    if format_funcs:
        return [format_funcs[idx](cell) for idx, cell in enumerate(row_cells)]
    else:
        return [l.strip() for l in row_cells]

def parse_data_into_lists(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))
    
    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)
    
    # remove headers
    rows.pop(0)
    
    values = []
    
    for row in rows:
        if row_has_multiplier(row):
            values.extend(parse_row_into_list(row, format_funcs=format_funcs))
        else:
            values.append(parse_row_into_list(row, format_funcs=format_funcs))
    
    return values

def create_rows(cursor, table_name, data, format_funcs=None):
    """
    Creates db rows using given cursor, with table name provided,
    using data formatted like:
    
    |colname1|colname2|
    |value2  |value2  |
    
    format_funcs is a list of functions to call to format each column
    first function used for column1, second function used for column2...
    returns the formatted data as it would have been sent to the db.
    """
    headers = parse_headers_into_list(data)
    values = parse_data_into_lists(data, format_funcs=format_funcs)
    
    # build the CQL and execute it
    statements = []
    
    for valueset in values:
        statements.append(
            'INSERT INTO '+ table_name + ' (' + ', '.join(headers) + ') ' + 'VALUES (' + ', '.join(valueset) + ')'
            )
    
    for stmt in statements:
        cursor.execute(stmt)
    
    return values

def cql_str(val):
    """
    Changes "val" to "'val'", so the inner values
    can easily be used in cql.
    """
    return "'{replace}'".format(replace=val)

def flatten_into_set(iterable):
    # flattens a nested list like: [[1, one, bananas], [2, two, oranges]]
    # into: set([1__one__bananas, 2__two__oranges]) (all elements cast as str)
    # why? so the set can be used in set comparisons
    flattened = []
    
    for sublist in iterable:
        items = [str(item) for item in sublist]
        flattened.append('__'.join(items))
    
    return set(flattened)