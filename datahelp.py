
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

def parse_data_into_lists(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))
    
    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)
    
    # remove headers
    rows.pop(0)
    
    # for the actual data values, remove extra spaces and format into list of lists
    # [[1, 'foo'], [2, 'bar']]
    values = []
    
    if format_funcs:
        for row in rows:
            values.append(
                # call appropriate formatters for data found in columns
                [format_funcs[idx](l.strip()) for idx, l in enumerate(row.split('|'))]
                )
    else:
        for row in rows:
            values.append(
                [l.strip() for l in row.split('|')]
                )
    
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
    
    from pprint import pprint
    pprint(values)
    pprint(statements)
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