
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

def parse_data_into_lists(data):
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
    
    for row in rows:
        values.append(
            [l.strip() for l in row.split('|')]
            )
    
    return values

def create_rows(cursor, table_name, data):    
    headers = parse_headers_into_list(data)
    values = parse_data_into_lists(data)
    
    # build the CQL and execute it
    statements = []
    
    for valueset in values:
        statements.append(
            'INSERT INTO '+ table_name + ' (' + ', '.join(headers) + ') ' + 'VALUES (' + ', '.join(valueset) + ')'
            )
    
    for stmt in statements:
        cursor.execute(stmt)

def inner_quotify(string):
    # takes a string like 'foo' and turns it into "'foo'"
    # to simplify comparison because parse_data_into_lists
    # makes a string like "'foo'" but data coming from the db
    # will look like 'foo'
    return "'{val}'".format(val=string)

def flatten_into_set(iterable):
    # flattens a nested list like: [[1, one, bananas], [2, two, oranges]]
    # into: set([1__one__bananas, 2__two__oranges]) (all elements cast as str)
    # why? so the set can be used in set comparisons
    flattened = []
    
    for sublist in iterable:
        items = [str(item) for item in sublist]
        flattened.append('__'.join(items))
    
    return set(flattened)