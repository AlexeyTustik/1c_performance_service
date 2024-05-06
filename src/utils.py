DB_FIELDS = {
    'time' : 'DateTime64(6)',
    'duration' : 'Nullable(UInt16)',
    'durationus' : 'Nullable(UInt16)',
    'exception': 'String',
    'dbcopy': 'String',
    'name': "String",
    'procedurename': 'String',
    'modulename': 'String',
    'dumpfile': 'String',
    'cycles': 'String',
    'sdbl' : 'String',
    'rows': "Nullable(UInt64)",
    'func': 'String',
    'rowsaffected': 'Nullable(UInt64)',
    'dbms': 'String',
    'dbpid': 'String',
    'osexception': 'String',
    't_computername': 'String',
    'context': 'String',
    'sessionid': 'Nullable(UInt64)',
    'descr': 'String',
    'dumperror': 'String',
    't_applicationname': 'String',
    't_connectid': 'String',
    'event_name' : 'String',
    'DataBase' : 'String',
    'plansqltext' : 'String',
    'context' : 'String',
    'usr' : 'String',
    'lka' : 'Nullable(Bool)',
    'lkato' : 'String',
    'lkp' : 'Nullable(Bool)',
    'lksrc' : 'Nullable(UInt64)',
    'lkpto' : 'Nullable(UInt64)',
    'lkpid' : 'String',
    'process' : 'String',
    'p_processname' : 'String',
    'sql' : 'String',
    't_clientid': 'String',
    'trans': 'Nullable(Bool)',
    'error': 'String',
    'regions': 'String',
    'waitconnections': 'String',
    'deadlockconnectionintersections': 'String',
    'infobaseid': 'String',
    'clusterid' : 'String',
    'ruleid': 'String',
    'txt': 'String',
    'prm': 'String'
}

PRIMARY_KEYS = ('time', 'event_name')

def primary_keys_to_query():
    return ', '.join(PRIMARY_KEYS)

def db_fields_to_query():
    key_list = [key + ' ' + value for key, value in DB_FIELDS.items()]
    return ', '.join(key_list)

def db_create_query(base_name):
    return f'''CREATE TABLE IF NOT EXISTS {base_name} 
    (
    {db_fields_to_query()}
    )
    PRIMARY KEY(
    {primary_keys_to_query()}
    )
    '''