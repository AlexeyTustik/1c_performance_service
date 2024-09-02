import http.client
import glob
import hashlib
import os
import urllib
import re
import datetime
import csv
import gzip
import io
import configparser
import time

def start():

    check_system_folder()
    check_processed()
    check_logs()

    conn = http.client.HTTPConnection(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    conn.request('GET', '/ping')

    response = conn.getresponse()
    if response.status != 200:
        raise Exception("no connection to clickhouse server")

    create_database(CLICKHOUSE_HOST, CLICKHOUSE_PORT, BASE_NAME)

    conn.close()
    
    while True:
        files = glob.glob(FILE_PATTERN, recursive=True)
        for file in files:
            process_file(file)
        time.sleep(30)

def create_directoty(directory_path):
    if not os.path.exists(directory_path):
        os.mkdir(directory_path)

def check_logs():
    create_directoty(LOG_PATH)

def check_system_folder():
    create_directoty(SYS_PATH)

def process_file(file):
    try:
        file_hash = get_hash_file(file)
    except FileNotFoundError: # File could be deleted
        return
    hash_file_name = os.path.join(IMPORTED_PATH, file_hash)
    if os.path.exists(hash_file_name):
        return

    for data in parse_logfile(file):
        data_csv = data_to_csv_string(data)
        data_encoded = data_to_gzip_csv_string(data_csv)
        conn = http.client.HTTPConnection(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        query_data = {'query': INSERT_QUERY, 'format_csv_delimiter': '|'}
        query = urllib.parse.urlencode(query_data)

        headers = login_headers()
        headers.update({'Content-Encoding': 'gzip'})

        conn.request('POST', '/?' + query , headers=headers, body=data_encoded)
        response = conn.getresponse()

        if response.status != 200:
            return

    processed_file = open(hash_file_name, 'w+')
    processed_file.close()

def get_hash_file(file):
    '''Получаем хеш файла, при чтении важно знать был он изменен или нет,
    чтобы не записывать одни и те же данные'''
    hash_function = hashlib.new('MD5')

    with open(file, 'rb') as file:
        while True:
            data = file.read(1024)
            if not data: break
            hash_function.update(data)

    return hash_function.hexdigest()


def check_processed():
    create_directoty(IMPORTED_PATH)

def login_headers():
    return {'X-ClickHouse-user': CLICKHOUSE_USER, 'X-ClickHouse-Key': CLICKHOUSE_PASSWORD}

def create_database(host, port, database_name):
    db_create_query_text = db_create_query(database_name)
    conn = http.client.HTTPConnection(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    conn.request('POST', '/', db_create_query_text, headers=login_headers())
    response = conn.getresponse()
    if response.status != 200:
        raise Exception('Error to create database')
    conn.close()

def data_to_csv_string(data):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=DB_FIELDS.keys(), delimiter='|')
    writer.writeheader()
    writer.writerows(data)
    return buffer.getvalue()

def data_to_gzip_csv_string(csv_string):
    csv_bytes = bytes(csv_string, encoding='utf-8')
    return gzip.compress(csv_bytes, compresslevel=9)

def parse_logfile(file_name: str):
    
    records = []

    with open(file_name, 'r', encoding='utf-8-sig') as f:
        str_log = ''
        for str in f.readlines():
            str = str.strip()
            if re.match(__START_RECORD_PATTERN, str):
                if str_log:
                    str_log = str_log.replace("''", '')
                    data = process_tj_record(str_log, file_name)
                    records.append(data)
                    if len(records) == 100_000:
                        yield records
                        records = []
                str_log = str
            else:
                str_log = '-#-'.join((str_log, str))
        if str_log:
            data = process_tj_record(str_log, file_name)
            records.append(data)

    yield records
    
def log_file_to_csv_gzip_bytes(file_name: str):
    records = parse_logfile(file_name=file_name)
    if not records:
        return None
    gzip_bytes = data_to_gzip_csv_bytes(data=records)
    return gzip_bytes

def append_to_dict(D_params, lparams):
    for params in lparams:
        new_param = params[0].replace(':', '_').lower()
        if new_param in D_params:
            new_value = params[1].replace("'","").replace("''", "").replace('"',"").replace('-#-', '\n').strip()
            D_params[new_param] = new_value

def process_tj_record(record: str, file_name: str):
    eventparams = re.findall(__START_RECORD_PATTERN, record)
    (minute, second, msec, duration, event_name) = eventparams[0]
    fileparams = re.findall(__FILE_NAME_PATTERN, file_name)
    (year, month, day, hour) = fileparams[0]
    
    data = dict.fromkeys(DB_FIELDS.keys())

    for pattern in __PATTERNS:
            params = re.findall(pattern, record)
            append_to_dict(data, params)
            if __PATTERNS.index(pattern) != len(__PATTERNS)-1:
                record = re.sub(pattern, "", record)

    event_time = datetime.datetime(year=int(f'20{year}'),
                                month=int(month),
                                day=int(day),
                                hour=int(hour),
                                minute=int(minute),
                                second=int(second),
                                microsecond = int(msec))
    data['duration'] = duration
    data['time'] = event_time
    data['event_name'] = event_name
    return data

def primary_keys_to_query():
    return ', '.join(PRIMARY_KEYS)

def db_fields_to_query():
    key_list = [key.lower() + ' ' + value for key, value in DB_FIELDS.items()]
    return ', '.join(key_list)

def db_create_query(base_name):
    return f'''CREATE TABLE IF NOT EXISTS {base_name} 
    (
    {db_fields_to_query()}
    )
    ENGINE = MergeTree
    ORDER BY ({primary_keys_to_query()})
    PRIMARY KEY(
    {primary_keys_to_query()})
    ;
    '''

__RE_PATTERNS = (",(\w+)='([^']+)",
                ',(\w+)="([^"]+)',
                ',([A-Za-z0-9А-Яа-я:]+)=([^,]+)')

__PATTERNS = [re.compile(pattern) for pattern in __RE_PATTERNS]
__START_RECORD_PATTERN = r'(\d{2}):(\d{2}).(\d{6})-(\d+),(\w+)' # Запись начинается Минуты:Секунды:Микросекунды, Имя_события
__FILE_NAME_PATTERN = r'(\d{2})(\d{2})(\d{2})(\d{2})' # Файл содержит ГГ.ММ.ДД.ЧЧ 

DB_FIELDS = {
    'time' : 'DateTime64(6)',
    'duration' : 'Nullable(UInt16)',
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
    't_clientid': 'String',
    'event_name' : 'String',
    'database' : 'String',
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
    't_computername': 'String',
    'trans': 'Nullable(Bool)',
    'error': 'String',
    'regions': 'String',
    'waitconnections': 'String',
    'deadlockconnectionintersections': 'String',
    'infobaseid': 'String',
    'clusterid' : 'String',
    'ruleid': 'String',
    'txt': 'String',
    'prm': 'String',
    'locks': 'String',
    'tablename': 'String'
}

PRIMARY_KEYS = ('time', 'event_name')

config = configparser.ConfigParser()
config.read('config.ini')

TECH_JOURNAL_PATH = config['VOLUMES']['TECH_JOURNAL_PATH']
SYS_PATH = config['VOLUMES']['SYS_PATH']
BASE_NAME = config['CLICKHOUSE']['BASE_NAME']
CLICKHOUSE_HOST = config['CLICKHOUSE']['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = int(config['CLICKHOUSE']['CLICKHOUSE_PORT'])
CLICKHOUSE_USER = config['CLICKHOUSE']['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = config['CLICKHOUSE']['CLICKHOUSE_PASSWORD']

IMPORTED_PATH = os.path.join(SYS_PATH, 'processed') # Служебная папка, в которой помечается что файл обработан. В имя пишется его хэш-функция
LOG_PATH = os.path.join(SYS_PATH, 'logs')
FILE_PATTERN = os.path.join(TECH_JOURNAL_PATH, '*', '*.log')
INSERT_QUERY = f'INSERT INTO {BASE_NAME} FORMAT CSV'

if __name__ == '__main__':
   start()
