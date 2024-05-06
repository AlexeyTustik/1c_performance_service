import http.client
import glob
import hashlib
import os
import utils
import parser
import urllib

TECH_JOURNAL_PATH = r'path_to_tech_journal'
BASE_NAME = "tj_data"
CLICKHOUSE_HOST = "clickhouse_server"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'clickhouse_user'
CLICKHOUSE_PASSWORD = 'clickhouse_password'


IMPORTED_PATH = os.path.join(TECH_JOURNAL_PATH, '.processed') # Служебная папка, в которой помечается что файл обработан. В имя пишется его хэш-функция
FILE_PATTERN = os.path.join(TECH_JOURNAL_PATH, '*', '*.log')
INSERT_QUERY = f'INSERT INTO {BASE_NAME} FORMAT CSV'

def start():
    conn = http.client.HTTPConnection(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    conn.request('GET', '/ping')

    response = conn.getresponse()
    if response.status != 200:
        raise Exception("no connection to clickhouse server")

    create_database(CLICKHOUSE_HOST, CLICKHOUSE_PORT, BASE_NAME)

    conn.close()
    check_processed() # В processed храним информацию о файлах

    while True:
        files = glob.glob(FILE_PATTERN, recursive=True)
        for file in files:
            process_file(file)

def process_file(file):
    file_hash = get_hash_file(file)
    hash_file_name = os.path.join(IMPORTED_PATH, file_hash)
    if os.path.exists(hash_file_name):
        return

    data = parser.parse_logfile(file)
    data_csv = parser.data_to_csv_string(data)
    data_encoded = parser.data_to_gzip_csv_string(data_csv)
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
    if not os.path.exists(IMPORTED_PATH):
        os.mkdir(IMPORTED_PATH)

def login_headers():
    return {'X-ClickHouse-user': CLICKHOUSE_USER, 'X-ClickHouse-Key': CLICKHOUSE_PASSWORD}

def create_database(host, port, database_name):
    db_create_query = utils.db_create_query(database_name)
    conn = http.client.HTTPConnection(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    conn.request('POST', '/', db_create_query, headers=login_headers())
    response = conn.getresponse()
    if response.status != 200:
        print(response.msg)
        raise Exception('Error to create database')
    conn.close()

if __name__ == '__main__':
   start()
