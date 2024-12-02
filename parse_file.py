import re
import datetime
import http.client
import urllib
import csv
import gzip
import io
import glob

class TechJournalBase:
    start_record_pattern = re.compile(r'(\d{2}):(\d{2}).(\d{6})-(\d+),(\w+)')
    file_name_pattern = re.compile(r'(\d{2})(\d{2})(\d{2})(\d{2})\.log')
    parameters_patterns =[re.compile(x) for x in (",([A-Za-z0-9:]+)='([^']+?(?:''[^']*)*)'",
                           ',([A-Za-z0-9:]+)="([^"]+?(?:""[^"]*)*)"',
                           ',([A-Za-z0-9:]+)=([^,]+)' )]    

class ClickHouseBase:
    db_fields = {
    'time' : 'DateTime64(6)',
    'duration' : 'Nullable(UInt64)',
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
    'sessionid': 'String',
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
    'tablename': 'String',
    'osthread': 'Nullable(UInt64)',
    'clientid': 'Nullable(UInt64)',
    'iname': 'String',
    'method': 'String',
    'process': 'String',
    'callid' : 'String',
    'mname': 'String',
    'dstclientid':  'Nullable(UInt64)',
    'memory': 'Nullable(Int64)',
    'memorypeak' : 'Nullable(Int64)',
    'inbytes': 'Nullable(Int64)',
    'outbytes': 'Nullable(Int64)',
    'cputime': 'Nullable(Int64)',
    'process': 'String',
    'result': 'String'
    }
    primary_keys = ('time', 'event_name')

class TechJournalReader(TechJournalBase):

    def __init__(self, file_name: str):
        self.file_name = file_name

    def read_tech_journal(self):
        with open(file=self.file_name, mode='r', encoding='utf-8-sig') as file:
            log_lines = []
            for line in file:
                if self.check_start_log(line):
                    if log_lines: yield ''.join(log_lines).rstrip()
                    log_lines = [line]
                else:
                    log_lines.append(line)
            if log_lines: yield ''.join(log_lines).rstrip()

    def __str__(self):
        return f'TechJournalReader: {self.file_name}'

    def check_start_log(self, log_line: str):
        return re.match(TechJournalBase.start_record_pattern, log_line) is not None


class TechJournalRecondParser(TechJournalBase, ClickHouseBase):
    
    def __init__(self, file_name: str):
        self.file_name = file_name

    def process_tj_record(self, record: str):
        eventparams = re.findall(TechJournalBase.start_record_pattern, record)
        (minute, second, msec, duration, event_name) = eventparams[0]
        fileparams = re.findall(TechJournalBase.file_name_pattern, self.file_name)
        (year, month, day, hour) = fileparams[0]
    
        data = dict()

        record_tmp = record
        for pattern in TechJournalBase.parameters_patterns:
            params = re.findall(pattern, record_tmp)
            for param in params:
                param_key = param[0].lower().replace(':', '_')
                if param_key in ClickHouseBase.db_fields:
                    data[param_key] = param[1]
            record_tmp = re.sub(pattern, '', record_tmp) 

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

class ClickHouseConnector(ClickHouseBase):

    def __init__(self, host: str, port: int, base_name: str, user: str, password: str):
        self.host = host
        self.port = port
        self.base_name = base_name
        self.user = user
        self.password = password
        self.ping()
        self.create_database()

    def login_headers(self):
        return {'X-ClickHouse-user': self.user, 'X-ClickHouse-Key': self.password}

    def ping(self):
        conn = http.client.HTTPConnection(host=self.host, port=self.port)
        conn.request('GET', '/ping')

        response = conn.getresponse()
        if response.status != 200:
            raise Exception("no connection to clickhouse server")

    def create_database(self):
        db_create_query_text = self.db_create_query()
        conn = http.client.HTTPConnection(host=self.host, port=self.port)
        conn.request('POST', '/', db_create_query_text, headers=self.login_headers())
        response = conn.getresponse()
        if response.status != 200:
            raise Exception('Error to create database')
        conn.close()

    def post_records(self, records: bytes):
        conn = http.client.HTTPConnection(host=self.host, port=self.port)
        insert_query = f'INSERT INTO {self.base_name} FORMAT CSV'
        query_data = {'query': insert_query, 'format_csv_delimiter': '|'}
        query = urllib.parse.urlencode(query_data)

        headers = self.login_headers()
        headers.update({'Content-Encoding': 'gzip'})

        conn.request('POST', '/?' + query , headers=headers, body=records)
        response = conn.getresponse()

        if response.status != 200:
            print(response.msg)
            print(response.headers)
            return False
        return True

    def db_create_query(self):
        return f'''CREATE TABLE IF NOT EXISTS {self.base_name} 
    (
    {self.db_fields_to_query()}
    )
    ENGINE = ReplacingMergeTree()
    ORDER BY ({self.primary_keys_to_query()})
    PRIMARY KEY(
    {self.primary_keys_to_query()})
    ;
    '''

    def db_fields_to_query(self):
        key_list = [key.lower() + ' ' + value for key, value in ClickHouseBase.db_fields.items()]
        return ', '.join(key_list)

    def primary_keys_to_query(self):
        return ', '.join(ClickHouseBase.primary_keys)

class BinaryCsvGzipWriter():

    def _data_to_csv_string(self, data):
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=ClickHouseBase.db_fields.keys(), delimiter='|')
        writer.writeheader()
        writer.writerows(data)
        return buffer.getvalue()

    def _csv_string_to_gzip(self, csv_string):
        csv_bytes = bytes(csv_string, encoding='utf-8')
        return gzip.compress(csv_bytes, compresslevel=9) 

    def dict_to_csv_gzip(self, data):
        csv_string = self._data_to_csv_string(data)
        return self._csv_string_to_gzip(csv_string)   

class InsertQueryGenerator(ClickHouseBase):
    def generate_sql(self, data):
        buffer = io.StringIO();
        writer = open(buffer)


if __name__ == '__main__':

    now = datetime.datetime.now()
    files = glob.glob('E:\\Logi_29_11_2024\\**\\*.log',recursive=True)
    connector = ClickHouseConnector('192.168.0.199', 8123, 'brif_29112024', 'user', 'password')
    counter = 1
    for file in files:
        test_reader = TechJournalReader(file_name=file)
        parser = TechJournalRecondParser(file_name=file)
        GzipWriter = BinaryCsvGzipWriter()
        result_data = []
        
        for record in test_reader.read_tech_journal():
            
            data = parser.process_tj_record(record=record)
            result_data.append(data)
            if len(result_data) == 10_000:
                binary_data = GzipWriter.dict_to_csv_gzip(result_data)
                connector.post_records(binary_data)
                result_data = []
                

        if result_data:
            binary_data = GzipWriter.dict_to_csv_gzip(result_data)
            connector.post_records(binary_data)

        print(counter,'/' ,len(files), file)
        counter=counter + 1
    print(datetime.datetime.now() - now)