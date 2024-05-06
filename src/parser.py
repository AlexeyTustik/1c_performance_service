import re
import datetime
import utils
import csv
import gzip
import io

def data_to_csv_string(data):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=utils.DB_FIELDS.keys(), delimiter='|')
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
                str_log = str
            else:
                str_log = '-#-'.join((str_log, str))
        if str_log:
            data = process_tj_record(str_log, file_name)
            records.append(data)

    return records
    
def log_file_to_csv_gzip_bytes(file_name: str):
    records = parse_logfile(file_name=file_name)
    if not records:
        return None
    gzip_bytes = data_to_gzip_csv_bytes(data=records)
    return gzip_bytes

def append_to_dict(D_params, lparams):
    for params in lparams:
        new_param = params[0].lower()
        if new_param in D_params:
            new_value = params[1].replace("'","").replace("''", "").replace('"',"").replace('-#-', '\n').strip()
            D_params[new_param] = new_value

def process_tj_record(record: str, file_name: str):
    eventparams = re.findall(__START_RECORD_PATTERN, record)
    (minute, second, msec, duration, event_name) = eventparams[0]
    fileparams = re.findall(__FILE_NAME_PATTERN, file_name)
    (year, month, day, hour) = fileparams[0]
    
    data = dict.fromkeys(utils.DB_FIELDS.keys())

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

__RE_PATTERNS = (",(\w+)='([^']+)",
                ',(\w+)="([^"]+)',
                ',([A-Za-z0-9А-Яа-я:]+)=([^,]+)')

__PATTERNS = [re.compile(pattern) for pattern in __RE_PATTERNS]
__START_RECORD_PATTERN = r'(\d{2}):(\d{2}).(\d{6})-(\d+),(\w+)' # Запись начинается Минуты:Секунды:Микросекунды, Имя_события
__FILE_NAME_PATTERN = r'(\d{2})(\d{2})(\d{2})(\d{2})' # Файл содержит ГГ.ММ.ДД.ЧЧ 