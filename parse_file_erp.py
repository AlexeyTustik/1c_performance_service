import re
import sys
import os
import datetime
import hashlib
import uuid
from pathlib import Path
from subprocess import call

current_time = datetime.datetime.now()

#Параметры загружаемые скриптом
list={
'ADMINISTRATOR':'\'\'',	    'AGENTURL':'\'\'',	                        'ALREADYLOCKED':'\'\'',	            'APPID':'\'\'',
'APPL':'\'\'',	            'APPLICATIONEXT':'\'\'',	                'APPLICATIONNAME':'\'\'',	        'AVERAGE_RT':'0',
'BODY':'\'\'',
'CALLID':'\'\'',	        'CALLS':'0',	                            'CALLWAIT':'0',	                    'CENTRALPORT':'\'\'',
'CENTRALSERVER':'\'\'',	    'CLIENTCOMPUTERNAME':'\'\'',	            'CLIENTID':'0',	                    'CLUSTER':'0',              'CLASTER1C':'\'\'',
'CLUSTERID':'\'\'',	        'COMPUTERNAME':'\'\'',	                    'CONNECTID':'0',	                'CONNECTION':'\'\'',        'CONTEXTHASH':'\'\'',
'CONNECTIONID':'\'\'',	    'CONNECTIONS':'\'\'',	                    'CONNECTSTRING':'\'\'',	            'CONTEXT':'\'\'',           'SQLHASH':'\'\'',
'COPYBYTES':'0',	        'CPUTIME':'0',	                            'CRITICALPROCESSESTOTALMEMORY':'0',	'CURRENT_RT':'0',           'DESCRHASH':'\'\'',
'DATA':'\'\'',	            'DATABASE':'\'\'',	                        'DATAPATH':'\'\'',	                'DBMS':'\'\'',
'DBPID':'0',	            'DEADLOCKCONNECTIONINTERSECTIONS':'\'\'',	'DEDICATEALL':'\'\'',	            'DEFICIT':'\'\'',
'DESCR':'\'\'',	            'DESCRIPTION':'\'\'',	                    'DISTRIBDATA':'\'\'',	            'DSTADDR':'\'\'',
'DSTID':'\'\'',	            'DSTPID':'\'\'',	                        'DSTSRV':'\'\'',	                'DUMPERROR':'\'\'',
'DUMPFILE':'\'\'',	        'DURATION':'0',	                            'ERR':'\'\'',	                    'EVENT':'\'\'',
'EXCEPTION':'\'\'',	        'EXPIRATIONTIMEOUT':'0',	                'EXTDATA':'\'\'',	                'FILENAME':'\'\'',
'FILESSIZE':'0',	        'FIRST':'\'\'',	                            'FORCE':'0',	                    'FORM':'\'\'',
'FORMITEM':'\'\'',	        'FUNC':'\'\'',	                            'HEADERS':'\'\'',	                'HOST':'\'\'',
'IB':'\'\'',	            'ID':'\'\'',	                            'INAME':'\'\'',	                    'INBYTES':'0',
'INFOBASE':'\'\'',	        'INFOBASEID':'\'\'',	                    'INFOBASES':'\'\'',	                'INSTANCEID':'\'\'',
'INTERFACE':'\'\'',	        'KILLBYMEMORYWITHDUMP':'\'\'',	            'KILLPROBLEMPROCESSES':'\'\'',	    'LIFETIMELIMIT':'0',
'LOADBALANCINGMODE':'\'\'',	'LOCKDURATION':'0',	                        'LOCKS':'\'\'',	                    'LVL':'0',
'MAINEVENT':'\'\'',	        'MANAGERLIST':'\'\'',	                    'MAXRPCONNECTIONS':'0',	            'MAXRPIBQUANTITY':'0',
'MEMORY':'0',	            'MEMORYPEAK':'0',	                        'MESSAGE':'\'\'',	                'METHOD':'\'\'',
'MNAME':'\'\'' ,	        'MODE':'\'\'',	                            'MODULE':'\'\'',	                'NMB':'\'\'',
'OBSOLETE':'\'\'',	        'ODATE':'\'\'',	                            'OSEXCEPTION':'\'\'',	            'OSTHREAD':'0',
'OUTBYTES':'0',	            'PACKETCOUNT':'0',	                        'PHRASE':'\'\'',	                'PID':'\'\'',
'PORT':'\'\'',	            'PRM':'\'\'',	                            'PROCESS':'\'\'',	                'PROCESSID':'\'\'',
'PROCESSNAME':'\'\'',	    'PROCURL':'\'\'',	                        'PROTECTED':'0',	                'QUERY':'\'\'',
'RANGES':'\'\'',	        'REF':'\'\'',	                            'REGIONS':'\'\'',	                'REGISTERED':'\'\'',
'RELEASED':'\'\'',          'DSTCLIENTID':'0',	                        'REQUEST':'\'\'',	                'RES':'\'\'',	
'RESULT':'\'\'',            'RETEXCP':'\'\'',	                        'RMNGRURL':'\'\'',	                'ROWS':'0',	
'ROWSAFFECTED':'0',         'RUNAS':'\'\'',	                            'SAFECALLMEMORYLIMIT':'0',	        'SBST':'\'\'',	
'SCRNAME':'\'\'',           'SDBL':'\'\'',	                            'SEANCEID':'\'\'',	                'SEARCHSTRING':'\'\'',	
'SERVER':'\'\'',            'SERVERCOMPUTERNAME':'\'\'',	            'SERVERID':'\'\'',	                'SERVERLIST':'\'\'',	
'SERVERNAME':'\'\'',        'SERVICENAME':'\'\'',	                    'SERVICEPRINCIPALNAME':'\'\'',	    'SESSIONID':'\'\'',	
'SQL':'\'\'',               'SRCADDR':'\'\'',	                        'SRCID':'\'\'',	                    'SRCNAME':'\'\'',	
'SRCPID':'\'\'',            'SRCPROCESSNAME':'\'\'',	                'STATUS':'\'\'',	                'TABLENAME':'\'\'',	
'TARGETCALL':'0',           'TEMPORARYALLOWEDPROCESSESTOTALMEMORY':'0',	'TEMPORARYALLOWEDPROCESSESTOTALMEMORYTIMELIMIT':'0',	
'TIMESTAMP':'\'\'',         'TMPCONNECTID':'\'\'',                      'TRANS':'0',	                    'TXT':'\'\'',	
'URI':'\'\'',	            'URL':'\'\'',                               'USEDSIZE':'0',	                    'USERNAME':'\'\'',	
'USR':'\'\'',	            'VAL':'0',                                  'WAITCONNECTIONS':'\'\''
}

dictInt=[]
for elem in list :
    if list[elem] == '0' :
        dictInt.append(elem)

count=0

parameters_patterns =[re.compile(x) for x in (",([A-Za-z0-9:]+)='([^']+?(?:''[^']*)*)'",
                           ',([A-Za-z0-9:]+)="([^"]+?(?:""[^"]*)*)"',
                           ',([A-Za-z0-9:]+)=([^,]+)' )]  

head_pattern = re.compile(r"(\d{2}):(\d{2}).(\d{6})-(\d+),(\w+),(\d+)")
split_pattern = re.compile(r"(\d{2}:\d{2}.\d{6}-\d+,\w+,\d+)")
pat=re.compile(r"\d{2}:\d{2}\.\d{6}-\d+,.*")

#Процедура обрабывает строку, разбирает её и заполняет словарь 
#А далее формирует строку для INSERT
def ParseLine(head,line):
    #Подготавливаем словарь (структура) для сбора параметров события ТЖ
    dict = list.copy()

    #line = line.replace('\'\'', '$$$').replace("\t","")
    line = line.replace("\t","")
    heads_all = re.findall(head_pattern,head)
    heads=heads_all[0]
    dict['SERVER']='\''+SERVER+'\''
    dict['CLASTER1C']='\''+CLASTER1C+'\''    
    dict['ODATE']='\''+odate+'\''
    dict['TIMESTAMP']='\''+time_stamp+''+heads[0]+':'+heads[1]+'.'+heads[2]+'\''
    dict['DURATION']=heads[3]
    dict['MAINEVENT']='\''+heads[4]+'\''
    dict['LVL']=heads[5]

    mas1=line.split(',Sql="')
    if len(mas1) > 1 :
        mas2=mas1[1].split('",Rows')
        if len(mas2) > 1 :
            dict["SQL"]='\''+mas2[0].replace("\\","\\\\").replace("\'","\\\'")+'\''
            line=mas1[0]+',Rows'+mas2[1]

    mas1=line.split(",Sql='")
    if len(mas1) > 1 :
        mas2=mas1[1].split("',Rows")
        if len(mas2) > 1 :
            dict["SQL"]='\''+mas2[0].replace("\\","\\\\").replace("\'","\\\'")+'\''
            line=mas1[0]+',Rows'+mas2[1]
    

    record_tmp=line
    for pattern in parameters_patterns:
        params = re.findall(pattern, record_tmp)
        for param in params:
            mass_par=param[0].split(':')
            if len(mass_par) > 1 :
                param_key=mass_par[1]
            else :
                param_key=mass_par[0]
    
            param_key = param_key.upper()
            if param_key in list:
                if param_key in dictInt :
                    dict[param_key] = param[1]
                else:
                    dict[param_key] = '\''+param[1].replace("\\","\\\\").replace("'","\\\'")+'\''
            else:
                print(param_key)   
        record_tmp = re.sub(pattern, '', record_tmp)        

    #Соберем ХЕШИ
    if dict['CONTEXT'] != '\'\'' :
        dict['CONTEXTHASH'] = '\''+hashlib.md5(dict['CONTEXT'].encode()).hexdigest()+'\''
    if dict['SQL'] != '\'\'' :
        dict['SQLHASH'] = '\''+hashlib.md5(dict['SQL'].encode()).hexdigest()+'\''
    if dict['DESCR'] != '\'\'' :
        dict['DESCRHASH'] = '\''+hashlib.md5(dict['DESCR'].encode()).hexdigest()+'\''
    
    #Соберем строку для insert
    res_line = ','.join(dict.values())
    fl.write('(')
    fl.write(res_line)
    fl.write('),\n')
    

#Получение щапки инсерта
def GetStartSQL():
    sql='INSERT INTO '+table_name+' \n ('
    first = True
    for key in list:
        if first :
            sql+=''+key
        else :
            sql+=','+key
        first = False
    sql+=')\nVALUES\n'    
    return sql

#Процедура отправки файла в ClickHouse
def SendInsert(file_out):
    rc = call('type '+file_out+' | gzip -c | curl -u '+username+':'+password+' -sS --data-binary @- -H \'Content-Encoding: gzip\' \''+url_clickhouse+'/\'', shell=True)
    if rc != 0:
        raise Exception('Error sent INSERT')

def SendInsert2(file_out):
    rc = call('sh /f/Vasin/CH/sent.sh '+file_out+'', shell=True)
    if rc != 0:
        raise Exception('Error sent INSERT')


#Основной алгоритм, побежали выполнять :
dir_log=sys.argv[1].strip("'")
file_out=sys.argv[2].strip("'")
table_name=sys.argv[3].strip("'")
SERVER=sys.argv[4].strip("'")
CLASTER1C=''+SERVER.replace('.gk.rosatom.local','')[:-1]+'1'
name_file=sys.argv[5]
url_clickhouse=sys.argv[6]
username=sys.argv[7]
password=sys.argv[8]

print('-------------')
print('FILE  : '+name_file)

#Положим в файл первую строку с шапкой инсерта, по списку параметров что собрали
sql = GetStartSQL()
fl=open(file_out,'w',encoding='utf-8')
fl.write(sql)

k=0
count=0
line=''
break_out_flag = False
for root, dirs, files in os.walk(dir_log):
    for file in files:
        #if os.path.getsize(os.path.join(root, file)) > 3 and file == name_file :
        if os.path.getsize(os.path.join(root, file)) > 3 :
            file_in = os.path.join(root, file);
            line=''

            #из имени файла получаем дату для партиций и timestamp
            Name_file='20'+Path(file_in).stem
            odate=Name_file[0:4]+'-'+Name_file[4:6]+'-'+Name_file[6:8]
            time_stamp=odate+' '+Name_file[8:10]+':'
            
            #ВАРИАНТ 1 Более быстрый(помещает весь файл в память)
            #Обработаем файл целиком

            #ifile = open(file_in, 'r', encoding='utf-8-sig').read()
            #InfoFromFile = re.split(split_pattern, ifile)
            #x=1
            #while x < len(InfoFromFile):
            #    ParseLine(InfoFromFile[x],InfoFromFile[x+1])
            #    count+=1
            #    if count >= 100000 :
            #        fl.close()
            #        SendInsert(file_out)
            #        fl=open(file_out,'w',encoding='utf-8')
            #        sql = GetStartSQL()
            #        fl.write(sql)
            #        count = 0
            #    x+=2

            #ВАРИАНТ 2 Стабильнее (не помещает весь файл в память, проглотит любые файлы)
            #Побежим по файлу и соберем строки для VALUES
            with open(file_in, 'r', encoding='utf-8-sig') as afile:
                for tmp1_line in afile:
                    tmp_line = tmp1_line.rstrip()
                    if re.match(pat,tmp_line)  is not None :
                        InfoFromFile = re.split(split_pattern, line)
                        x=1
                        while x < len(InfoFromFile):
                            ParseLine(InfoFromFile[x],InfoFromFile[x+1])
                            x+=2    
                        count+=1  
                        line = tmp_line                     
                    else:
                        line=''.join((line,tmp_line))
                    
                    #Если накопилось 100 000 событий - отправим и начнем формировать новый файл
                    if count >= 100000 :
                        fl.close()
                        SendInsert(file_out)
                        fl=open(file_out,'w',encoding='utf-8')
                        sql = GetStartSQL()
                        fl.write(sql)
                        count = 0
                        line=''
            if line != '' :
                InfoFromFile = re.split(split_pattern, line)
                x=1
                while x < len(InfoFromFile):
                    ParseLine(InfoFromFile[x],InfoFromFile[x+1])
                    x+=2
                
fl.close()  
#Если есть что-то не отправленное - отправим
if count > 0 :
    SendInsert(file_out)


current_time2 = datetime.datetime.now()

print(current_time)
print(current_time2)