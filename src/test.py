import csv
import io
import gzip
import zlib
import parser
from parser import process_tj_record
import main

# data = [['Имя', 'Возраст'], ['Алиса', 25], ['Боб', 22]]

# buffer = io.StringIO()
# #writer = csv.DictWriter(buffer, delimiter='\t')
# #writer.
# writer.writerows(data)

# print(buffer.getvalue())
# print(parser.data_to_gzip_csv_bytes(data))


data = []

test_file_name = '24070816.log'
#simple_record = '36:50.509001-0,EXCPCNTX,0,ClientComputerName=,ServerComputerName=,UserName=,ConnectString='
#row1 = process_tj_record(simple_record, test_file_name)
#data.append(row1)

#simple_record = '''36:23.962001-1,HASP,1,process=1cv8,OSThread=11088,Txt='
#MEMOHASP_HASPID(,port=201,ser=ORGL8,,,,)->id=756112492,,stat=0,' '''
#row2 = process_tj_record(simple_record, test_file_name)
#data.append(row2)

#simple_record = r'''36:56.650005-0,EXCP,1,process=1cv8,OSThread=11088,Exception=81029657-3fe6-4cd6-80c0-36de78fe6657,Descr='src\rclient\src\ClientImpl.cpp(7934):
#81029657-3fe6-4cd6-80c0-36de78fe6657: server_addr=tcp://127.0.0.1:1541 descr=127.0.0.1:1541:10061(0x0000274D): Подключение не установлено, т.к. конечный компьютер отверг запрос на подключение. ;
#line=1026 file=src\rtrsrvc\src\DataExchangeTcpClientImpl.cpp' '''
#row3 = process_tj_record(simple_record, test_file_name)
#data.append(row3)

#simple_record = '36:56.650002-2063002,EXCPCNTX,1,SrcName=CONN,process=1cv8,OSThread=11088,ClientID=11,Txt=Outgoing connection closed'
#row4 = process_tj_record(simple_record, test_file_name)
#data.append(row4)

#from parser import data_to_gzip_csv_bytes

#data_encoded = data_to_gzip_csv_bytes(data)
#print(data_encoded)

#print(gzip.decompress(data_encoded).decode('utf-8'))

#data = parser.parse_logfile('C:/Users/Pr/Downloads/log1c/rphost_1059522/24082420.log')
#csv_data = parser.data_to_csv_string(data)
#with open('C:/Users/Pr/Downloads/log1c/test.csv', 'w+') as f:
#    f.write(csv_data)
#    f.close()
#print(csv_data)

#import sys
#print(sys.getsizeof(data))
#print(sys.getsizeof(data_encoded))


#test_string = '''32:30.774000-2997,DBPOSTGRS,3,process=rphost,p:processName=test,OSThread=2289635,t:clientID=8734,t:applicationName=BackgroundJob,t:computerName=ubuntuserver,t:connectID=133,DBMS=DBPOSTGRS,DataBase=192.168.0.199\test,Trans=0,dbpid=2272666,Sql='SELECT Creation,Modified,Attributes,DataSize,BinaryData FROM Params WHERE FileName = $1 ORDER BY PartNo',Prm="
#p_1: 'evlogparams.inf'::mvarchar",RowsAffected=1,Result=PGRES_TUPLES_OK'''

#row1 = process_tj_record(test_string, test_file_name)
#print(row1)



test_string  = '''23:26.126001-1,DBPOSTGRS,5,process=rphost,p:processName=test,OSThread=2269330,t:clientID=8632,t:applicationName=1CV8C,t:computerName=server,t:connectID=86,SessionID=1,Usr=Администратор,AppID=1CV8C,DBMS=DBPOSTGRS,DataBase=192.168.0.199\test,Trans=1,dbpid=2269338,Func=quickInsert,tableName=_InfoRg4821,Sql='INSERT INTO _InfoRg4821 (_Fld4822RRef,_Fld4823,_Fld4824,_Fld4825,_Fld4826,_Fld4827,_Fld4828,_Fld4829,_Fld4830,_Fld4831,_Fld4832,_Fld4833) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',Prm='
p_1: ''\\246\\254\\264.\\231d\\351\\204\\021\\357S@\\371\\031\\251\\235''::bytea
p_2: 63860127806052
p_3: 1
p_4: ''2024-08-24 20:00:00''::timestamp
p_5: 0.054
p_6: 1
p_7: ''{"Разд":[0],"КонфВер":"1.0.0.1","Конф":"Учебная конфигурация","Платф":"8.3.24.1624"}''::mvarchar
p_8: ''2024-08-24 20:23:26''::timestamp
p_9: 63860127806106
p_10: ''Администратор''::mvarchar
p_11: ''2024-08-24 20:23:26''::timestamp
p_12: FALSE',RowsAffected=1,Context='Форма.Вызов : Обработка.ОтчетПоДатеДокументаПокупки.Форма.Форма.Модуль.СформироватьНаСервере
Обработка.ОтчетПоДатеДокументаПокупки.Форма.Форма.Форма : 17 : ОценкаПроизводительности.ЗакончитьЗамерВремени("Выполнение обработки ''ОтчетПоДатеДокументаПокупки''", ВремяНачала);
	ОбщийМодуль.ОценкаПроизводительности.Модуль : 59 : ЗафиксироватьДлительностьКлючевойОперации(ПараметрыЗамера);
		ОбщийМодуль.ОценкаПроизводительности.Модуль : 745 : Запись.Записать();'''
row1 = process_tj_record(test_string, test_file_name)
print(row1)