import cx_Oracle
import mysql.connector
import json
import urllib3
import csv
from settings.credentials import *
from settings.parameters import *

urllib3.disable_warnings()
from datetime import datetime

sysdate = datetime.now().strftime('%d/%m/%Y')
sysdateWSO2 = datetime.now().strftime('%m%Y')

print(str(datetime.now().strftime('%d/%m/%Y-%H:%M:%S')+': Inicio da atividade'))
# --------------------------- Abrindo conexão com MYSql ---------------------------
db = mysql.connector.connect(user=CRD_USER_DB_BLAZON, passwd=CRD_PWD_DB_BLAZON, host=PAR_BLAZON_IP, db=PAR_BLAZON_DB_NAME)
cursor_blazon = db.cursor()

# fazendo select dos usuário ativos nos blazon
cursor_blazon.execute("select (select case when EntryAttrib.value = 'ALGAR S A EMPREENDIMENTOS E PARTICIPAÇÕES' or EntryAttrib.value = 'ALGAR S/A EMPREENDIMENTOS E PARTICIPACOES' then 15 else 0 end from EntryAttributeValue EntryAttrib where EntryAttrib.entry_id = u.id and EntryAttrib.name = 'location') empresa, "
                      "(select IFNULL(trim(CONCAT('015',EntryAttrib.value)),0) from EntryAttributeValue EntryAttrib where EntryAttrib.entry_id = u.id and EntryAttrib.name = 'registrationNumber') usuarioQualitor,t.ENTITLEMENT, t.`TASK IDENTIFIER`, t.REQUESTER, t.`USERNAME REQUESTER`, t.EXECUTOR, t.STATUS "
                      "from ANALYTICS_PROVISIONING_GRANT_ENTITLEMENT_TASKS_VIEW t, `User` u where u.username = t.`USERNAME REQUESTER` and t.RESOURCE = 'CSC ATENDE - ATENDENTE' and t.STATUS IN ('TODO','WAITING_ASSIGN')")

blazon = cursor_blazon.fetchall()
db.close()

# --------------------------- Abrindo conexão com Qualitor ---------------------------
#HML
#dsn_tns = "(DESCRIPTION = (ADDRESS_LIST=(LOAD_BALANCE=on)(ADDRESS=(PROTOCOL=TCP)(HOST=172.33.3.101)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=172.33.3.100)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=172.33.3.99)(PORT=1521)))(CONNECT_DATA =(SERVICE_NAME = QUALITH_gru15r)))"

#PRD
conn = cx_Oracle.connect(user=CRD_USER_DB_QUALITOR, password=CRD_PWD_DB_QUALITOR, dsn=PAR_QUALITOR_TNS)
c = conn.cursor()

list_export = []
users_qualitor = []
exec_insert = 'S'

#percorre todos as tarefas em aberto no blazon
for blazon_row in blazon:
    #select para encontrar codigo usuario no qualitor
    if blazon_row[1] != None:
        c.execute("select CDUSUARIO from ad_usuario where NMUSUARIOREDE = '"+blazon_row[1]+"' and rownum = 1")
        users_qualitor = c.fetchall()

    if users_qualitor == []:
        c.execute("select CDUSUARIO from ad_usuario where idativo = 'Y' and upper(NMUSUARIO) = '" + blazon_row[4] + "' and rownum = 1")
        users_qualitor = c.fetchall()

    #verifica solicitante é da empresa 15 e se foi encontrado no qualitor
    if users_qualitor != []:
        #verifica se equipe é de visualização
        if 'VISUALIZAÇÃO' in blazon_row[2][13:]:
            #faz select na tabela de equipes para encontrar o codigo
            c.execute("select CDEQUIPE from hd_equipe where idativo = 'Y' AND upper(NMEQUIPE) = '" + blazon_row[2][13:][:-15] + "' and rownum = 1")
            equipe_qualitor = c.fetchall()
            #se encontrar a equipe
            if equipe_qualitor != []:
                list_export.append((blazon_row[3],blazon_row[4],blazon_row[2],'SUCESSO'))
                #insert equipe visualização
                c.execute("select count(*) from ad_usuarioequiperest where cdusuario = '"+str(users_qualitor[0][0])+"' and cdequipe = '"+str(equipe_qualitor[0][0])+"'")
                exists_tupla = c.fetchall()
                if exists_tupla[0][0] == 0 and exec_insert == 'S':
                    c.execute("insert into ad_usuarioequiperest values ('"+str(users_qualitor[0][0])+"','"+str(equipe_qualitor[0][0])+"')")
                    conn.commit()
                #print('INSERT PARA ' + str(users_qualitor[0][0]) + '  -  VISUALIZAÇÃO: '+ str(equipe_qualitor[0][0]))
            else:
                list_export.append((blazon_row[3],blazon_row[4],blazon_row[2],'ERROR - EQUIPE NÃO ENCONTRADA'))
        #não sendo de visualização cai neste laço
        else:
            #faz select na tabela de equipes para encontrar o codigo
            c.execute("select CDEQUIPE from hd_equipe where idativo = 'Y' AND upper(NMEQUIPE) = '" + blazon_row[2][13:] + "' and rownum = 1")
            equipe_qualitor = c.fetchall()
            if equipe_qualitor != []:
                list_export.append((blazon_row[3],blazon_row[4],blazon_row[2],'SUCESSO'))
                #insert equipe atendimento
                c.execute("select count(*) from hd_equipeusuario where cdusuario = '"+str(users_qualitor[0][0])+"' and cdequipe = '"+str(equipe_qualitor[0][0])+"'")
                exists_tupla = c.fetchall()
                if exists_tupla[0][0] == 0 and exec_insert == 'S':
                    c.execute("insert into hd_equipeusuario  values ('"+str(users_qualitor[0][0])+"','"+str(equipe_qualitor[0][0])+"')")
                    conn.commit()

                    # insert equipe visualização
                    c.execute("select count(*) from ad_usuarioequiperest where cdusuario = '" + str(users_qualitor[0][0]) + "' and cdequipe = '" + str(equipe_qualitor[0][0]) + "'")
                    exists_tupla = c.fetchall()
                    if exists_tupla[0][0] == 0 and exec_insert == 'S':
                        c.execute("insert into ad_usuarioequiperest values ('" + str(users_qualitor[0][0]) + "','" + str(equipe_qualitor[0][0]) + "')")
                        conn.commit()
                    #print('INSERT PARA ' + str(users_qualitor[0][0]) + '  -  ATENDIMENTO PARA VISUALIZAÇÃO: ' + str(equipe_qualitor[0][0]))

                #print('INSERT PARA ' + str(users_qualitor[0][0]) + '  -  ATENDIMENTO: ' + str(equipe_qualitor[0][0]))
            else:
                list_export.append((blazon_row[3],blazon_row[4],blazon_row[2],'ERROR - EQUIPE NÃO ENCONTRADA'))
    else:
        list_export.append((blazon_row[3], blazon_row[4], blazon_row[2], 'ERROR - USUÁRIO NÃO ENCONTRADO'))

c.close()

# --------------------------- Abrindo conexão com Qualitor ---------------------------

with open('C:/Automations/insertEqupesQualitor/reports/Insert_equipes_qualitor_blazon.csv', 'w', newline='') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["TAREFA", "NOME", "EQUIPE", "STATUS"])

    for popula_csv in list_export:
        writer.writerow([popula_csv[0],popula_csv[1],popula_csv[2],popula_csv[3]])

print(str(datetime.now().strftime('%d/%m/%Y-%H:%M:%S')+': Fim da atividade'))
