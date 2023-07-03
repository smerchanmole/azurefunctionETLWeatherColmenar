import azure.functions as func
import logging
import pandas as pd
import requests
import json
import time
import datetime
# importamos la libreria de conexion a postgres. Antes hay que instalarla con pip3 install psycopg2-binary
import psycopg2
#Importanmos la libreria con las conexions a nuestra bbdd
import sys
from colorama import init as colorama_init
from colorama import Fore
from colorama import Style
from markupsafe import escape
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os


# code for accessing ddbb
# ## Funciones para el acceso a base de datos

# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #


# ####################################################################### #
# FUNTION: conectar_db                                                    #
# DESCRIPTION: Generate a connection to the database (postgreSQL)         #
# INPUT: Data needed to connect and the inital connection query           #
# OUTPUT: Cursor and Connection,  print error if happens                  #
# ####################################################################### #
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    conn=None
    cur=None
    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Open the cursor and launch initial query
        cur = conn.cursor()

        # Query execution
        cur.execute(PS_QUERY)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            cur=""
            conn=""
        else:
            cur=""
            con=""

    return cur, conn

# ####################################################################### #
# FUNTION: cerrar_conexion_bbdd                                           #
# DESCRIPTION: Close the connection                                       #
# INPUT: Data needed to close                                             #
# OUTPUT: Nothing                                                         #
# ####################################################################### #
def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    PS_CURSOR.close()
    PS_CONN.close()

# ####################################################################### #
# FUNTION: escribir_log                                                   #
# DESCRIPTION: Write the operation in a log table (just for info)         #
# INPUT: Data needed to write the log                                     #
# OUTPUT: 1                                                               #
# ####################################################################### #
def escribir_log (PS_CURSOR, PS_CONN, ip, comando, extra):
    # Escribimos el mensaje en la tabla logs.
    x=datetime.datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertLOG="INSERT INTO public.logs (hora, ip, comando, extra) values ('"+str(x.isoformat())+"','"+ip+"','"+comando+"','"+extra+"')"
    # print (InsertLOG)

    PS_CURSOR.execute(InsertLOG)
    return 1


app = func.FunctionApp()
@app.function_name(name="etlweathersensor")
@app.route(route="etl")
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    # ----------------------- MY CODE ---------------------------------
    # ----------------------- MY CODE ---------------------------------
    # ----------------------- MY CODE ---------------------------------

    database_ip = "YOURDATABASEIP"
    database_port = 5432  # your database port
    database_db = "YOUR DATABASE"
    database_user = "YOURUSER"
    database_password = "YOURPASS"

    EMAIL_SERVER = "YOUR EMAIL SERVER"
    EMAIL_ALERT = "YOUR ALERT EMAIL ADDRESS"

    http_body = 'FUNCTION TO ETL AEMET WHEATHER DATA SENSORs \n '
    http_body += '*******************************************:\n\n'

    http_body += '* EXTRACCION:\n '
    mail_body = "Hola Santiago:\nLa salida de la ETL es:\n "
    # ## Acceso a Open Data de AEMET
    # #### Lo primero que hacemos es llamar a la primera URL de la AEMET, que nos da las URL con los datos de vuelta (si todo va bien)
    #
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** ACCESSO A DATOS DE LA API OPEN DATA DE AEMET    ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")

    # Key es el key de la AEMET
    key= YOURKEY!
    # URI tiene la informacion de la peticion que vamos a hacer (cambia para cada llamada)
    uri = "opendata/api/observacion/convencional/datos/estacion/3191E/?api_key="  # prediccion de Manzanares el Real.

    # montamos la URL y mostramos para ver que esta ok
    uri_total = "https://opendata.aemet.es/" + uri + key
    print(f"{Fore.GREEN}*** La URL es:{Style.RESET_ALL}{Fore.YELLOW}{uri}YOUR_API_KEY_HERE{Style.RESET_ALL}")

    http_body += '  - La URL que nos dira la URL de los datos y de los metadatos:\n'
    http_body += '  - ' + uri_total + '\n'
    mail_body += "SOBRE EXTRACCION: \n   La URL que nos dira la URL de los datos y de los metadatos:"
    mail_body += uri_total

    # hacemos la llamada y capturamos los datos en un objeto response, si lo imprimimos nos dice el resultado de HTTP
    response = requests.get(uri_total, verify=False)
    print(f"{Fore.GREEN}*** La respuesta es:{Style.RESET_ALL}{Fore.YELLOW}{response}{Style.RESET_ALL}")

    http_body += ' - La respuesta del servidor web es: ' + str(response.status_code) + '\n'
    mail_body += "\n   La respuesta del servidor web es:" + str(response.status_code)

    # Convertimos esa salida que es un JSON a un objeto JSON
    json_response = json.loads(response.text)
    print(
        f"{Fore.GREEN}*** El Objeto JSON de respuesta nos dira la URL donde estan los datos climatologicos y sus metadatos:{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{json_response}{Style.RESET_ALL}")

    # Imprimimos el campo que tiene la URL buena
    print(f"{Fore.GREEN}*** Los datos estan en la URL a la que apunta el campo json[datos]{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{json_response['datos']}{Style.RESET_ALL}")

    http_body += ' - La URL de la que cogemos los datos: '
    http_body += json_response['datos'] + '\n'
    mail_body += "SOBRE EXTRACCION: \n   La URL de la que cogemos los datos: " + json_response['datos']

    # ### Capturamos ya los datos de la URL buena y los pasamos a un JSON (1º transformación) que nos permitira ponerlo en un dataframe (pandas)
    # capturamos los datos de la URL buena, comprobando que ha cogido bien la información y lo pasamos a un nuevo json
    # Esta URL tiene los datos meteorologicos de las últimas 24h, e decir, 24 entradas. Y TIENE UN DESFASE DE 2 o 3 HORAS menos que la actual.

    response2 = requests.get(json_response["datos"], verify=False)
    print(
        f"{Fore.GREEN}*** Repuesta de la URL con los datos:{Style.RESET_ALL}{Fore.YELLOW}{response2}{Style.RESET_ALL}")
    json_response2 = json.loads(response2.text)
    # print("*** Datos Json de la sonda mas cercana a la zona de riego (IDEMA:3191E):")
    # print(json.dumps(json_response2, indent=3))
    http_body += ' - La respuesta del servidor web es: ' + str(
        response2.status_code) + '\n\n'
    mail_body += "\n   La respuesta del servidor web es:" + str(response2.status_code)

    http_body += '* TRANSFORMACION:\n>'
    mail_body += "\nSOBRE TRANSFORMACION:\n"

    # lo pasamos a pandas con un dataFrame
    df = pd.DataFrame(json_response2)

    print(f"{Fore.GREEN}*** Datos introducidos en un Panda DataFrame:{Style.RESET_ALL}")
    #print (df)

    # mostramos las columnas para luego poder ordenarlas como queremos
    # print ("*** Listado de las columnas")
    # print (df.columns.tolist())
    print("")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** DATOS CLIMATOLOGICOS DE LA SONDA                ***{Style.RESET_ALL}")
    print(f"{Fore.CYAN}*** *********************************************** ***{Style.RESET_ALL}")
    print("")

    # ordenamos las columnas
    columnas_ordenadas = ['fint', 'idema', 'ubi', 'lat', 'lon', 'alt', 'prec', 'vmax', 'dmax', 'vv', 'dv', 'hr', 'pres',
                          'ta', 'tamin', 'tamax']
    print(f"{Fore.GREEN}*** Columnas ordenadas como queremos:{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{df[columnas_ordenadas].to_markdown(tablefmt='grid')}{Style.RESET_ALL}")
    http_body += df[columnas_ordenadas].to_markdown(tablefmt='grid')
    mail_body += df[columnas_ordenadas].to_markdown(tablefmt='grid')

    http_body += '\n\n* CARGA:'
    mail_body += "\nSOBRE CARGA EN BBDD:\n "

    # ## Metermos los datos en postgre SQL.
    # #### Como puede haber datos repetidos por horas, hacemos un "upsert"

    ##insertamos las ultimas 24 horas en la tabla con un UPSERT

    # cargamos los datos de conexion

    # definimos la operacion SQL, y si existe esa linea, no ejecutes el insert (nos fiamos del dato que hay)
    SQLupsert = "insert into public.sonda_colmenar (fint, idema, ubi,lat,lon,alt,prec,vmax,dmax,vv,dv,hr,pres,ta,tamin,tamax) values "
    SQLupsert_end = " on conflict(fint) do nothing"

    # conectamos con la tabla
    cur, con = conectar_bd(database_ip, database_port, database_user, database_password, database_db,"select 1")
    if con=="":
        http_body += '\n\n*********** ERROR AL CONECTAR CON LA BBDD ************:'
        return func.HttpResponse(http_body)


    print(f"{Fore.GREEN}*** Realizando la escritura en BBDD ***{Style.RESET_ALL}")

    for index, fila in df.iterrows():
        cadena = "('"
        cadena += fila['fint'] + "','" + fila['idema'] + "','" + fila['ubi'] + "'," + \
                  str(fila['lat']) + "," + str(fila['lon']) + "," + str(fila['alt']) + \
                  "," + str(fila['prec']) + "," + str(fila['vmax']) + "," + str(fila['dmax']) + \
                  "," + str(fila['vv']) + "," + str(fila['dv']) + "," + str(fila['hr']) + "," + \
                  str(fila['pres']) + "," + str(fila['ta']) + "," + str(fila['tamin']) + "," + \
                  str(fila['tamax'])
        cadena += ')'
        SQL = SQLupsert + cadena + SQLupsert_end

        # print("*** Consulta SQL:")
        # print(SQL)

        fallos_sonda = SQL.count("nan")

        if fallos_sonda > 0:
            print("HAY UN VALOR NO VALIDO, OBVIAMOS ENTRADA.")
        else:
            # print ("")#ejecutamos el upsert
            cur.execute(SQL)

    # al final del for actualizamos logs
    print(f"{Fore.GREEN}*** Actualizamos el log con el datos sondas {Style.RESET_ALL}")
    escribir_log(cur, con, "Azure", "ACTUALIZAMOS SONDA_COLMENAR", "operacion normal")

    con.commit()  # hay que hacer esto para que se escriban

    ##cerramos la conexion
    cerrar_conexion_bbdd(cur, con)

    http_body += '\n - Datos escritos en BBDD\n'
    mail_body += "Datos escritos en la BBDD correctamente. \n\n FIN DEL PROGRAMA.\n Que pase un buen dia, su asistente:\n JARBIS."

    # Le mandamos un correo a quien esté configurado si nos pasamos en algun limite

    # create message object instance
    print("-SENDING EMAIL:")
    msg = MIMEMultipart()

    message = mail_body

    # setup the parameters of the message
    password = "your_password"
    msg['From'] = "alertas@m.com"
    msg['To'] = EMAIL_ALERT
    msg['Subject'] = "RESUMEN ETL DATOS SONDA AEMET"

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    # create server
    server = smtplib.SMTP(EMAIL_SERVER + ': 2525') #el puerto 25 lo cierra el FW

    # server.starttls()

    # Login Credentials for sending the mail
    # server.login(msg['From'], password)

    # send the message via the server.
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()

    print("   *** successfully sent email to %s:" % (msg['To']))

    print(f"{Fore.BLUE}*** FIN DEL PROGRAMA ***{Style.RESET_ALL}")
    http_body += '* LOG\n - Correo enviado'

    return func.HttpResponse(http_body)
