#####################################################################################################################################################
# Este script se utiliza para permitir a airflow procesar los DAGs de forma correcta.                                                               #
# Permite:                                                                                                                                          #
# 1) Crear toda la base de datos si la misma no fue creada previamente.                                                                             #
# 2) Ejecutar el procesamiento del modelo dimensional.                                                                                              #
# 3) Cerrar este script en caso que ya no se necesite.                                                                                              #
#                                                                                                                                                   #
# Los mensajes a recibir son:                                                                                                                       #
#    1. "crear_db" : Define que se debe crear la base de datos junto a las tablas, triggers, privilegios, procedimientos, etc.                      #
#                    En caso que ya existe no se crea.                                                                                              #
#    2. "iniciar_modelo" : Define que se carga el modelo dimensional con los datos del área de preparación.                                        #
#    3. "procesar_area_procesamiento" : Se define que se debe realizar el procesamiento del área de preparación.                                    #
#    4. "procesar_y_iniciar_modelo" : Se define que se realice el procesamiento del área de preparación y se cargue posteriormente al modelo.       #
#    5. "cerrar_listener" : Define que se cierre el listener para evitar consumo de recursos.                                                       #
#    6. "test-comunicacion" : Testea la comunicación.                                                                                               #
#                                                                                                                                                   #
# El objetivo es evitar gastar más recursos para que se comuniquen los servicios, lo ideal sería generar una estrategia como                        #
# utilizar un broker de procesamiento, crear una API para gestionar las peticiones u cualquier otra estrategia.                                     #
# Dada la naturaleza del proyecto esta solución simple es suficiente para mi caso de uso.                                                           #
#####################################################################################################################################################

from datetime import datetime
from os import getenv
import pymssql
import subprocess
from sys import exit
from time import sleep


def enviar_mensaje(respuesta:str='NO SE DEFINIÓ RESPUESTA') -> bool:
    '''Envía un mensaje de respuesta al evento.'''

    # Se realiza la ejecución de un Script
    FECHA_ACTUAL=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    QUERY=f"INSERT INTO {TBL_MENSAJE}(CID, mensaje, emisor, receptor, fecha_creacion, estado, resultado) VALUES ('{CID}', '{mensaje}', 'mssql', '{emisor}', '{FECHA_ACTUAL}', 'procesado', '{respuesta}')"
    
    try:
        cursor.execute(QUERY)
        conn.commit()
        print(f'mensaje enviado:{QUERY}')
        return True
    except Exception as e:
        print(f'ERROR ::: No se pudo enviar el mensaje: {respuesta}! ERROR: {e}')
        return False


# Variables de entornos necesarias
DB_USER=getenv('DB_USER_ADM') 
DB_PASS=getenv('DB_PWD_ADM') 
DB_NAME_COM='comunication_db'

# Información de la DB
TBL_MENSAJE = 'mensajes'

# Variables de tiempo
TIEMPO_DE_ESPERA = 5 # segundos, cambielo si lo desea pero esta cantidad esta bien
PROCESAR_DESDE = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

try:
    conn = pymssql.connect('sql_server', DB_USER, DB_PASS, DB_NAME_COM)
    cursor = conn.cursor()
except Exception as e: 
    raise ConnectionError(f'ERROR ::: {type(e)} : {e}')

# Solo se especifica como emisor a airflow porque es lo que necesito para mi caso de uso
# En caso que el proyecto se extendiera y siguiera decidiendo que el diseño de este script es suficiente
# En vez de utilizar una estrategia mejor simplemente se elimina el emisor='airflow'.
SQL_QUERY_ESCUCHA = f"SELECT * FROM {TBL_MENSAJE} WHERE emisor='airflow' AND fecha_creacion>'{PROCESAR_DESDE}'"

while True:
    SQL_QUERY_ESCUCHA = f"SELECT CID, mensaje, emisor, receptor, fecha_creacion, estado, resultado FROM {TBL_MENSAJE} WHERE emisor='airflow' AND fecha_creacion>'{PROCESAR_DESDE}'"
    sleep(TIEMPO_DE_ESPERA)

    cursor.execute(SQL_QUERY_ESCUCHA)
    consulta = cursor.fetchone()

    if consulta is not None:
        # Se obtienen todos los valores
        CID = consulta[0]
        mensaje = consulta[1]
        emisor = consulta[2]
        receptor = consulta[3]
        fecha_creacion = consulta[4]
        estado = consulta[5]
        resultado  = consulta[6]

        try:
            match mensaje:
                case 'crear_db':
                    cursor.execute(f"SELECT name FROM master.dbo.sysdatabases WHERE name = 'proyecto_matias_alvarez'")
                    EXISTE_DB = cursor.fetchone() is not None

                    if not EXISTE_DB:
                        print('INFO ::: La DB no existe... Inicando script "cargar_script.sh ..."')
                        proceso = subprocess.run(['/home/bash_script/cargar_script.sh'], stdout=subprocess.PIPE)
                        enviar_mensaje(respuesta='OK' if proceso.returncode == 0 else 'FAIL')
                    else: 
                        print('INFO ::: La DB ya fue creada previamente!')
                        enviar_mensaje(respuesta='ALREADY DONE')

                case 'iniciar_modelo':
                    print('INFO ::: Iniciando carga del modelo con las tablas del área de presentación ya procesadas..."')
                    proceso = subprocess.run(['/home/bash_script/cargar_modelo_dimensional.sh'], stdout=subprocess.PIPE)
                    enviar_mensaje(respuesta='OK' if proceso.returncode == 0 else 'FAIL')

                case 'procesar_area_procesamiento':
                    print('INFO ::: Procesando area de preparacion ..."')
                    proceso = subprocess.run(['/home/bash_script/procesar_area_de_preparacion.sh'], stdout=subprocess.PIPE)
                    enviar_mensaje(respuesta='OK' if proceso.returncode == 0 else 'FAIL')

                case 'procesar_y_iniciar_modelo':
                    print('INFO ::: Procesando area de preparacion y cargar al modelo dimensional..."')
                    proceso = subprocess.run(['/home/bash_script/procesar_y_cargar_modelo_dimensional.sh'], stdout=subprocess.PIPE)
                    enviar_mensaje(respuesta='OK' if proceso.returncode == 0 else 'FAIL')

                case 'cerrar_listener':
                    print('INFO ::: Cerrando listener')
                    enviar_mensaje(respuesta='OK')
                    exit(0)

                case 'test-comunicacion':
                    print(f'INFO ::: Se confirma comunicacion con {emisor}!')
                    enviar_mensaje(respuesta='OK')

                case _:
                    pass
            
            PROCESAR_DESDE=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        except subprocess.CalledProcessError as e:
            enviar_mensaje(proceso=None, mensaje='ERROR')
            print(f'ERROR ::: No se pudo ejecutar la solicitud: {e}')
            continue


