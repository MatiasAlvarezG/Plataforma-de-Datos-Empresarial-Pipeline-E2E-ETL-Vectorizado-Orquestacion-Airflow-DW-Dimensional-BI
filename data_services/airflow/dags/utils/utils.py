from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
from uuid import uuid4
from time import sleep
from ErrorTypes.ErrorTypes import EventFailedError, UndefinedEnvironmentVariable


def __obtener_base_de_datos(env:str='DATABASE_SQL') -> str:
    '''Se obtiene el nombre de la base de datos.'''
    from os import getenv
    # Se obtiene el nombre de la base de datos basado en la variable de entorno DATABASE_SQL
    # La misma fue definida en el DockerFile
    DATABASE  =  getenv(env)#('DATABASE_SQL')

    if DATABASE is None:
        raise UndefinedEnvironmentVariable('ERROR ::: No se estableció la variable de entorno DATABASE_SQL con el nombre de la base de datos!')
    
    return DATABASE


def _enviar_mensaje_mssql(mensaje:str, **kwargs):
    '''
    Tarea que permite enviar un mensaje al contenedor de SQL Server.
    La idea es poder enviar 3 tipos de mensajes:
    1. "crear_db" : Pide que se cree la DB.
    2. "iniciar_modelo" : Pide que se inicie el procesamiento del modelo dimensional.
    3. "procesar_area_procesamiento" : Pide que se inicie solo el procesamiento del área de preparación.
    4. "procesar_y_iniciar_modelo" : Pide que se procese el área de preparación y se cargue posteriormente al modelo.
    5. "cerrar_listener" : Pide que se cierre el listener para evitar consumo de recursos.
    6. "test-comunicacion" : Testea la comunicación.

    Las respuestas pueden ser: 
    - "OK" (Se genero el proceso de manera correcta)
    - "FAIL" (El proceso falló) 
    - "ALREADY DONE" (El proceso ya se realizó). 

    IMPORTANTE: 
    Debe asegurarse un tiempo de espera para que el proceso se lleve a cabo. 
    El método que envía el mensaje debe responsabilizarse de generar un tiempo de espera acorde a su caso de uso.

    El objetivo de esta medida es evitar tener que usar una API (como FastAPI/Flask, etc), usar una cola RabbitMQ, compatir volumen, etc 
     para comunicar necesidades entre airflow y MSSQL
    Se escribe un registro en la base de datos "mensajes" con el tipo de peticion esperada.
    El contendor de MSSQL tiene un Script python que esta esperando peticiones de airflow, una vez recibida alguna inicia el proceso
    designado si es que es posible (por ejemplo, si se pide crear_db pero la DB ya existe no creará la base de datos).
    '''

    # Se obtiene la base de datos
    # DATABASE = __obtener_base_de_datos('DATABASE_SQL_COM')

    #TABLE = 'mensajes'

    # Se crea un ID de correlación para seguir el evento
    CID = str(uuid4())

    # Se define un tiempo de espera de 120 segundos para la creación y procesamiento del área de preparación
    # Para otro caso se define 10 segundos.
    #TIEMPO_ESPERA = 120 if mensaje not in ('cerrar_listener', 'test-comunicacion') else 10 #timedelta(seconds=100)

    # Se define un comando para el envio del mensaje a MSSQL
    COMANDO = f"INSERT INTO mensajes(CID, mensaje, emisor, receptor, fecha_creacion, estado, resultado) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    try:
        hook = kwargs.get('hook')

        if hook is None:
            # Se inicia la conexión. Recordar que MSSQL_CONN_COMM hace referencia a la bd encargada de la comunicación
            # y no al modelo dimensional. Esta definido en el DockerFile de airflow
            hook = MsSqlHook(mssql_conn_id='MSSQL_CONN_COMM')

        # Se envía el mensaje de peticion
        hook.run(COMANDO, parameters=(CID, mensaje, 'airflow', 'mssql', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'pendiente', None))

        # QUERY = f"SELECT TOP(1) resultado FROM mensajes WHERE CID = '{CID}' AND emisor='mssql' AND estado='procesado' ORDER BY fecha_creacion DESC"

        QUERY = f"SELECT resultado FROM mensajes WHERE CID = %s AND emisor=%s AND estado=%s ORDER BY fecha_creacion DESC"

        #sleep(TIEMPO_ESPERA)
        # Se envia un mensaje para obtener la respuesta del evento
        sleep(15)
        RESPUESTA = hook.get_first(QUERY, parameters=(CID, 'mssql', 'procesado')) # hook.get_records(QUERY)

        print(f'RESPUESTA ES: {RESPUESTA}')

        # Extraer el valor escalar de la respuesta
        if RESPUESTA and isinstance(RESPUESTA, (list, tuple)):
            if len(RESPUESTA) > 0 and isinstance(RESPUESTA[0], (list, tuple)):
                RESPUESTA = RESPUESTA[0][0]
            else:
                RESPUESTA = RESPUESTA[0]

        # Convertir a string si es necesario
        if RESPUESTA is not None and not isinstance(RESPUESTA, str):
            RESPUESTA = str(RESPUESTA)

	# Se envia un mensaje de recibiento de respuesta
        hook.run(COMANDO, parameters=(CID, 'Recibido', 'airflow', 'mssql', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'procesado', RESPUESTA))

        # # Se espera un tiempo en caso que se haya ejecutado el evento
        # if RESPUESTA == 'OK': 
        #     # No se utiliza un bucle para esperar el momento exacto de "resolución"
        #     # Esto si bien puede generar un tiempo de espera inecesario permite que MSSQL se acomode a todos los cambios que deba generar internamente
        #     sleep(TIEMPO_ESPERA)

        return RESPUESTA
    
    except Exception as e:
        raise EventFailedError(f'ERROR ::: No se pudo procesar la solicitud: {type(e)} | {e}')

default_args = {
        'depends_on_past': False,
        'email': ['matias_alvarez_data_engineer@hotmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2}
