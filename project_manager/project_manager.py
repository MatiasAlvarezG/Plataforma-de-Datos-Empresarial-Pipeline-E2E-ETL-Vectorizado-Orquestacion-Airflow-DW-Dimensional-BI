import click
from os.path import abspath, dirname, join
from utils.methods import *
from utils.ErrorTypes import *


@click.group()
def cli():
    '''Gestor de servicios y DAGs'''
    pass


@cli.command()
@click.argument('servicios', required=False)
def construir_servicios(servicios):
    '''
    Construye los servicios del proyecto. No los inicia.
    '''
    click.echo('INFO ::: Instalando todos los servicios!')
    ejecutar_comando_bash(target='bash', accion=['docker', 'compose', 'up', '--build', '--no-start'], cwd=DIR_PROYECTO)


@cli.command()
@click.argument('eliminar_volumenes', required=False, type=bool)
def eliminar_servicios(eliminar_volumenes):
    '''Elimina todos los contenedores. Pase el valor True para eliminar volúmenes.'''
    click.echo('INFO ::: Eliminado todos los servicios!')
    comando = ['docker', 'compose', 'down', '-v'] \
        if eliminar_volumenes  \
        else ['docker', 'compose', 'down']
    ejecutar_comando_bash(target='bash', accion=comando, cwd=DIR_PROYECTO)


@cli.command()
def iniciar_proyecto(): 
    '''Inicia los servicios y utilidades del proyecto.'''
    click.echo('INFO ::: Iniciando todos los servicios...')
    try:
        procesar_cambio_de_estado(estado='iniciar', SERVICIOS=ALL_SERVICES)
        # Se inicia el Listener para la comunicación entre Airflow y MSSQL.
        _iniciar_listener() 

        # Pdocker exec -it apache_superset bash -c "/home/data/crear_db.sh"
    except CLIException as e:
        # En caso de error se vuelven a cerrar todos los servicos
        click.echo('ERROR ::: Cerrando todos los servicios...')
        procesar_cambio_de_estado(estado='cerrar', SERVICIOS=ALL_SERVICES)
        return False

    click.echo('INFO ::: Se inició el proyecto con éxito!')
    return True


@cli.command()
@click.argument('servicios', nargs=-1, required=True)
def iniciar_servicios(servicios):
    '''
    Inicia uno o más servicios. Usar all-services para todos.
    '''
    if servicios is None:
        raise CLIException('ERROR ::: Se requiere al menos un servicio')
    
    SERVICIOS = ALL_SERVICES if servicios[0] == 'all-services' else servicios

    return procesar_cambio_de_estado(estado='iniciar', SERVICIOS=SERVICIOS)


@cli.command()
@click.argument('servicios', nargs=-1, required=True)
def apagar_servicios(servicios):
    '''
    Apaga uno o más servicios. (Usar el nombre del contendor).
    '''
    if servicios is None:
        raise CLIException('ERROR ::: Se requiere al menos un servicio')
    
    SERVICIOS = ALL_SERVICES if servicios[0] == 'all-services' else servicios

    return procesar_cambio_de_estado(estado='cerrar', SERVICIOS=SERVICIOS)


@cli.command()
def mostrar_servicios_iniciados(): #(servicios_iniciados):
    '''Muestra los servicios que se iniciaron.'''
    click.echo('INFO ::: Mostrando servicios iniciados...')
    ejecutar_comando_bash(target='bash', accion=['docker', 'ps', '--format', '{{.Names}}'])


@cli.command()
@click.argument('dags_id', nargs=-1, required=True)
def procesar_dags(dags_id):
    '''Ejecuta uno o más DAGs (usar ID de los dags).'''
    if dags_id is None:
        raise CLIException('ERROR ::: Se requiere el ID del DAG')
    elif isinstance(dags_id, str):
        dags_id = [dags_id]

    click.echo('INFO ::: Recuerde que el/los DAGs deben estar despausados. Utilice el flag: depausar-dags <dag-id> <dag-id2> ... y luego ejecute de nuevo procesar-dags.')
    for dag in dags_id:
        ACCION = "/usr/bin/docker exec -ti airflow_proyecto_matias_alvarez bash -c"  + f" '/usr/local/bin/airflow dags trigger {dag}'"  
        click.echo(f'INFO ::: Ejecutando : {dag}')

        click.echo(f'INFO ::: Se Ejecutó con éxito : {dag}') \
            if ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True) \
            else click.echo(f'ERROR ::: No se pudo ejecutar con éxito : {dag}')	


@cli.command()
@click.argument('dags_id', nargs=-1, required=True)
def despausar_dags(dags_id):
    '''Pausa uno o más DAGs (Usar ID del DAG).'''
    if dags_id is None:
        raise CLIException('ERROR ::: Se requiere al menos el ID de un DAG.')
    elif isinstance(dags_id, str):
        dags_id = [dags_id]

    for dag in dags_id:
        ACCION = "/usr/bin/docker exec -ti airflow_proyecto_matias_alvarez bash -c"  + f" '/usr/local/bin/airflow dags unpause {dag}'"  

        click.echo(f'INFO ::: Se despausó con éxito : {dag}') \
            if ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True) \
            else click.echo(f'ERROR ::: No se pudo pausar : {dag}')	


@cli.command()
@click.argument('dags_id', nargs=-1, required=True)
def pausar_dag(dags_id):
    '''Despausa uno o más DAGs (Usar ID del DAG).'''
    if dags_id is None:
        raise CLIException('ERROR ::: Se requiere al menos el ID de un DAG.')
    
    for dag in dags_id:
        ACCION = "/usr/bin/docker exec -ti airflow_proyecto_matias_alvarez bash -c"  + f" '/usr/local/bin/airflow dags pause {dag}'"  

        click.echo(f'INFO ::: Se pausó con éxito : {dag}') \
            if ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True) \
            else click.echo(f'ERROR ::: No se pudo pausar : {dag}')	


@cli.command()
@click.argument('dags_id',nargs=-1, required=True)
def estado_de_dag(dags_id):
    '''Muestra el estado de un DAG (Usar ID del DAG)'''
    if dags_id is None:
        raise CLIException('ERROR ::: Se requiere al menos el ID de un DAG.')

    elif isinstance(dags_id, str):
        dags_id = [dags_id]

    for dag in dags_id:
        ACCION = "/usr/bin/docker exec -ti airflow_proyecto_matias_alvarez bash -c"  + f" '/usr/local/bin/airflow dags list-runs -d {dag}'"  

        if ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True) is False:
            click.echo(f'ERROR ::: No se pudo ver el estado del dag : {dag}')	


def _iniciar_listener():
    click.echo('INFO ::: Iniciando Listener...')
    ACCION = '/usr/bin/docker exec -d sql_server_proyecto_matias_alvarez bash -c "nohup python3 /home/python/listener.py > /home/python/listener.log 2>&1 &"'
             
    if ejecutar_comando_bash(target="bash", accion=ACCION, modo_shell=True) is False:
        CLIException('ERROR ::: Hubo un error al iniciar el Listener de MSSQL.')
        
    click.echo('INFO ::: Se ejecutó con éxito el Listener!')
    

@cli.command()
def iniciar_listener(): 
    '''Inicia el Listener.'''
    _iniciar_listener()


@cli.command()
def finalizar_listener():
    '''Finaliza el Listener.'''
    click.echo('INFO ::: Finalizando Listener...')

    ACCION = "/usr/bin/docker exec -d sql_server_proyecto_matias_alvarez bash -c 'kill $(pgrep -f listener.py)'"  

    if ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True) is False:
        CLIException('ERROR ::: Hubo un error al cerrar el Listener de MSSQL.')

    click.echo('INFO ::: Se cerró con éxito el Listener!')


@cli.command()
def ver_dags_id():
    '''Muestra los IDs de los DAGS.'''
    print(''' 
Los IDs de los DAGs son los siguientes: 
  procesamiento_y_carga_all_dataset : Realiza el procesamiento inicial de todos los dataset.
  procesamiento_area_preparacion : Procesa el área de preparación.',
  carga_modelo_dimensional : Carga el modelo dimensional con los datos del área de preparación.
  procesamiento_area_preparacion_y_carga_modelo_dimensional : Procesa el área de preparación y realiza la carga al modelo dimensional.
  proceso_completo_del_proyecto : Realiza el procesamiento de todos los dataset, procesa el área de preparación y carga los datos al modelo dimensional.

Se recomienda utilizar el DAG maestro "proceso_completo_del_proyecto" para orquestar todos los procesos de una sola vez.
''')


@cli.command()
def ver_url_webui():
    '''Muestra las URL del proyecto'''
    ACCION = "hostname -I | awk '{print$1}'"
    IP=ejecutar_comando_bash(target='bash', accion=ACCION, modo_shell=True, retornar_valor=True)
    print(f'''
Las URL de las WebUI son las siguientes:
  Apache Airflow: localhost:8080
  Apache Superset: localhost:9000
En caso que la configuración de su sistema no le permita utilizar localhost para acceder utilice su IP local que es: {IP}
''')


@cli.command()
def ver_informacion_contenedores():
    '''Muestra la información de los contendores del proyecto.'''
    print(f'''
Apache Airflow : 
        Función: Orquestar el flujo de trabajo del proyecto.
	Nombre Del Contendor:'airflow_proyecto_matias_alvarez'
	Nombre Del Host:'airflow'. 
        Puerto: 8080
        Credenciales WebUI: usuario:airflow | contraseña: airflow
	Requerimiento: MySQL iniciado previamente.
MySQL : 
        Función: Base de datos para la metadata de Airflow.
	Nombre Del Contendor:'mysql_proyecto_matias_alvarez' 
	Nombre Del Host:'mysql'.
        Puerto: 3306
	Requerimiento: Ninguno.
SQL Server : 
        Función: Base de datos para el almacen de datos y comunicación entre servicios.
	Nombre Del Contendor:'sql_server_proyecto_matias_alvarez'
	Nombre Del Host:'sql_server'.
        Puerto: 1433
	Requerimiento: Ninguno.
Apache Superset : 
        Función: Crear Dashboard y charts con los datos del modelo dimensional del almacen de datos.
	Nombre Del Contendor:'apache_superset_proyecto_matias_alvarez'
	Nombre Del Host:'apache_superset'.
        Puerto: 9000
        Credenciales WebUI: usuario:superset | contraseña: superset
	Requerimiento: Ninguno.
''')


if __name__ == '__main__':
    DIR_SCRIPT = abspath(dirname(__file__))
    DIR_PROYECTO = join(DIR_SCRIPT, '..')

    ALL_SERVICES = ('mysql_proyecto_matias_alvarez', 
                    'sql_server_proyecto_matias_alvarez',
                    'apache_superset_proyecto_matias_alvarez',
                    'airflow_proyecto_matias_alvarez')
    cli()


