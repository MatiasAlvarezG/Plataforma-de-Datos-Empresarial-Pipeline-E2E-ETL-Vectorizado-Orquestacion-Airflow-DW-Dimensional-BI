import click
from time import sleep
import subprocess as sp


def _cambiar_estado_servicio(estado:str, SERVICIOS:str|list|tuple, SLEEP=10) -> bool:
    '''Método que genera el cambio de estado de un servicio.'''
    accion = 'start' if estado == 'iniciar' else 'stop'

    if not isinstance(SERVICIOS, (str, list, tuple)):
        click.echo('ERROR ::: Debe pasar una cadena o un iterable con los nombres de los servicios!')
        return False
    
    elif isinstance(SERVICIOS, str):
        SERVICIOS = [SERVICIOS]
    
    tipo_mensaje = 'Iniciando' if estado == 'iniciar' else 'Cerrando'

    for servicio in SERVICIOS:
        click.echo(f'INFO ::: {tipo_mensaje} servicio: {servicio}')
        try:
            proceso_servicios = sp.run(['docker', accion, servicio], stdout=sp.PIPE, check=True)
            if proceso_servicios.returncode != 0:
                click.echo(f'ERROR ::: No se pudo procesar la solicitud en el servicio {servicio}.')
                return False
            sleep(SLEEP)
            click.echo('INFO ::: [OK]!')

        except sp.CalledProcessError as e:
            click.echo(f'ERROR ::: Hubo un error en {servicio}: {e.stderr}')
            return False
        except Exception as e:
            click.echo(f'ERROR ::: Hubo un error inesperado: {str(e)}')
            return False

    return True


def ejecutar_comando_bash(target:str, accion:list, options:str='', modo_shell=False, **kwargs):
    '''
    Método que permite ejecutar un comando bash.
    El parámetro target puede ser: "bash" o el nombre de un contenedor.
    Si es el nombre de un contenedor se realiza un docker exec.
    IMPORTANTE: Para esta versión no se recomienda utilizar el nombre de un contendor!

    Si se utiliza bash entonces se puede utilizar directamente comandos de bash.
    El modo shell permite usar el parámetro "shell" de subprocess.run() (permite: &, &&, |, etc).
    
    El parámetro options permite especificar las opciones de docker exec (-i, -t, etc).
    Debe agregarle el prefijo '-' a la opción. Si son varias opciones utilice un único prefijo,
     por ejemplo: '-ti' para el modo interactivo y pseudo TTY. Por defecto el valor es una cadena vacía.
    '''

    if target != 'bash' and isinstance(accion, str):
        accion = accion.split(' ')

    STDOUT = kwargs.get('stdout', None)
    STDERR = kwargs.get('stderr', None)
    CHECK = kwargs.get('check', True)
    SHELL = modo_shell is True
    CWD = kwargs.get('cwd', None)
    RETORNAR_VALOR = kwargs.get('retornar_valor', False)

    try:
        if target != 'bash':
            comando = ['/usr/bin/docker', 'exec', target, 'bash', '-c'] if not options or options=='' else ['/usr/bin/docker', 'exec', options, target, 'bash', '-c'] # 
            comando.extend(accion) 
        else:
            comando = accion

        if modo_shell:
            if isinstance(accion, (list, tuple)):
                comando = ''.join(str(accion))[1:-1]

        proceso = sp.run(
                    comando, 
                    shell=SHELL,
                    check=CHECK,
                    stdout=STDOUT, 
                    stderr=STDERR, 
                    capture_output=RETORNAR_VALOR,
                    text=RETORNAR_VALOR,
                    cwd=CWD
                )
        
        return proceso.returncode == 0 if RETORNAR_VALOR is False else proceso.stdout
    
    except sp.CalledProcessError as e:
        click.echo(f'ERROR ::: Hubo un error en realizar la acción: {e.stderr}')
        return False
    
    except Exception as e:
        click.echo(f'ERROR ::: Hubo un error inesperado: {str(e)}')
        return False


def procesar_cambio_de_estado(estado, SERVICIOS, throw_exc:bool=False) -> bool:
    '''
    Método que procesa el cambio de estado de uno o más de los servicios definidos en el docker-compose.yml.
    El parámetro estado puede tener los valores: "iniciar", "cerrar". Por defecto es "iniciar".
    El parámetro throw_exc define si debe lanzarse una excepción en caso de falla o retornar False. Por defecto es False.
    '''
    if _cambiar_estado_servicio(estado=estado, SERVICIOS=SERVICIOS) is False:
        if estado == 'iniciar':
            # En caso de error se cierran los servicios que llegaron a iniciarse
            _cambiar_estado_servicio(estado='cerrar', SERVICIOS=SERVICIOS)
            if throw_exc:
                CLIException('ERROR ::: Hubo un error al iniciar alguno o todos los servicios!')
            return False
    return True


