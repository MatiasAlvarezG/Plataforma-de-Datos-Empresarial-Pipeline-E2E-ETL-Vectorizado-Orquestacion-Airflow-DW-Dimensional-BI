# Modo de uso:
# python src/transform/utils/load_db_tmp.py nombre_tabla path_archivo
# python src/transform/utils/load_db_tmp.py 'nombre_tabla1;nombre_tabla2;nombre_tablaN' 'path_archivo1;path_archivo2;path_archivoN'

from sys import argv, exit, path
from os import getenv, remove
from os.path import exists, isfile
from sqlalchemy import create_engine, inspect

path.append('src')
from transform.utils.constants import TMP_TIPO_PRODUCTO, TMP_PRODUCTO, TMP_CANAL_VENTA, TMP_TIPO_GASTO, \
                                      TMP_PROVEEDOR, TMP_SUCURSAL, TMP_CLIENTE, TMP_EMPLEADO, TMP_GASTO, TMP_COMPRA, TMP_VENTA
from transform.utils.transformation_methods import cargar_dataset


def _verificar_tabla(TABLA:list) -> bool:
    '''Método que verifica que la tabla propuesta sea un nombre valido.'''
    for tabla in TABLA:
        if tabla not in [TMP_TIPO_PRODUCTO,
                        TMP_PRODUCTO,
                        TMP_CANAL_VENTA,
                        TMP_TIPO_GASTO,
                        TMP_PROVEEDOR,
                        TMP_SUCURSAL,
                        TMP_CLIENTE,
                        TMP_EMPLEADO,
                        TMP_GASTO,
                        TMP_COMPRA,
                        TMP_VENTA]:
            raise ValueError(f'El nombre de la tabla {tabla} no existe en la base de datos!')
    return True


def _verificar_ruta(RUTA:list)->bool:
    '''Método que verifica la existencia de la ruta y que sea un archivo.'''
    for ruta in RUTA:
        if not exists(ruta):
            raise ValueError(f'No existe la ruta {ruta} especificada!')

        if not isfile(ruta):
            raise ValueError(f'La ruta {ruta} hace referencia a un directorio y no un archivo!')
    return True


def verificar_argumentos(arg)->tuple:
    '''
    Se verifican los argumentos provistos, los mismos deben ser 2:
        1) Debe hacer referencia al nombre de las tablas.
            A) Debe ser una tabla temporal válida

        2) Debe hacer referencia a la rutas de los archivos a cargar.
            A) Debe existir el archivo

    En caso que no se cumpla la verificación de argumentos se lanzará una excepción.
    En caso de que si se cumpla la verificación entonces se retorna una tupla que contiene como componentes una lista con las tablas y rutas correspondiente.
    '''
    N = len(arg)

    # Se verifica cantidad de argumentos
    if not isinstance(arg, list) or N != 2:
        raise ValueError('''ERROR ::: Debe pasar exactamente dos argumentos.
                         Ejemplo de uso:
                            (1) - Se carga una única tabla temporal
                                 python src/transform/utils/load_db_tmp.py nombre_tabla path_archivo
                            (2) - Se carga varias tablas temporales
                                 python src/transform/utils/load_db_tmp.py \'nombre_tabla1;nombre_tabla2;nombre_tablaN\' \'path_archivo1;path_archivo2;path_archivoN\'
                         IMPORTANTE:
                            (1) El separador debe ser únicamente ';' en el caso de querer cargar más de una tabla.
                            (2) Cuando se va a procesar varios archivos debe utilizarse comillas simples: \'tmp_venta;tmp_gasto\' \'path_archivo1;path_archivo2\'.
                            (3) Solo se puede insertar en las tablas temporales.
                         ''')

    # Se obtiene una lista con el nombre de o las tablas/rutas
    TABLAS = arg[0].split(';')
    RUTAS = arg[1].split(';')

    try:
        if _verificar_tabla(TABLAS) and _verificar_ruta(RUTAS):
            return (TABLAS, RUTAS)
    except ValueError as e:
        raise ValueError(f'ERROR ::: {str(e)}')


def obtener_credenciales() -> tuple:
    '''
    Retorna una tupla que contiene la IP del servidor, el nombre de la base de datos, el usuario y contraseña para insertar en las tablas.
    Las variables de entorno deben estar definidas.
    '''
    msg = '''ERROR ::: Falta definir una o más variables de entornos, las mismas deben ser:
                       $IP_SERVER_SQL : IP del servidor, no hace falta el puerto
                       $DATABASE : Nombre de la base de datos.
                       $USERNAME : Nombre del usuario SQL.
                       $PASSWORD : Contraseña del usuario SQL.'''
    try:
        IP_SERVER =  getenv('IP_SERVER_SQL')
        DATABASE  =  getenv('DATABASE_SQL')
        USERNAME  =  getenv('DB_USER_PYTHON')
        PASSWORD  =  getenv('DB_PWD_PYTHON')

        if None in [IP_SERVER, DATABASE, USERNAME, PASSWORD]:
            print(f'''IP_SERVER : {IP_SERVER}
                      DATABASE  : {DATABASE}
                      USERNAME  : {USERNAME}
                      PASSWORD  : {PASSWORD}''')
            raise TypeError(msg)
        return (IP_SERVER, DATABASE, USERNAME, PASSWORD)
    except KeyError as e:
        raise KeyError(msg) from e
    except Exception as e:
        raise Exception(f"ERROR ::: Error al obtener las credenciales: {str(e)}") from e
    

def crear_conexion_db(ip, db, user, pw, port=1433):
    '''Se retorna un engine para gestionar las conexiones.'''
    try:
        URI = f'mssql+pymssql://{user}:{pw}@{ip}:{port}/{db}'
        engine = create_engine(URI)
    except ValueError as e:
        raise ValueError(f"ERROR ::: Error en la URI de conexión: {e}") from e
    except Exception as e:
        raise Exception(f"ERROR ::: No se pudo conectar a la base de datos: {str(e)}") from e
    return engine 

def cargar_tabla(DATOS, engine):
    '''Se carga la información a la tabla correspondiente. Al finalizar se elimina el archivo de la ruta establecida.'''

    ## Como se utiliza to_sql() de pandas no es necesario pero si se deja de utilizar la librería en 
    ##  algún momento se debería utilizar este método para poder crear la query dinámicamente para cada tabla.
    # def obtener_esquema_tabla(tabla, engine):
    #     # Se obtiene todas la información de las columnas de la tabla
    #     columnas = inspect(engine).get_columns(tabla)

    #     # Se obtiene una lista que contiene solo los nombres de las columnas
    #     get_nombres = [columna['name'] for columna in columnas]

    #     # Se obtiene una cadena que hace referencia a la cantidad de parámetros a asignar
    #     get_parametrizacion = ', '.join('?' for _ in get_nombres)

    #     return  f"INSERT INTO {tabla} ({', '.join(get_nombres)}) VALUES ({get_parametrizacion})"

    for tabla, ruta in DATOS:
        try:
            # Se carga el dataset
            try:
                df = cargar_dataset(ruta, extension='csv', dtype=None)
            except FileNotFoundError:
                raise FileNotFoundError(f"ERROR ::: El archivo {ruta} no se encuentra.")
            except Exception as e:
                raise Exception(f"ERROR ::: Error al cargar el archivo {ruta}: {str(e)}")

            try:
                with engine.connect() as connection:
                    # Se carga el DataFrame a SQL
                    df.to_sql(name=tabla, con=engine, index=False, method='multi', if_exists='append', chunksize=1000)
                    print(f'INFO ::: Se cargo exitosamente la tabla {tabla}!')
            except Exception as e:
                raise Exception(f"ERROR ::: Error al cargar la tabla {tabla} en la base de datos: {str(e)}")

            try:
                # Se elimina el archivo
                remove(ruta)
                print(f'INFO ::: Se eliminó exitosamente el archivo {ruta}!')
            except FileNotFoundError:
                raise FileNotFoundError(f"ERROR ::: No se pudo encontrar el archivo para eliminar: {ruta}")
            except PermissionError:
                raise PermissionError(f"ERROR ::: Permiso denegado para eliminar el archivo: {ruta}")
            except Exception as e:
                raise Exception(f"ERROR ::: Error al eliminar el archivo {ruta}: {str(e)}")

        except Exception as e:
            print(f'ERROR ::: Al intentar cargar la tabla {tabla}: {str(e)}')
            raise

def main(DATOS) -> None:
    try:
        # Se obtiene las credenciales de la base de datos
        IP_SERVER, DATABASE, USERNAME, PASSWORD = obtener_credenciales()

        # Se crea la conexión a la base de datos
        engine = crear_conexion_db(IP_SERVER, DATABASE, USERNAME, PASSWORD)

        # Se carga a la base de datos
        cargar_tabla(DATOS, engine)

    except Exception as e:
        raise f'TYPE: {type(e)} : {e}'

if __name__ == '__main__':
    # Se verifican los argumentos
    TABLAS, RUTAS = verificar_argumentos(argv[1:])

    # Se inicia el proceso de carga
    main(zip(TABLAS,RUTAS))

    exit(0)


