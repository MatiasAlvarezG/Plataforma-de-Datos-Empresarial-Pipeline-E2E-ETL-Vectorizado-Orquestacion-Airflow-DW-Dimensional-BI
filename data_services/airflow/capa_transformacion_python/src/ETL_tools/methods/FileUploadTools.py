# Métodos relacionados a la carga de DataFrames
from pandas import read_csv, read_excel, concat, DataFrame
from chardet import  detect
import os


def obtener_separador( path : str, 
                       encoding : str = 'utf-8', 
                       candidatos : list = [',',';','|','\t',':'],
                       try_solve_encoding : bool = True,
                       strict_mode : bool = False #strict_mode : bool = True
                       ) -> str:

    '''
    Este método devuelve el separador utilizado en un archivo de texto plano. Dependiendo del modo estricto, 
    puede generar una excepción o devolver None si no se encuentra un separador. 
    Se recomienda que el archivo tenga un encabezado, aunque puede ser utilizado de todas formas. 
    Sin embargo, no es posible distinguir entre archivos que contienen una sola columna de los que no lo hacen.
    Esto significa que si se procesa un archivo y no se sabe si tiene una columna, el método podría devolver 
    None o lanzar una excepción, lo que hace imposible determinar si no se encontró un separador o si el archivo
    tiene una sola columna. Esta ambigüedad puede complicar el proceso automático de carga de información 
    en módulos como pandas al utilizar el método leer_archivo(), por lo que se recomienda que si sabe que su 
    archivo es unicolumnar simplemente pase el separador ',' (o cualquier otro, a excepción de: '\\n') en el
    método leer_archivo(...,sep=',')
    
    Parameters:
    -----------
        - path : str 
            Ruta absoluta del archivo a procesar.
        - encoding : str
            Especifica el encoding del archivo .
            Su valor por defecto es 'utf-8'
        - candidatos : list
            Especifica los separadores a probar.
            Si se llega a pasar una cadena entonces lanzara una excepción TypeError.
            Su valor por defecto es la lista: [',',';','|','\t',':']
        - try_solve_encoding : bool
            Especifica que debe intentarse solucionar el encoding si se encuentra con un problema de ese tipo
            caso contrario retorna un raise de tipo UnicodeDecodeError.
            Su valor por defecto es True.
        - strict_mode : bool [DESPRECIADO]
            Si es verdadero se garantiza:
                Encontrar el separador correspondiente asegurando que no haya un separador distinto entre valores
                En caso de no hacerlo retornara un Raise
                NOTA: Si el archivo contiene una única columna entonces no podrá encontrar un separador y retornara una excepción 
            Si es falso:
                Si encuentra un separador no verifica que sea consistente por todo el archivo
                En caso de no encontrar un separador no retorna una excepción y retorna None
            Puede activar el strict_mode cuando considere que los archivos no van a tener una inconsistencia 
            en sus separadores.
            Su valor por defecto es False.

    Returns:
    -----------
        - sep : str
            Separador 

    Excepcions:
    -----------
        - ValueError : La ruta del archivo es nula.
        - FileNotFoundError : El archivo no existe en la ruta pasada por parámetro.
        - UnicodeDecodeError : El archivo no pudo procesarse porque su encoding pasado es incorrecto.
        - TypeError : 
                   El archivo no se pudo procesar posiblemente porque no era un archivo plano.
                   El parámetro ´candidatos´ no es de tipo list como esperaba.

    '''
    #Verifica que el separador aparece la misma cantidad (menos una unidad) que las columnas en todas las lineas
    def verificar_integridad_separador(file, columnas_totales : int, separador : str) -> bool: 
        for linea in file.readlines():
            if columnas_totales != linea.count(separador):
                return False
        return True
    
    if path is None:
        raise ValueError(f'ERROR ::: El parámetro path fue pasado como valor Nulo.')
    
    if encoding is None:
        encoding='utf-8'

    if not isinstance(candidatos, list):
        raise TypeError('ERROR ::: Los valores para posibles candidatos deben pasarse como una lista donde cada\
                        elemento corresponde a un separador distinto.')
    
    if candidatos is None or len(candidatos)==0:
        candidatos = [',',';','|','\t',':']

    if strict_mode:
        print('INFO ::: El modo estricto fue desactivado por generar problemas. Se volverá a activar en una versión futura!')
        strict_mode = False

    try:
        with open(path, mode='r', encoding=encoding, newline='') as file:
            for sep in candidatos:
                encabezado = file.readline()
                cant_separadores = encabezado.split(sep) 
                if len(cant_separadores) > 1:
                    if strict_mode:
                        if verificar_integridad_separador(file, 
                                                          len(cant_separadores) - 1, 
                                                          sep):
                            return sep
                        else:
                            raise ValueError(f'ERROR ::: El separador es inconsistente, no se puede garantizar el separador.')
                    else: return sep

            #Fin del bucle, no se encontro un separador
            if strict_mode:
                raise TypeError(f'ERROR ::: No se pudo detectar el separador.')
            return None
                
    except FileNotFoundError:
        raise FileNotFoundError(f'ERROR ::: No existe el archivo: {os.path.basename(path)} en {os.path.dirname(path)}.')
    except UnicodeDecodeError as e: 
        if try_solve_encoding:
            print('INFO ::: Archivo con encoding incorrecto - Recreando Archivo con encoding \'UTF-8\'')
            to_utf8(path, detectar_encoding(path))
            return obtener_separador(path, strict_mode=strict_mode)
        else:
            error_message = f'ERROR ::: Archivo con encoding incorrecto: {e}'
            raise UnicodeDecodeError(e.encoding, e.object, e.start, e.end, error_message)
            # raise e
    except :
        raise TypeError(f'ERROR ::: No se pudo procesar el archivo {os.path.basename(path)} en {os.path.dirname(path)} por causa desconocida')


#### METODOS DE DETECCION Y RESOLUCION DE ENCODING
def detectar_encoding(path : str) -> str:
    '''
    Método que detecta el tipo de encoding de un archivo CSV.
    '''
    if path is None:
        raise ValueError(f'ERROR ::: El parámetro path fue pasado como valor Nulo.')
    try: 
        with open(path, 'rb') as archivo:
            resultado = detect(archivo.read())
        return resultado['encoding']
    except FileNotFoundError:
        raise FileNotFoundError(f'ERROR ::: No existe el archivo: {os.path.basename(path)} en {os.path.dirname(path)}.')
    except Exception as e:
        raise type(Exception)(f'ERROR ::: Hubo un error al procesar el archivo: {e}.')


def to_utf8(path : str, encoding : str = 'utf-8'):
    '''
    Transforma un archivo que esta no esta encodeado en 'UTF-8' y retorna un nuevo archivo que si lo esta.
    NOTA : PISA EL ARCHIVO ORIGINAL.

    Parameters
    ------
       - path : str
            Ruta del archivo a solucionar.
       - encoding : str
            Encoding Original del archivo, en caso de no pasarse se retornara una excepción.
    '''
    if path is None:
        raise ValueError(f'ERROR ::: El parametro path fue pasado como valor Nulo.')
    
    if not isinstance(path, str):
        raise TypeError(f'ERROR ::: El parámetro path debe ser una cadena.')   
    
    if encoding is None:
        raise ValueError(f'ERROR ::: El parámetro encoding fue pasado como valor Nulo.')
    
    if not isinstance(encoding, str):
        raise TypeError(f'ERROR ::: El parámetro encoding debe ser una cadena.')   
    try:    
        with open(path, 'rb') as archivo:
            # Leer el contenido del archivo
            contenido = archivo.read()

            # Decodificar el contenido con el encoding inicial
            decoded_content = contenido.decode(encoding)

            # Codificar el contenido con UTF-8
            utf8_content = decoded_content.encode('utf-8')

        # Se guarda el archivo encodeado en utf-8
        with open(path, 'wb') as archivo:
            archivo.write(utf8_content)

        print(f'INFO ::: {path} fue encodeado a UTF-8.')

    except UnicodeDecodeError as e:
        error_message = f'ERROR ::: Error de decodificación: {e}.'
        raise UnicodeDecodeError(e.encoding, e.object, e.start, e.end, error_message)
    
    except UnicodeEncodeError as e:
        print(f'INFO ::: Error de codificación: {e} intentando resolver.')
        to_utf8(path, encoding='UTF-16')
    except FileNotFoundError:
        raise FileNotFoundError(f'ERROR ::: El archivo no fue encontrado: {path}.')
    except Exception as e:
        raise type(e)(f'ERROR ::: Hubo un error al procesar el archivo: {e}.')
    

#### Método de lectura de csv 

def leer_archivo( path : str, 
                  type_file : str = 'csv', 
                  sep : str = None, 
                  usecols = None, 
                  dtype = 'str', 
                  strict_mode : bool = False)  -> DataFrame: #strict_mode : bool = True)  -> DataFrame:
    '''
    Retorna un DataFrame del archivo que reside en la ubicación ´path´.
    Los archivos deben ser planos con una estructura de separadores entre sus valores o pueden ser archivos excel con una o varias hojas.
    Inicialmente se asume que todos los archivos tienen un encoding UTF-8 en caso que no lo sea se intentara encodear a UTF-8.
    Si no se pasa un separador se intentara encontrar el separador correcto, además se garantiza que la información sea consistente en todo
    el DataFrame ya que el modo estricto garantiza que no falta o haya ningún otro valor como separador. 
    Puede desactivarlo pasando el strict_mode como ´False´.

    Parameters
    ------
    - path : str
        Ruta del archivo a cargar como DataFrame.
    - type_file : str 
        Tipo de archivo: csv ó xlsx.
        Por defecto su valor es 'csv'.
    - sep : str 
        Separador del archivo csv.
        Por defecto su valor es None.
    - usecols : list
        Columnas a utilizar del archivo.
        Por defecto su valor es None.
    - dtype : str
        El tipo de valor que tendrá cada columna, por defecto es str.
        Por defecto su valor es 'str'.
    - strict_mode : bool [DESPRECIADO]
        En caso de utilizar un archivo csv y deba encontrarse el tipo de separador entonces 
        puede elegir si desea garantizar que el separador encontrado es consistente por todo el
        archivo o no y simplemente retornar la posible solución ahorrando tiempo.
        Por defecto su valor es False.
    '''
    if path is None:
        raise ValueError(f'ERROR ::: El parámetro path fue pasado como valor Nulo.')
    
    if not isinstance(path, str):
        raise TypeError(f'ERROR ::: El parámetro path debe ser una cadena.')   
    
    if sep == '\n':
        raise ValueError('ERROR ::: No se puede pasar el delimitador salto de linea (\n).')

    if usecols is not None:
        if not isinstance(usecols,list):
            raise TypeError('ERROR ::: el parámetro usecols debe ser una lista.')
    
        #usecols vacio hace que no se lea ninguna columna
        if len(usecols) == 0:
            usecols = None

    df = None
    try: 
        if type_file.lower() in ['csv','txt']:
            df = read_csv(path, 
                          sep = obtener_separador(path,
                                                  strict_mode=strict_mode
                                                  ) 
                                  if sep is None else sep,
                          encoding = 'utf-8',
                          usecols = usecols,
                          dtype = dtype)
        else: # xlsx
            sheets = read_excel(path, sheet_name=None) 
            df = concat(sheets.values(), ignore_index=True)
    except UnicodeDecodeError as e: 
        print('INFO ::: Archivo con encoding incorrecto - Recreando Archivo con encoding \'UTF-8\'.')
        to_utf8(path, detectar_encoding(path))
        return leer_archivo(path, type_file, sep)
    except FileNotFoundError as e:
        raise FileNotFoundError(f'ERROR ::: El archivo no fue encontrado: {path}.')
    except Exception as e:
        ERROR = type(Exception)
        raise ERROR(f'ERROR ::: No se pudo leer el archivo por un error inesperado: {e}.')
    if df is None:
        raise ValueError(f'ERROR ::: Hubo un error al procesar el dataframe y no fue procesado por ninguna opción aceptada (csv ó xlsx).')
    return df




































































# Matias Alvarez - Data Engineer

