# Métodos de verificación
from sys import path
path.append('src')

from pandas import DataFrame
from ETL_tools.methods.FileUploadTools import *
from os.path import join
from transform.utils.constants import DIR_PROCESAR


def verificar_columna_int(df : DataFrame, 
                          nombre_columna : str,
                          try_cast : bool = False
                          ):
    '''
    Retorna True si los elementos de una columna son de tipo int, en caso de no contener elemento enteros retorna False
    El método puede intentar solucionar el tipo de dato de la columna si se habilita el parámetro try_cast como True.
    En caso de utilizar try_cast el mismo intentara cambiar el tipo de datos de la columna y retorna True en caso de lograrlo
    caso contrario retornara False y la columna seguirá siendo del mismo tipo como era inicialmente.
    '''
    if df is None:
        raise ValueError('ERROR ::: Se esperaba que el parámetro df no fuera de tipo NoneType.')
    
    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: El parámetro df no es de tipo DataFrame.')

    if nombre_columna is None:
        raise ValueError('ERROR ::: Debe pasar una cadena que corresponda a un columna valida del DataFrame a procesar.')
    
    if not isinstance(nombre_columna, str):
        raise TypeError('ERROR ::: La columna pasada como parámetro debe ser de tipo string.')
    
    retornar = False
    if df[nombre_columna].dtype in ['int8', 'int16', 'int32', 'int64','int']:
        retornar = True
    else:
        if (try_cast):
            try:
                df[nombre_columna] = df[nombre_columna].astype('int64', copy=False, errors='raise')
                retornar = True
            except ValueError:
                retornar = False
    return retornar


def comprobar_existencia_columnas(df: DataFrame = None, 
                                  correctos: list = None, 
                                  log: bool = False) -> bool | list:
    '''
    Método que comprueba que un dataframe contiene las columnas correctas especificada en la variable correctos.

    Parameters
    -----------
        df : pd.DataFrame
            DataFrame a verificar.
        correctos : list
            Lista que contiene las columnas correctas que debería tener el DataFrame df.
            Por defecto su valor es None.
        log : bool
            Booleano que especifica que debe retornar una lista que contenga las columnas faltantes
            en caso que no exista alguna, caso contrario retorna el valor booleano True.
            Requiere de un recorrido exhaustivo.
    
    Returns
    -----------
        bool
            True = Si el DataFrame está correcto.
            False = Si el DataFrame está incorrecto y log tiene el valor False.
        faltantes : list
            Lista que contiene las columnas faltantes que se requieren.
        NoneType
            Hubo un problema y no se pudo procesar el DataFrame.
    '''
    if df is None:
        raise ValueError('ERROR ::: El DataFrame pasado por parámetro no tiene referencia.')
    
    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: El parámetro df no es de tipo DataFrame.')
    
    if correctos is None:
        raise ValueError('ERROR ::: Debe pasar una lista con las columnas correctas a verificar.')
    
    if not isinstance(correctos, list):
        raise TypeError('ERROR ::: El parámetro correctos debe ser de tipo list.')
    
    try:
        # Se convierte las columnas en conjuntos 
        columnas_df = set(df.columns)
        columnas_correctas = set(correctos)

        # Se verifica si todas las columnas correctas están en el DataFrame
        if columnas_correctas.issubset(columnas_df):
            return True 
        else:
            if log:
                faltantes = list(columnas_correctas - columnas_df)
            return faltantes if log else False  

    except Exception as e:
        print(f'INFO ::: No se pudo comprobar la existencia de las columnas debido a un error inesperado: {e}')
        return None


def verificar_existencia_provincia_argentina(provincia):
    return (df_localidades['Provincia'] == provincia).any()


def verificar_existencia_departamento_argentina(departamento):
    return (df_localidades['Departamento'] == departamento).any()


def verificar_existencia_ciudad_argentina(ciudad,departamento = None):
    if departamento is not None:
        condicion = (
            (df_localidades['NombreMunicipioIndec'].isin([ciudad]) | 
            df_localidades['Localidad'].isin([ciudad])) & 
            (df_localidades['Departamento'].isin([departamento]))
        ) 
    else: #Si no se paso un partido
        condicion = (
            (df_localidades['NombreMunicipioIndec'].isin([ciudad]) | 
            df_localidades['Localidad'].isin([ciudad]))
        )
        
    return condicion.any()

dir_localidades = join(DIR_PROCESAR, 'Localidades.csv')
    
try:
    df_localidades = leer_archivo(dir_localidades, type_file='csv', sep=',', strict_mode=False)
except Exception as e:
    print(f'Hubo un problema al cargar el DataFrame Localidades\n{e}')
    exit(1)













# Matias Alvarez - Data Engineer
