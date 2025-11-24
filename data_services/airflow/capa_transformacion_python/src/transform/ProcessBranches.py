###
# Script para procesar la información sobre sucursales.
# Salidas exit:
#    0 : Se proceso correctamente el DataFrame
#    1 : El archivo a procesar no existe
#    2 : Hubo un error relacionado al valor de un dato que no permitió terminar la solicitud
#    3 : Hubo un error relacionado al tipo de un dato que no permitió terminar la solicitud
#    4 : Hubo un error desconocido que no permitió terminar la solicitud
#    5 : Hubo un error al intentar crear una conexión con la base de datos.
#    6 : Hubo un error al intentar interactuar con la base de datos.
#    7 : Hubo un error al almacenar el CSV.
###

from os.path import join
from sys import path
path.append('src')

from ETL_tools.methods.FileUploadTools import *
from ETL_tools.methods.StandardizationDictionary import dict_normalizar_provincias, dict_normalizar_localidades, dict_normalizar_direcciones
from ETL_tools.methods.GeneralTransformationMethods import normalizar_valores
from ETL_tools.methods.IdTransformationMethods import procesar_duplicados_identicos
from utils.constants import DIR_SIN_PROCESAR, DESCONOCIDO, DESCONOCIDO_INT
from utils.transformation_methods import cargar_dataset, comprobar_columnas, aplicar_metodos_generales, \
                                         verificar_y_transformar_col_enteras, almacenar_dataframe

# Paso 0 - Cargar DF
def cargar_dfs() -> tuple[DataFrame]:
    '''Primer paso previo a la transformación: 
        1) Se carga el DataFrame del CSV a procesar
    '''
    # Se carga el dataset
    df = cargar_dataset(dir_csv)

    #Se verifica que no falten columnas
    columnas_correctas = ['ID', 'Sucursal', 'Direccion', 'Localidad', 
                          'Provincia', 'Latitud', 'Longitud']
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df


# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {'ID': DESCONOCIDO_INT, 'Sucursal' : DESCONOCIDO,  'Direccion' : DESCONOCIDO, 'Localidad' : DESCONOCIDO,
                        'Provincia' : DESCONOCIDO,  'Latitud' :  DESCONOCIDO_INT, 'Longitud' : DESCONOCIDO_INT }
    df = aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
    
    return df

                
# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    # Se cambia despues el la palabra ID por IdSucursal
    CASOS = [('ID', 'Error_ID', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    df['Error_ID'] = DESCONOCIDO
    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)


# Paso 3 - Se desnormaliza las sucursales
def desnormalizar_sucursales(df)-> DataFrame:
        # Se normalizan los valores de sucursal
        df['Sucursal'].replace('[^A-Za-z\s]',
                               '',
                               regex=True, 
                               inplace=True)
        df['Sucursal'].replace('Mdq', 'Mar Del Plata', inplace=True)

        # Se agrega la columna número sucursal la cual contiene el número de sucursal
        df['Numero_Sucursal'] =  df.groupby('Sucursal').cumcount() + 1

        # Se agrega la columna Sucursal Completa que almacenará el nombre completo de la sucursal
        df['Sucursal_Completa'] = (df['Sucursal'].str.strip() + ' ' + df['Numero_Sucursal'].astype(str))
        return df

# Paso 4 - Se normaliza las columnas geográficas
def normalizar_geografia(df):
        # Se normaliza la dirección
        normalizar_valores(df['Direccion'],
                           dict_normalizar_direcciones())

        # Se normalizan las localidades
        df['Localidad'].replace(dict_normalizar_localidades(),
                                        regex=False,
                                        inplace=True)
        # Se normalizan las provincias 
        df['Provincia'].replace(dict_normalizar_provincias(),
                                        regex=False,
                                        inplace=True)

        ### Se normalizan las columnas Longitud y latitud

        # [general] Se reemplaza comas por puntos
        df[['Longitud','Latitud']] = df[['Longitud','Latitud']].map(lambda x : str.replace(x,',','.'))

        ## Se trabaja Longitud
        df['Error_Longitud'] = DESCONOCIDO # 0

        # Se marcan como error aquellas longitudes que estan mal escritas
        cond = ~(df['Longitud'].astype(str).str.match(r'-?[0-9]{2}\.[0-9]+'))
        errores = df['Longitud'][cond]
        if not errores.empty:
            df.loc[errores.index, 'Error_Longitud'] = errores
            df.loc[errores.index, 'Longitud'] = '-1'

        ## Se trabaja Latitud
        df['Error_Latitud'] = DESCONOCIDO # 0
        # Se marcan como error aquellas latitudes que estan mal escritas
        cond = ~(df['Latitud'].astype(str).str.match(r'-?[0-9]{2}\.[0-9]+'))
        errores = df['Latitud'][cond]
        if not errores.empty:
            df.loc[errores.index, 'Error_Latitud'] = errores
            # df.loc[errores.index,'Verificar_Latitud'] = 1
            df.loc[errores.index, 'Latitud'] = '-1'

        return df


# Paso 5 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdSucursal_Descartado'] = 0
    df['IdSucursal_Correcto'] = df['ID']

    return df


# Paso 6 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['ID', 'Sucursal_Completa', 'Sucursal','Numero_Sucursal', 
                            'Direccion', 'Localidad', 'Provincia', 'Latitud',
                            'Longitud', 'IdSucursal_Actualizado', 'IdSucursal_Descartado',  
                            'IdSucursal_Correcto', 'Error_Latitud','Error_Longitud', 'Error_IdSucursal']


    if comprobar_columnas(df, columnas_correctas, True, True):
        # Se aplica el orden establecido
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')


def main() -> None: 
    try:
        # Paso 0 - Se carga el DataFrame
        df = cargar_dfs()
        
        # Paso 1 - Aplicar métodos generales
        df = aplicar_metodos_generales_df(df) 

        # Paso 2 - Almacenar errores 
        df = almacenar_errores(df)
        df = procesar_duplicados_identicos(df=df, 
                                           columna_id='ID')
        # Se renombran algunas columnas
        df.rename({'ID_Actualizado' : 'IdSucursal_Actualizado',
                   'Error_ID' : 'Error_IdSucursal'}, 
                   axis=1, inplace=True)
        
        # Paso 3 - Descartar y normalizar productos
        df = desnormalizar_sucursales(df)

        # Paso 4 - Se normaliza las columnas geográficas
        df = normalizar_geografia(df)

        # Paso 5
        df = agregar_columnas(df)

        # Paso 6
        df = comprobar_columnas_finales(df)

        # Se renombran las columnas
        df.rename({'ID' : 'IdSucursal'},
                  axis=1, 
                  inplace=True)
    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame sucursales\nERROR:{e})')
        exit(4)
        
    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Sucursales.csv'
    dir_csv : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia la transformación
    main()

