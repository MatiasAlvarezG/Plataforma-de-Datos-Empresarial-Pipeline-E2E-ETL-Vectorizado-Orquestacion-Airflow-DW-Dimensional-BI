###
# Script para procesar la información sobre tipo de producto
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

from sys import path
from os.path import join
from numpy import where

path.append('src')
from ETL_tools.methods.FileUploadTools import *
from ETL_tools.methods.IdTransformationMethods import procesar_duplicados_identicos
from utils.constants import DESCONOCIDO, DESCONOCIDO_INT, DIR_SIN_PROCESAR
from utils.transformation_methods import cargar_dataset, comprobar_columnas, \
                                         aplicar_metodos_generales, verificar_y_transformar_col_enteras, almacenar_dataframe

# Paso 0 - Cargar DF
def cargar_dfs() -> tuple[DataFrame]:
    '''Primer paso previo a la transformación: 
        1) Se carga el DataFrame del CSV a procesar
    '''
    # Se carga el dataset
    df = cargar_dataset(dir_csv)

    #Se verifica que no falten columnas
    columnas_correctas = ['IdTipoProducto', 'TipoProducto']
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df


# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {'IdTipoProducto' : DESCONOCIDO_INT, 
                       'TipoProducto' : DESCONOCIDO}
    return aplicar_metodos_generales(df=df, 
                                     rellenar_nulos=rellenar_nulos, 
                                     aplicar_transformaciones_de_cadena=True)


# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    CASOS = [('IdTipoProducto', 'Error_IdTipoProducto', '[^\\d]', 'integer', DESCONOCIDO_INT)]
    
    df['Error_IdTipoProducto'] = DESCONOCIDO
    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)


# Paso 3 - Se agregan columnas necesarias
def agregar_columnas(df)-> DataFrame:
    cond = (df['IdTipoProducto'] == -1) | (df['TipoProducto'].isin([DESCONOCIDO,'Sin Dato',
                                                                  'Sin Datos','NN','Nn',
                                                                  'Sin Valor','Sin Valores'])
                                         ) 
    df['TipoProducto_Descartado'] = where(cond, 1, 0)
    df['Error_TipoProducto'] = DESCONOCIDO
    df['IdTipoProducto_Correcto'] = df['IdTipoProducto']
    df['IdTipoProducto_Descartado'] = 0

    return df


# Paso 4
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['IdTipoProducto', 'TipoProducto', 'IdTipoProducto_Actualizado',
                        'IdTipoProducto_Descartado', 'IdTipoProducto_Correcto', 'Error_IdTipoProducto',
                        'Error_TipoProducto', 'TipoProducto_Descartado']
    if comprobar_columnas(df, columnas_correctas, True, True):
        # Se aplica el orden establecido
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')


def main() -> None:
    try:
        # Paso 0 - Se carga el dataset
        df = cargar_dfs()

        # Paso 1 - Se aplican los métodos generales y se procesan los duplicados directos
        df = aplicar_metodos_generales_df(df)

        # Paso 2 - Se almacena los errores de la columna IdTipoProducto
        df = almacenar_errores(df)
        df = procesar_duplicados_identicos(df=df, 
                                            columna_id='IdTipoProducto')
        # Paso 3 - Se agregan las columnas necesarias 
        df = agregar_columnas(df)

        # Paso 4 - Se comprueban las columnas finales
        df = comprobar_columnas_finales(df)

    except FileNotFoundError as e:
        print(f'ERROR ::: No existe el archivo en la ubicación: {dir_csv}')
        exit(1)
    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame producto\nERROR:{e})')

    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'tipo_producto.csv'
    dir_csv : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia la transformación
    main()



