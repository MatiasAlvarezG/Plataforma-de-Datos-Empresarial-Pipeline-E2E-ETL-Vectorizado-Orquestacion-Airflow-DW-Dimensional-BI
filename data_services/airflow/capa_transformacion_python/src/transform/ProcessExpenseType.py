###
# Script para procesar la información sobre tipo de gasto.
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
    columnas_correctas = ['IdTipoGasto', 'Descripcion', 'Monto_Aproximado']
    
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df


# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  { 'IdTipoGasto' : DESCONOCIDO_INT, 'Descripcion' : DESCONOCIDO, 'Monto_Aproximado' : DESCONOCIDO_INT}
    df = aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
    
    return df

# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    # Se cambia despues el la palabra ID por IdSucursal
    CASOS = [('IdTipoGasto', 'Error_IdTipoGasto', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('Monto_Aproximado', 'Error_Monto', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    df['Error_IdTipoGasto'] = DESCONOCIDO
    df['Error_Monto'] = DESCONOCIDO

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

# Paso 3 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdTipoGasto_Descartado'] = 0
    df['IdTipoGasto_Correcto'] = df['IdTipoGasto']
    df['Error_Descripcion'] = DESCONOCIDO

    return df


# Paso 4 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['IdTipoGasto', 'Descripcion', 'Monto_Aproximado',
                          'IdTipoGasto_Actualizado', 'IdTipoGasto_Descartado','IdTipoGasto_Correcto',
                          'Error_IdTipoGasto', 'Error_Descripcion', 'Error_Monto']


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
                                            columna_id='IdTipoGasto')
        # Paso 3
        df = agregar_columnas(df)

        # Paso 4
        df = comprobar_columnas_finales(df)
    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame tipo de gasto \nERROR:{e})')
        exit(4)
    
    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'TiposDeGasto.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia el proceso de transformación
    main()
