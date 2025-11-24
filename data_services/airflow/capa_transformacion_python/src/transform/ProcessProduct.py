###
# Script para procesar la información sobre producto
# Orden de procesamiento:
#   Después de tipo producto
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
from pandas import concat, DataFrame
from numpy import where

path.append('src')
from ETL_tools.methods.GeneralTransformationMethods import limpiar_y_castear_columna, normalizar_valores
from ETL_tools.methods.IdTransformationMethods import procesar_duplicados_identicos
from ETL_tools.methods.StandardizationDictionary import dict_normalizar_producto
from ETL_tools.methods.MethodsOutliers import encontrar_outliers_IQR 
from ETL_tools.methods.ProductMethods import desnormalizar_df_producto
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
    columnas_correctas = ['IdProducto', 'Producto', 'Precio']
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df


# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {'IdProducto' : DESCONOCIDO_INT, 
                        'Producto' :  DESCONOCIDO, 
                        'Precio' : DESCONOCIDO_INT,
                        'IdTipoProducto' : DESCONOCIDO_INT}
    df = aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
    
    return df


# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    CASOS = [('IdProducto', 'Error_IdProducto', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('IdTipoProducto', 'Error_IdTipoProducto', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    # df['Error_IdProducto'] = DESCONOCIDO
    df['Error_IdTipoProducto'] = DESCONOCIDO

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)


# Paso 3 - Descartar y normalizar productos
def normalizar_y_descartar_productos(df) -> DataFrame:
    # Cualquier Problema de ID
    cond = ( 
            (df['IdProducto'].le(DESCONOCIDO_INT)) | 
            df['Error_IdProducto'].ne(DESCONOCIDO) |
            df['IdProducto_Actualizado'].eq(1)
        )

    #Productos que son descartados, también se utilizará para descartar productos con problemas de outlier
    df['Producto_Descartado'] = where(cond, 1, 0)

    #Normalizo 'Producto', quitando abreviaciones
    normalizar_valores(df['Producto'], 
                       dict_normalizar_producto())
    
    return df


# Paso 4 - Solucionar los casos erróneos de precios y outliers
def solucionar_precio_y_outliers(df) -> DataFrame:
    df['Error_Precio'] = DESCONOCIDO

    #Si Precio no puede ser float se resuelve sus inconsistencias.
    if df['Precio'].dtype not in ['float32', 'float64']:
        df = limpiar_y_castear_columna(df=df, 
                                        columna='Precio', 
                                        columna_error='Error_Precio',
                                        filtrar='[^0-9.]',
                                        new_type='float',
                                        rellenar_nulos=DESCONOCIDO_INT
        )
        
    #Encuentro outliers y precios que corresponden a cero
    precio_outliers = encontrar_outliers_IQR(df=df, 
                                             col='Precio', 
                                             filtrar=True, 
                                             valor=10000)
    precio_cero = df[df['Precio'] == 0]

    #Se descartan (No hay forma de darles un precio, se debe solucionar a lo sumo en la previa del load al modelo de datos)
    descartar = concat([precio_outliers, precio_cero]) #Se unen
    if not descartar.empty:
        df.loc[descartar.index, 'Error_Precio'] = descartar['Precio']
        df.loc[descartar.index, 'Producto_Descartado'] = 1
        df.loc[descartar.index, 'Precio'] = DESCONOCIDO_INT

    return df


# Paso 5 - Desnormalizar productos en marca, modelo y componente
def desnormalizar_columnas_productos(df) -> DataFrame:
    #Se inicia el proceso de desnormalizacion del producto
    df['Marca'] = DESCONOCIDO
    df['Modelo'] = DESCONOCIDO
    df['Componente'] = DESCONOCIDO
    desnormalizar_df_producto(df=df, not_found=DESCONOCIDO)

    return df


# Paso 6 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdProducto_Descartado']= 0
    df['IdProducto_Correcto']= df['IdProducto']

    # Porque no se hace el merge
    df['TipoProducto_Descartado']=0
    df['TipoProducto']=DESCONOCIDO
    df['Error_TipoProducto']=DESCONOCIDO

    return df


# Paso 7 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = \
        ['IdProducto', 'Producto', 'TipoProducto', 'Marca', 'Modelo', 'Componente', 'Precio', 'IdTipoProducto', 'Producto_Descartado', 'IdProducto_Actualizado', 
            'IdProducto_Descartado', 'IdProducto_Correcto',
        'Error_IdProducto', 'Error_Precio', 'Error_IdTipoProducto', 'TipoProducto_Descartado', 'Error_TipoProducto']
    
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
                                    columna_id='IdProducto')
        
        # Paso 3 - Descartar y normalizar productos
        df = normalizar_y_descartar_productos(df)

        # Paso 4
        df = solucionar_precio_y_outliers(df)

        # Paso 5
        df = desnormalizar_columnas_productos(df)

        # Paso 6
        df = agregar_columnas(df)

        # Paso 7
        df = comprobar_columnas_finales(df)

    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame producto\nERROR:{e})')
        exit(4)

    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'producto.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia el proceso de transformación
    main()
