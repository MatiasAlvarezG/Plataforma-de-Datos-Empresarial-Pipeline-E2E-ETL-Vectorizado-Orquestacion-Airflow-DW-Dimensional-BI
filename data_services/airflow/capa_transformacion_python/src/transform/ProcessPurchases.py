###
# Script para procesar la información sobre compras.
# Orden de procesamiento:
#   Después de producto y proveedores
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
from ETL_tools.methods.SupplierMethods import *
from ETL_tools.methods.VerificationMethods import *
from ETL_tools.methods.MethodsOutliers import encontrar_outliers_IQR, precio_correspondiente 
from ETL_tools.methods.GeneralTransformationMethods import limpiar_y_castear_columna
from ETL_tools.methods.DateMethods import *

from transform.utils.transformation_methods import cargar_dataset, aplicar_metodos_generales, \
                                    verificar_y_transformar_col_enteras, almacenar_dataframe, \
                                    procesar_y_transformar_fecha, comprobar_columnas
from transform.utils.constants import DIR_FERIADO, DIR_SIN_PROCESAR, DESCONOCIDO, DESCONOCIDO_INT



# Paso 0
def cargar_dfs() -> tuple[DataFrame]:
    '''Primer paso previo a la transformación: 
        1) Se carga el DataFrame del CSV a procesar
        2) Se carga el DataFrame que hace referencia a los feriados    
    '''
    # Se carga el dataset
    df = cargar_dataset(dir_csv)

    #Se verifica que no falten columnas
    columnas_correctas = ['IdCompra', 'Fecha', 'Fecha_Año', 'Fecha_Mes', 'Fecha_Periodo',
                          'IdProducto', 'Cantidad', 'Precio', 'IdProveedor']

    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)

    # Se crea un df para obtener los feriados
    df_feriado = cargar_dataset(DIR_FERIADO)
    return (df, df_feriado)


# Paso 1
def aplicar_metodos_generales_df(df) -> DataFrame:
    '''
    Se aplican los metodos generales para estandarizar los datos.
    '''
    rellenar_nulos = {'IdCompra':DESCONOCIDO_INT, 'Fecha':'12-31-9999', 'Fecha_Año':DESCONOCIDO_INT, 'Fecha_Mes':DESCONOCIDO_INT, 
                      'Fecha_Periodo':DESCONOCIDO_INT, 'IdProducto':DESCONOCIDO_INT, 'Cantidad':1, 'Precio':-1, 'IdProveedor':DESCONOCIDO_INT}
    
    return aplicar_metodos_generales(df=df, 
                                     rellenar_nulos=rellenar_nulos, 
                                     aplicar_transformaciones_de_cadena=False)


# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    # Se cambia despues el la palabra ID por IdSucursal
    CASOS = [('IdCompra', 'Error_IdCompra', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('IdProducto', 'Error_IdProducto', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('Cantidad', 'Error_Cantidad', '[^\\d]', 'integer', 1),
             ('IdProveedor', 'Error_IdProveedor', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]

    df['Error_IdCompra'] = DESCONOCIDO
    df['Error_IdProducto'] = DESCONOCIDO
    df['Error_Cantidad'] = DESCONOCIDO
    df['Error_IdProveedor'] = DESCONOCIDO 

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)


# Paso 3 - Se desnormalizan las fechas         
def transformar_fechas(df, df_feriados) -> DataFrame:
    df = procesar_y_transformar_fecha(df=df, df_feriados=df_feriados, 
                                      formato_in='MDA',
                                      formato_out='AMD',
                                      col_fecha='Fecha',
                                      col_dia=f'Fecha_Dia', col_mes=f'Fecha_Mes', col_año=f'Fecha_Año',
                                      sufijo=None)

    # Se elimina la columna Datetime
    df.drop(columns='Fecha_DT', axis=1, inplace=True)

    return df


# Paso 4 - Se procesan los precios y cantidades, almacenando los errores y solucionándolos. Se detecta outliers
def procesar_precio_cantidad(df) -> DataFrame:
    # Se asegura de tener valores positivos o como minimo cantidad 1 (si existe un registro entonces se compró por lo menos un producto)
    cond = df['Cantidad'].le(0)
    df['Cantidad'] = df['Cantidad'].mask(cond, 1)
    # Se verifica la columna Precio
    df['Error_Precio'] = DESCONOCIDO
    if df['Precio'].dtype not in ['float32', 'float64']: 
        df = limpiar_y_castear_columna(df=df, 
                                        columna='Precio', 
                                        columna_error='Error_Precio',
                                        filtrar='[^0-9.]',
                                        new_type='float',
                                        rellenar_nulos=DESCONOCIDO_INT
            )
    # Se encuentran filas con outliers basado en el método de detección por IQR
    precios_desnormalizados = encontrar_outliers_IQR(df=df,
                                                    col='Precio',
                                                    filtrar=True, #NOTA: Estaba sin agregar. Lo puse xq no se si se agrego el parametro dsp y se le dio predefinido False
                                                    valor=10000,
                                                    quartiles=[0.25, 0.75],
                                                    rango_IQR=1.5)

    # Se crea la columna Outlier que indica si Precio es un outlier
    df['Outlier'] = df.apply(lambda row: 1 
                                        if (row['Precio'] == -1) or 
                                            (row['IdCompra'] in precios_desnormalizados['IdCompra'].values) 
                                        else 0, axis=1)
    # Se almacena el Precio considerado Outlier en la columna Error_{}
    df['Error_Precio'] = df.apply(lambda row : row['Precio'] if (row['Outlier'] == 1 ) else DESCONOCIDO,axis=1)

    #Solución problemas de outliers
    #[Nota de Diseño para el lector
    # Dada la incertidumbre respecto a la integridad de los datos en el dataset y la posible inclusión de valores manipulados deliberadamente, 
    # así como la  falta de certeza sobre si el dataframe 'df_producto' se refiere exclusivamente a productos del año 2020, se propone una estrategia para abordar esta situación de manera prudente.
    # Los precios se ajustaran de acuerdo al valor promedio del año de compra teniendo en cuenta los valores de aquellos productos que no son considerados outliers o atípicos 
    # Por ejemplo, al considerar el idproducto 42754 y los valores obtenidos mediante la función pivot_table(...), 
    #  se observa la siguiente distribución: 14497.512857 (2015), 865.226000 (2016), 888.797500 (2017), 876.285714 (2018), 866.305714 (2019), 801.920833 (2020). 
    # Al calcular los valores promedio del id 42754 para cada año, excluyendo los outliers, se obtienen los siguientes resultados: 
    #    865.098333 (2015), 865.226000 (2016), 888.797500 (2017), 876.285714 (2018), 866.305714 (2019), 874.913636 (2020).
    # Con esto se logra conseguir un precio mas acorde por año contra el precio que puede haber almacenado que correspondería al precio del año 2020.
    # Dado que en el modelo de datos se aplica SCD de tipo 2 se podría resolver este problema a partir de ahora hacia los años posteriores, simplemente filtrando el precio según sus cambios de precio
    #]
    mask = df['Outlier'] == 1     
    df.loc[mask, 'Precio'] = df.loc[mask].apply(
        lambda x: precio_correspondiente(df_outlier=df, 
                                        df_col_id='IdCompra',
                                        df_col_outlier='Outlier',
                                        df_col_fecha='Fecha_Año',
                                        df_col_fecha_aux=None,
                                        df_col_precio='Precio',
                                        id=x['IdProducto'], 
                                        año=x['Fecha_Año'],
                                        unknown_values=[-1, '-1', 'Desconocida', DESCONOCIDO, None],
                                        not_found=-1), 
        axis=1
    )

    # Se crea la columna Compra Total
    df['Compra_Total'] = df['Precio'] * df['Cantidad']
    return df

# Paso 5 - Se comprueba las columnas finales
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['IdCompra','IdFecha', 'Fecha', 'Fecha_Año', 'Fecha_Mes','Fecha_Dia', 'Fecha_Periodo',
                          'Numero_Dia_Semana', 'Dia_Semana','Semana_Año', 'Mes', 'Trimestre', 'Feriado', 'Outlier',
                          'IdProducto', 'Cantidad', 'Precio', 'Compra_Total', 'IdProveedor', 'Error_IdCompra', 'Error_IdProducto',
                          'Error_Cantidad', 'Error_Precio','Error_Fecha', 'Error_IdProveedor']

    # Se retorna el DF con las columnas en el orden esperado
    if comprobar_columnas(df, columnas_correctas, True, True):
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')

    
def main() -> None:
    try:
        # Paso 0 - Cargar DFs necesarios.
        df, df_feriado = cargar_dfs()

        # Paso 1 - Aplicar métodos generales.
        df = aplicar_metodos_generales_df(df=df)
        
        # Paso 2 - Se corrige y almacena errores de las columnas enteras.
        df = almacenar_errores(df)

        # Paso 3 - Se procesa, corrige y desnormaliza fecha.
        df = transformar_fechas(df, df_feriado)

        # Paso 4 - Se procesan los precios y cantidades, almacenando los errores y solucionándolos. Se detecta outliers.
        df = procesar_precio_cantidad(df)

        # Paso 5 - Se verifica la existencia de todas las columnas necesarias.
        df = comprobar_columnas_finales(df)

        # Se renombra las columnas finales
        df.rename({'Fecha_Año': 'Fecha_Anio',
                   'Dia_Semana': 'Nombre_Dia',
                   'Semana_Año': 'Semana_Anio',
                   'Mes': 'Nombre_Mes'
                   }, inplace=True, axis=1)

    except ValueError as e:
        print(f'{e}')
        exit(2)
    except TypeError as e:
        print(f'{e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame compras\nERROR:{e})')
        exit(4)

    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Compra.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia el proceso de transformación
    main()

