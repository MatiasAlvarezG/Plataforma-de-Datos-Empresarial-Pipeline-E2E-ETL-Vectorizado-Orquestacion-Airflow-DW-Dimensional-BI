###
# Script para procesar la información sobre ventas
# Orden de procesamiento:
#   Después de canal de ventas, clientes, sucursales, empleados, producto
# Salidas exit:
#    0 : Se proceso correctamente el DataFrame.
#    1 : El archivo a procesar no existe.
#    2 : Hubo un error relacionado al valor de un dato que no permitió terminar la solicitud.
#    3 : Hubo un error relacionado al tipo de un dato que no permitió terminar la solicitud.
#    4 : Hubo un error desconocido que no permitió terminar la solicitud.
#    5 : Hubo un error al intentar crear una conexión con la base de datos.
#    6 : Hubo un error al intentar interactuar con la base de datos.
#    7 : Hubo un eror al intentar almacenar el archivos CSV.
###

from sys import path
from os.path import join
from pandas import to_numeric, DataFrame


path.append('src')
from ETL_tools.methods.MethodsOutliers import encontrar_outliers_IQR, precio_correspondiente
from transform.utils.transformation_methods import cargar_dataset, aplicar_metodos_generales, \
                                    verificar_y_transformar_col_enteras, almacenar_dataframe, \
                                    procesar_y_transformar_fecha, comprobar_columnas
from transform.utils.constants import DIR_FERIADO, DIR_SIN_PROCESAR, DESCONOCIDO, DESCONOCIDO_FECHA, DESCONOCIDO_INT


# Paso 0
def cargar_dfs() -> tuple[DataFrame]:
    '''Primer paso previo a la transformación: 
        1) Se carga el DataFrame del CSV a procesar
        2) Se carga el DataFrame que hace referencia a los feriados    
    '''
    # Se carga el dataset
    df = cargar_dataset(dir_csv)

    #Se verifica que no falten columnas
    columnas_correctas = ['IdVenta', 'Fecha', 'Fecha_Entrega', 'IdCanal', 
                          'IdCliente', 'IdSucursal', 'IdEmpleado', 
                          'IdProducto', 'Precio', 'Cantidad']
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
    rellenar_nulos = {'IdVenta': DESCONOCIDO_INT, 'Fecha': DESCONOCIDO_FECHA, 'Fecha_Entrega': DESCONOCIDO_FECHA, 
                      'IdCanal': DESCONOCIDO_INT, 'IdCliente': DESCONOCIDO_INT, 'IdSucursal': DESCONOCIDO_INT, 
                      'IdEmpleado': DESCONOCIDO_INT, 'IdProducto': DESCONOCIDO_INT, 'Precio': DESCONOCIDO_INT, 
                      'Cantidad': 1}
    
    return aplicar_metodos_generales(df=df, 
                                     rellenar_nulos=rellenar_nulos, 
                                     aplicar_transformaciones_de_cadena=False)


# Paso 2
def transformar_fechas(df, df_feriados) -> DataFrame:
    '''
    Se corrigen, normaliza el formatos además de desnormalizar la fecha en campos necesarios para análisis, 
     también se traza los errores.'''

    for sufijo in ['Venta', 'Entrega']:
        col_fecha = 'Fecha' if sufijo=='Venta' else 'Fecha_Entrega'
        df = procesar_y_transformar_fecha(df=df, df_feriados=df_feriados, 
                                          col_fecha=col_fecha,
                                          col_dia=f'Fecha_Dia_{sufijo}', col_mes=f'Fecha_Mes_{sufijo}', col_año=f'Fecha_Año_{sufijo}',
                                          sufijo=sufijo)

    # Se agrega la columnas Dias_Entregado que hace referencia a la cantidad de días que tardó en entregarse la venta
    df['Dias_Entregado'] = ( df['Fecha_Entrega_DT'] - df['Fecha_Venta_DT'] ).dt.days.astype(int)

    # Se convierte a entero Dias_Entregado
    df['Dias_Entregado'] = df['Dias_Entregado'].fillna(DESCONOCIDO_INT).astype(int)

    # Se eliminan la columnas Datetime
    df.drop(columns=['Fecha_Venta_DT', 'Fecha_Entrega_DT'], axis=1, inplace=True)

    return df


# Paso 3
def procesar_precio_y_cantidades(df) -> DataFrame:
    '''
    Se corrigen las columnas precio y cantidad, se almacenan sus errores y se detectan outliers.
    '''
    # Se corrige las cantidades menor o iguales a cero
    df['Cantidad'] = df['Cantidad'].mask(df['Cantidad'] <= 0, 1)

    # Se verifica y corrige la columna Precio
    df['Precio'] = to_numeric(df['Precio'], errors='coerce').fillna(DESCONOCIDO_INT)

    #Precio debe ser float -si es que no lo es todavía-
    df['Precio'] = df['Precio'].astype('float64',False)

    # Se consiguen las filas con outliers basado en el método de detección por IQR
    precios_desnormalizados = encontrar_outliers_IQR(df,
                                                     'Precio',
                                                     filtrar=True,
                                                     valor=10000)
    
    # Se crea las columnas Outlier que indica si en esa fila existio/existe un outlier
    df['Outlier'] = df.apply(lambda row: 1 if (row['Precio'] == DESCONOCIDO_INT) or 
                                              (row['IdVenta'] in precios_desnormalizados['IdVenta'].values) 
                                    else 0, axis=1)

    df['Error_Precio'] = df.apply(lambda row : row['Precio'] if (row['Outlier'] == 1 ) else DESCONOCIDO,axis=1)
    
    # Se solucionan los problemas de outliers
    mask = df['Outlier'] == 1
    df.loc[mask, 'Precio'] = df.loc[mask].apply(
        lambda x: precio_correspondiente(df, 'IdVenta','Outlier','Fecha_Año_Venta',
                                            'Fecha_Año_Entrega','Precio',
                                            x['IdProducto'], x['Fecha_Año_Venta']), 
        axis=1
    )

    # Se agrega la columna Compra_Total
    df['Compra_Total'] = df.Precio * df.Cantidad

    # Se renombra la columna Fecha que corresponde a la fecha de ventas
    df.rename({'Fecha' : 'Fecha_Venta'},inplace=True, axis=1)

    return df

# Paso 4
def comprobar_columnas_finales(df) -> DataFrame:
    df['Tipo_Canal_Venta']=DESCONOCIDO
    columnas_correctas = ['IdVenta', 'Fecha_Venta', 'Fecha_Entrega', 'Dias_Entregado', 'IdCanal', 'IdCliente',
        'IdSucursal', 'IdEmpleado', 'IdProducto', 'Precio', 'Cantidad',
        'Compra_Total','Outlier',
        'Tipo_Canal_Venta','IdFecha_Venta', 'Fecha_Dia_Venta', 'Fecha_Mes_Venta', 'Mes_Venta','Fecha_Año_Venta',
        'Fecha_Periodo_Venta','Numero_Dia_Semana_Venta', 'Dia_Semana_Venta', 'Semana_Año_Venta', 'Trimestre_Venta',
        'Feriado_Venta',  'IdFecha_Entrega', 
        'Fecha_Dia_Entrega','Fecha_Mes_Entrega','Mes_Entrega', 'Fecha_Año_Entrega','Fecha_Periodo_Entrega','Numero_Dia_Semana_Entrega',
        'Dia_Semana_Entrega', 'Semana_Año_Entrega', 'Trimestre_Entrega', 'Feriado_Entrega', 'Error_IdVenta', 
        'Error_Precio', 'Error_IdCanal', 
        'Error_IdCliente', 'Error_IdSucursal',  'Error_IdEmpleado', 'Error_IdProducto', 'Error_Cantidad','Error_Fecha_Venta','Error_Fecha_Entrega']

    if comprobar_columnas(df, columnas_correctas, True, True):
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')


def main() -> None: 
    try:
        # PASO 0 - Cargar DFs necesarios
        df, df_feriado = cargar_dfs()

        # PASO 1 (A) - Aplicar métodos generales
        df = aplicar_metodos_generales_df(df=df)
        
        # Se verifican y corrigen las columnas
        # Componentes: 0:Id{PRINCIPAL}, 1:Error_{principal}, 2:regex, 3:new_type, 4:rellenar_nulos
        CASOS = [('IdVenta', 'Error_IdVenta', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdVenta
                 ('IdCanal', 'Error_IdCanal', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdCanal
                 ('IdCliente', 'Error_IdCliente', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdCliente
                 ('IdSucursal', 'Error_IdSucursal', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdSucursal
                 ('IdEmpleado', 'Error_IdEmpleado', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdEmpleado
                 ('IdProducto', 'Error_IdProducto', '[^\\d]', 'integer', DESCONOCIDO_INT), # IdProducto
                 ('Cantidad', 'Error_Cantidad', '[^\\d]', 'integer', 1) # Cantidad
                ]
            
        # Se crean las columnas de error necesarias:
        df['Error_IdVenta'] = DESCONOCIDO
        df['Error_IdCanal'] = DESCONOCIDO
        df['Error_IdCliente'] = DESCONOCIDO
        df['Error_IdSucursal'] = DESCONOCIDO
        df['Error_IdEmpleado'] = DESCONOCIDO
        df['Error_IdProducto'] = DESCONOCIDO
        df['Error_Cantidad'] = DESCONOCIDO

        # Paso 1 (B) - Verificar y Transformar columnas enteras
        df = verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

        # Paso 2 - Verificar, Transformar, Normalizar formato y Desnormalizar en nuevas columnas de interés
        df = transformar_fechas(df, df_feriado)

        # Paso 3 - Corregir, Descartar y Trazar Precios, Cantidades y Outliers
        df = procesar_precio_y_cantidades(df)

        # Paso 4 - Garantizar la existencia de todas las columnas de interés
        df = comprobar_columnas_finales(df)

        # Nombre finales para que sean lo mismo con la base de datos
        df.rename({'Fecha_Año_Venta' : 'Fecha_Anio_Venta',
                   'Semana_Año_Venta' : 'Semana_Anio_Venta',
                   'Fecha_Año_Entrega' : 'Fecha_Anio_Entrega',
                   'Semana_Año_Entrega' : 'Semana_Anio_Entrega',
                   'Dia_Semana_Venta': 'Nombre_Dia_Venta',
                   'Dia_Semana_Entrega': 'Nombre_Dia_Entrega',
                   'Mes_Entrega': 'Nombre_Mes_Entrega',
                   'Mes_Venta': 'Nombre_Mes_Venta'
                   },
                   inplace=True, axis=1)

    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame de ventas \nERROR:{e})')
        exit(4)

    # print(df.head())
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Ventas.csv'
    NAME_FERIADOS = 'Feriados.csv'

    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia el proceso de transformación
    main()

