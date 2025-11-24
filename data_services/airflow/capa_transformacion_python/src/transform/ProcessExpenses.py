###
# Script para procesar la información sobre gastos.
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
from ETL_tools.methods.DateMethods import *
from ETL_tools.methods.GeneralTransformationMethods import limpiar_y_castear_columna
from ETL_tools.methods.MethodsOutliers import encontrar_outliers_IQR
from transform.utils.transformation_methods import cargar_dataset, aplicar_metodos_generales, \
                                    verificar_y_transformar_col_enteras, almacenar_dataframe, \
                                    procesar_y_transformar_fecha, comprobar_columnas
from transform.utils.constants import DIR_FERIADO, DIR_SIN_PROCESAR, DESCONOCIDO, \
                                      DESCONOCIDO_FECHA, DESCONOCIDO_INT, DESCONOCIDO_FLOAT


# Paso 0
def cargar_dfs() -> tuple[DataFrame]:
    '''Primer paso previo a la transformación: 
        1) Se carga el DataFrame del CSV a procesar
        2) Se carga el DataFrame que hace referencia a los feriados    
    '''
    # Se carga el dataset
    df = cargar_dataset(dir_csv)

    #Se verifica que no falten columnas
    columnas_correctas = ['IdGasto', 'IdSucursal', 'IdTipoGasto', 'Fecha', 'Monto']

    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)

    # Se crea un df para obtener los feriados
    df_feriado = cargar_dataset(DIR_FERIADO)
    return (df, df_feriado)

# Paso 1
def aplicar_metodos_generales_df(df) -> DataFrame:
    '''
    Se aplican los métodos generales para estandarizar los datos.
    '''
    rellenar_nulos = {'IdGasto': DESCONOCIDO_INT, 
                      'IdSucursal': DESCONOCIDO_INT, 
                      'IdTipoGasto': DESCONOCIDO_INT, 
                      'Fecha' : DESCONOCIDO_FECHA, 
                      'Monto': DESCONOCIDO_INT}
    
    return aplicar_metodos_generales(df=df, 
                                     rellenar_nulos=rellenar_nulos, 
                                     aplicar_transformaciones_de_cadena=False)

            
# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    # Se cambia despues el la palabra ID por IdSucursal
    CASOS = [('IdGasto', 'Error_IdGasto', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('IdSucursal', 'Error_IdSucursal', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('IdTipoGasto', 'Error_IdTipoGasto', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]

    df['Error_IdGasto'] = DESCONOCIDO
    df['Error_IdSucursal'] = DESCONOCIDO
    df['Error_IdTipoGasto'] = DESCONOCIDO 

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

# Paso 3 - Se desnormalizan las fechas         
def transformar_fechas(df, df_feriados) -> DataFrame:
    df = procesar_y_transformar_fecha(df=df, df_feriados=df_feriados, 
                                        col_fecha='Fecha',
                                        col_dia=f'Fecha_Dia', col_mes=f'Fecha_Mes', col_año=f'Fecha_Año',
                                        sufijo=None)

    # Se eliminan la columna Datetime
    df.drop(columns='Fecha_DT', axis=1, inplace=True)

    return df

# Paso 4 - Se procesa Monto solucionando sus errores y outliers
def procesar_monto(df) -> DataFrame:
    #Se verifica y corrige Precio (Monto)
    df['Error_Monto'] = DESCONOCIDO
    if df['Monto'].dtype not in ['float64', 'float32']:
                df = limpiar_y_castear_columna(df=df, 
                                                columna='Monto', 
                                                columna_error='Error_Monto',
                                                filtrar='[^0-9.]',
                                                new_type='float',
                                                rellenar_nulos=DESCONOCIDO_FLOAT
                    )

    # Se solucionan los problemas de precios atípicos o outliers
    precios_desnormalizados = encontrar_outliers_IQR(df,'Monto')#, df[df.Monto==-1]

    # Se crea la columna Outlier para marcar que el registro tiene valores atipicos
    df['Outlier'] = df.apply(lambda row: 1 
                                        if (row['Monto'] == -1) or 
                                            (row['IdGasto'] in precios_desnormalizados['IdGasto'].values) 
                                        else 0, axis=1)
    return df

# Paso 5 - Se comprueba las columnas finales
def comprobar_columnas_finales(df) -> DataFrame:
    df['Monto_Aproximado_Tipo']=DESCONOCIDO_INT
    df['Descripcion']=DESCONOCIDO
    columnas_correctas = ['IdGasto', 'IdSucursal', 'IdTipoGasto', 'Descripcion', 'Monto',
                            'Monto_Aproximado_Tipo','IdFecha', 'Fecha', 'Fecha_Dia', 'Fecha_Mes', 'Fecha_Año',
                            'Fecha_Periodo', 'Numero_Dia_Semana', 'Dia_Semana', 'Semana_Año',
                            'Mes', 'Trimestre', 'Feriado', 'Outlier',
                            'Error_IdGasto', 'Error_IdSucursal', 'Error_IdTipoGasto', 'Error_Monto', 'Error_Fecha']

    # Se retorna el DF con las columnas en el orden esperado
    if comprobar_columnas(df, columnas_correctas, True, True):
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')

    
def main() -> None:
    try:
        # Paso 0 - Cargar DFs necesarios
        df, df_feriado = cargar_dfs()

        # Paso 1 - Aplicar métodos generales
        df = aplicar_metodos_generales_df(df=df)
        
        # Paso 2 - Se corrige y almacena errores de las columnas enteras 
        df = almacenar_errores(df)

        # Paso 3 - Se procesa, corrige y desnormaliza fecha
        df = transformar_fechas(df, df_feriado)

        # Paso 4 - Se procesa, se corrige y se traza monto y outliers
        df = procesar_monto(df)

        # Paso 5 - Se verifica la existencia de todas las columnas necesarias
        df = comprobar_columnas_finales(df)
        df.rename({'Fecha_Año': 'Fecha_Anio',
                   'Dia_Semana': 'Nombre_Dia',
                   'Semana_Año': 'Semana_Anio',
                   'Mes': 'Nombre_Mes'
                   }, inplace=True, axis=1)

    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame de gasto \nERROR:{e})')
        exit(4)

    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Gasto.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia la transformación
    main()
