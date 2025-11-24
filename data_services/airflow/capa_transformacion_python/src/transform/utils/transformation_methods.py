from pandas import DataFrame, Series, to_datetime
from os.path import join, dirname, splitext, abspath
from sys import path
path.append('src')

from ETL_tools.methods.FileUploadTools import *
from ETL_tools.methods.VerificationMethods import verificar_columna_int, comprobar_existencia_columnas
from ETL_tools.methods.GeneralTransformationMethods import metodos_generales, limpiar_y_castear_columna
from ETL_tools.methods.DateMethods import *
from transform.utils.constants import DESCONOCIDO, DESCONOCIDO_FECHA, DESCONOCIDO_INT, DIR_SEMI_PROCESADO


def cargar_dataset(dir_csv:str, extension:str = 'csv', dtype='str') -> DataFrame:
    '''
    Método para obtener el dataframe basado en una ruta y extensión.
    '''
    if not isinstance(dir_csv, str):
        raise TypeError('ERROR ::: Se esperaba una cadena como ruta para la variable dir_csv.')
    
    if dir_csv == '' or extension == '':
        raise ValueError(f'la variable dir_csv ó extension están vacía.')
    
    try:
        return leer_archivo(path=dir_csv, type_file=extension, dtype=dtype)
    except FileNotFoundError as e:
        raise FileNotFoundError(f'ERROR ::: No existe el archivo en la ubicación: {dir_csv}')    


def comprobar_columnas(df:DataFrame, 
                       columnas_correctas:list, 
                       obtener_log:bool=True, 
                       strict_mode:bool=True) -> bool | list:
    comprobacion = comprobar_existencia_columnas(df=df, 
                                                correctos=columnas_correctas, 
                                                log=obtener_log)

    if strict_mode and isinstance(comprobacion, list):
        raise KeyError(f'ERROR ::: Faltan la/s columna/s {comprobacion}')

    if comprobacion is None:
        raise KeyError(f'ERROR ::: No se pudo procesar y verificar las columnas del DataFrame')
    
    return comprobacion


def aplicar_metodos_generales(df:DataFrame, 
                              rellenar_nulos:int|str|float|list, 
                              aplicar_transformaciones_de_cadena:bool=False) -> DataFrame:
    return metodos_generales(df=df, 
                             rellenar_nulos=rellenar_nulos,
                             transformaciones=aplicar_transformaciones_de_cadena)


def verificar_y_transformar_col_enteras(df:DataFrame, CASOS:list[tuple]) -> DataFrame:
    '''
    Método para verificar, transformar y almacenar errores de columnas entera.
    El parámetro CASOS es una lista que contiene una o más tuplas con las siguiente componentes:
        Componente 0: Id{PRINCIPAL}.
        Componente 1: Error_{IdPrincipal}.
        Componente 2: Expresión regex.
        Componente 3: Tipo de dato a castear la columna.
        Componente 4: valor para rellenar los casos Nulos/Desconocidos.
    '''
    for caso in CASOS:
        if not verificar_columna_int(df=df,
                                    nombre_columna=caso[0],
                                    try_cast=True):
            df = limpiar_y_castear_columna(df=df,
                                          columna=caso[0],
                                          columna_error=caso[1],
                                          filtrar=caso[2], 
                                          new_type=caso[3],
                                          rellenar_nulos=caso[4]
                )
    return df


def almacenar_dataframe(df : DataFrame, NAME : str, header=True,index=False, sep=',') -> bool:
    '''Método para almacenar un dataframe con extensión .csv'''
    if not isinstance(df, DataFrame):
        raise TypeError(f'Se esperaba un DataFrame. Se entregó un {type(df)}')
    
    if not isinstance(NAME, str):
        raise TypeError(f'Se esperaba una cadena para la variable NAME')
    
    if NAME == '':
        raise ValueError(f'la variable NAME esta vacía.')
    
    FINAL = f'{splitext(NAME)[0]}.csv'
    ruta_destino = join(DIR_SEMI_PROCESADO, FINAL)
    try:
        df.to_csv(ruta_destino, header=header, index=index, sep=sep)
        print(f'Se procesó {NAME} y se almacenó en {ruta_destino}')
        return True
    except Exception as e:
        return False


def procesar_y_transformar_fecha(df:DataFrame,
                                 df_feriados:DataFrame,
                                 col_fecha='Fecha',
                                 formato_in='AMD',
                                 formato_out='AMD',
                                 col_dia='Fecha_Dia',
                                 col_mes='Fecha_Mes',
                                 col_año='Fecha_Año',
                                 sufijo=None) -> DataFrame:
    
    df[col_fecha] = df[col_fecha].str.replace('/', '-', regex=False)
    if str.upper(formato_in) != 'AMD':
        # Se asume que si no es formato AMD es formato MDA ya que upstream puede entregar estos 2 casos.
        #Se utiliza 'coerce' para que ponga NaT como respuesta a los casos donde existe -1 ya que si se usa 'ignore' simplemente no podrá castear a datetime
        df[col_fecha] = to_datetime(df[col_fecha], format='%m-%d-%Y', errors='coerce')#format='%Y-%m-%d', errors='coerce')

        # Reemplazar los valores NaT por '9999-12-31'        
        df[col_fecha].fillna(to_datetime(DESCONOCIDO_FECHA, format='%m-%d-%Y', errors='coerce'), inplace=True)
        df[col_fecha] = df[col_fecha].astype(dtype=str) 
        formato_in = 'AMD'


    # Se crean las columnas necesarias 
    col_error = f'Error_Fecha_{sufijo}' if sufijo else 'Error_Fecha'
    col_dt = f'Fecha_{sufijo}_DT' if sufijo else 'Fecha_DT'
    col_id_fecha = f'IdFecha_{sufijo}' if sufijo else 'IdFecha'
    col_periodo = f'Fecha_Periodo_{sufijo}' if sufijo else 'Fecha_Periodo'
    col_num_dia = f'Numero_Dia_Semana_{sufijo}' if sufijo else 'Numero_Dia_Semana'
    col_dia_sem = f'Dia_Semana_{sufijo}' if sufijo else 'Dia_Semana'
    col_sem_año = f'Semana_Año_{sufijo}' if sufijo else 'Semana_Año'
    col_trim = f'Trimestre_{sufijo}' if sufijo else 'Trimestre'
    col_nombre_mes = f'Mes_{sufijo}' if sufijo else 'Mes'
    col_feriado = f'Feriado_{sufijo}' if sufijo else 'Feriado'

    # Se verifica y corrige Fecha
    # Recordar que extraer_partes_fecha retorna una tupla de la forma (dia, mes, año)
    df[[col_dia, 
        col_mes,
        col_año]
      ] = df[col_fecha].apply(lambda x : extraer_partes_fecha(fecha=x,
                                                              formato=formato_in,
                                                              strict_mode=False))\
                        .apply(Series)
    
    # Se busca aquellos lugares donde no había un dia|mes|año válido
    cond = (df[[col_dia, 
                col_mes, 
                col_año]] == '-1').any(axis=1)
    df[col_fecha] = df[col_fecha].mask(cond, DESCONOCIDO_FECHA)

    mask = (
            df[col_fecha] != DESCONOCIDO_FECHA) & \
            ~(df[col_fecha].str.contains('-', regex=True)
            )
    indices_erroneos = df[mask].index

    # Se almacena los errores encontrados y se actualiza la fecha con el valor desconocido
    df[col_error]=DESCONOCIDO
    if not indices_erroneos.empty:
        df.loc[indices_erroneos, col_error] = indices_erroneos
        df.loc[indices_erroneos, 
                    col_fecha] = df.loc[indices_erroneos].apply(lambda x : transformar_fecha_por_formato_por_fila(fecha=x,
                                                                                                                formato=formato_in,
                                                                                                                sep='-',
                                                                                                                columnas=[col_año, col_mes, col_dia]), 
                                                                axis=1)

    # Se crea la columa Fecha_DT que almacena en formato DateTime  
    df[col_dt] = to_datetime(df[col_fecha], errors='coerce')

    # Se agrega la columna IdFecha
    df[col_id_fecha] = df.apply(lambda x : obtener_id_fecha_fila(fila=x,
                                                                formato=formato_in, 
                                                                columnas=[col_año,col_mes,col_dia]),
                                axis=1
                        )

    filtrar_fecha_desconocida = ~df[col_id_fecha].isin(['99991231'])

    # Se agrega la columna fecha_periodo_venta
    df[col_periodo] = df[col_año].astype(str) + (df[col_mes].apply(obtener_mes_tostring)) 

    # Se agrega la columna Numero_Dia_Semana_Venta que almacena el numero del dia 
    df[col_num_dia] = df[col_fecha].apply(lambda x : obtener_num_dia_semana_por_fecha(fecha=x, 
                                                                                      formato=formato_in,
                                                                                      ignorar_fecha=DESCONOCIDO_FECHA,
                                                                                      ERROR='Desconocido',
                                                                                      strict_mode=False))

    # Se agrega la columna Dia_Semana_Venta que corresponde al nombre del dia de venta
    df[col_dia_sem] = df[col_num_dia].apply(retornar_nombre_dia)

    # Se agrega la columna Semana del año de ventas
    df[col_sem_año] = df[col_fecha].apply(lambda x : obtener_num_semana_por_fecha(fecha=x, 
                                                                                  formato=formato_in,
                                                                                  ignorar_fecha=DESCONOCIDO_FECHA,
                                                                                  ERROR='Desconocido',
                                                                                  strict_mode=False))
    # Se agrega la columna Trimestre de ventas
    df[col_trim]  = df[col_mes].where(filtrar_fecha_desconocida,
                                      DESCONOCIDO_INT).apply(obtener_trimestre_del_año)    

    # Se agrega la columna que almacenará el nombre del mes de ventas
    df[col_nombre_mes] = df[col_mes].where(filtrar_fecha_desconocida, 
                                           DESCONOCIDO).apply(retornar_nombre_mes)

    # Se agrega la columna Feriado_Venta que marcará si la venta fue en un día feriado
    df[col_feriado] = 0 
    df.loc[df[col_dt].isin(df_feriados['Fecha_DT']), col_feriado] = 1

    return df