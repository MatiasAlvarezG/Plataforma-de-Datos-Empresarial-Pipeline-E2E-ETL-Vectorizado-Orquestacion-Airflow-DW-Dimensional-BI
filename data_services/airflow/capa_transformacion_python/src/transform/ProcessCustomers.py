###
# Script para procesar la información sobre clientes
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
from numpy import where
from re import sub
from os.path import join

path.append('src')
from ETL_tools.methods.FileUploadTools import *
from ETL_tools.methods.ClientMethods import *
from ETL_tools.methods.GeneralTransformationMethods import normalizar_valores
from ETL_tools.methods.StandardizationDictionary import dict_normalizar_direcciones, dict_normalizar_localidades, dict_normalizar_provincias
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
    columnas_correctas = ['ID','Provincia','Nombre_y_Apellido','Domicilio','Edad','Localidad','X','Y'] 
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df

# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {
                        'ID' : DESCONOCIDO_INT, 'Provincia' : DESCONOCIDO, 'Nombre_y_Apellido' : DESCONOCIDO, 
                        'Domicilio' : DESCONOCIDO, 'Edad' : DESCONOCIDO_INT,
                        'Localidad' : DESCONOCIDO, 'X' : '-1', 'Y' : '-1' 
                        }
    
    return aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
  
# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    CASOS = [('ID', 'Error_ID', '[^\\d]', 'integer', DESCONOCIDO_INT),
             ('Edad', 'Error_Edad', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    df['Error_ID'] = DESCONOCIDO
    df['Error_Edad'] = DESCONOCIDO

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

# Paso 3 - Se normaliza las columnas geográficas
def normalizar_geografia(df):
    # Se normaliza Domicilio
    normalizar_valores(df['Domicilio'],
                    dict_normalizar_direcciones())

    df['Domicilio'].replace(dict_normalizar_direcciones(),
                                    regex=False,
                                    inplace=True)

    # Se normaliza las localidades
    df['Localidad'].replace(dict_normalizar_localidades(),
                            regex=False,
                            inplace=True)

    df['Provincia'].replace(dict_normalizar_provincias(),
                            regex=False,
                            inplace=True)
    ## [general] Se reemplaza los posibles comas por puntos
    df[['X','Y']] = df[['X','Y']].map(lambda x : str.replace(x,',','.'))

    # Se normaliza la columna longitud
    df['Error_Longitud'] =  DESCONOCIDO

    cond = ~(df['X'].astype(str).str.match(r'-?[0-9]{2}\.[0-9]+'))
    errores = df['X'][cond]
    if not errores.empty:
        df.loc[errores.index, 'Error_Longitud'] = errores
        df.loc[errores.index, 'X'] = '-1'

    # Se normaliza la columna latitud
    df['Error_Latitud'] =  DESCONOCIDO
    cond = ~(df['Y'].astype(str).str.match(r'-?[0-9]{2}\.[0-9]+'))
    errores = df['Y'][cond]
    if not errores.empty:
        df.loc[errores.index, 'Error_Latitud'] = errores
        df.loc[errores.index, 'Y'] = '-1'
    return df


# Paso 4 - Se agrega el grupo etario y si es mayor de edad
def desnormalizar_edad(df) -> DataFrame:
    df['Grupo_Etario'] = df['Edad'].apply(obtener_grupo_etario)        

    # Se agrega la columna Mayor_Edad que indica si un cliente es mayor de edad
    cond = df['Edad']>=18
    df['Mayor_Edad'] = where(cond ,1,0)        

    return df

# Paso 5 - Se desnormaliza el nombre y apellido de cada cliente
def desnormalizar_nombre_y_apellido(df) -> DataFrame:
    # Se eliminan caracteres extraños del nombre y apellido
    df['Nombre_y_Apellido'] = df['Nombre_y_Apellido'].apply(lambda x: sub(r'[^a-zA-Z\s]', '', str(x)))

    # Se crean las columnas necesarias
    df['Nombre_Completo'] = DESCONOCIDO 
    df['Apellido_Completo'] = DESCONOCIDO 
    df['Nombre_1'] = DESCONOCIDO 
    df['Nombre_2'] = DESCONOCIDO 
    df['Nombre_3'] = DESCONOCIDO 
    df['Apellido_1'] = DESCONOCIDO 
    df['Apellido_2'] = DESCONOCIDO 
    df['Apellido_3'] = DESCONOCIDO 

    # Se  inicia la desnormalización de nombre y apellido
    for idx, fila in enumerate(df.Nombre_y_Apellido, df.Nombre_y_Apellido.index[0]):
        PERSONA = fila.split(' ')
        nombre, apellido = separar_nombre_y_apellidos(PERSONA)

        df.loc[idx, 'Nombre_Completo'] = ' '.join(nombre)
        df.loc[idx, 'Apellido_Completo'] = ' '.join(apellido)

        df.loc[idx, 'Nombre_1'] = nombre[0] if nombre else df.loc[idx, 'Nombre_1'] 
        df.loc[idx, 'Nombre_2'] = nombre[1] if len(nombre) >= 2 else df.loc[idx, 'Nombre_2'] 
        df.loc[idx, 'Nombre_3'] = nombre[2] if len(nombre) >= 3 else df.loc[idx, 'Nombre_3'] 
        df.loc[idx, 'Apellido_1'] = apellido[0] if apellido else df.loc[idx, 'Apellido_1'] 
        df.loc[idx, 'Apellido_2'] = apellido[1] if len(apellido) >= 2 else df.loc[idx, 'Apellido_2'] 
        df.loc[idx, 'Apellido_3'] = apellido[2] if len(apellido) >= 3 else df.loc[idx, 'Apellido_3'] 
    return df

# Paso 6 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdCliente_Descartado']=0
    df['IdCliente_Correcto']=df['ID']

    return df

# Paso 7 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['ID', 'Provincia', 'Nombre_y_Apellido', 'Nombre_Completo', 'Apellido_Completo',
                        'Nombre_1','Nombre_2', 'Nombre_3', 'Apellido_1', 
                        'Apellido_2', 'Apellido_3', 
                        'Domicilio', 'Edad', 'Grupo_Etario', 'Mayor_Edad',
                        'Localidad', 'X', 'Y', 'IdCliente_Actualizado', 'IdCliente_Descartado',
                        'IdCliente_Correcto', 'Error_IdCliente','Error_Edad', 'Error_Longitud','Error_Latitud'
                        ]

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

        # Se eliminan los IDs duplicados directos
        df = procesar_duplicados_identicos(df=df, 
                                           columna_id='ID')
        # Se renombran algunas columnas
        df.rename({'ID_Actualizado' : 'IdCliente_Actualizado',
                    'Error_ID' : 'Error_IdCliente'}, 
                  axis=1, inplace=True)
        
        # Paso 3 - Se normaliza las columnas Pais, Ciudad, Provincia, etc
        df = normalizar_geografia(df)

        # Paso 4 - Se desnormaliza la edad en distintas columnas
        df = desnormalizar_edad(df)

        # Paso 5 - Se desnormaliza nombre y apellido
        df = desnormalizar_nombre_y_apellido(df)

        # Paso 6 - Se agrega las columnas necesarias
        df = agregar_columnas(df)
                #Se elimina col10

        # Paso 7 - Se elimina las columnas innecesarias
        try:
            del df['col10']
        except:
            pass

        # Paso 8 - Se comprueba las columnas finales
        df = comprobar_columnas_finales(df)

        # Paso 9 - Se renombran las columnas finales
        df.rename({'ID' : 'IdCliente', 'X' : 'Longitud', 'Y' : 'Latitud'}, inplace=True, axis=1)
    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame de clientes \nERROR:{e})')
        exit(4)

    # Se almacen el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Clientes.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia la transformación
    main()
