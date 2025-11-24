###
# Script para procesar la información sobre proveedores
# Salidas exit:
#    0 : Se proceso correctamente el DataFrame.
#    1 : El archivo a procesar no existe.
#    2 : Hubo un error relacionado al valor de un dato que no permitió terminar la solicitud.
#    3 : Hubo un error relacionado al tipo de un dato que no permitió terminar la solicitud.
#    4 : Hubo un error desconocido que no permitió terminar la solicitud.
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
from ETL_tools.methods.GeneralTransformationMethods import normalizar_valores
from ETL_tools.methods.IdTransformationMethods import procesar_duplicados_identicos
from ETL_tools.methods.StandardizationDictionary import dict_tipo_sociedades, dict_normalizar_provincias,dict_normalizar_direcciones

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
    columnas_correctas = ['IDProveedor', 'Nombre', 'Address', 'City', 'State', 'Country', 'departamen']
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df

# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {'IDProveedor' : DESCONOCIDO_INT, 
                        'Nombre' : DESCONOCIDO, 
                        'Address' : DESCONOCIDO, 
                        'City' : DESCONOCIDO, 
                        'State' : DESCONOCIDO, 
                        'Country' : DESCONOCIDO, 

                        'departamen' : DESCONOCIDO}
    return aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
  

# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    CASOS = [('IDProveedor', 'Error_IDProveedor', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    df['Error_IDProveedor'] = DESCONOCIDO

    return verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

# Paso 3 - Se normaliza las columnas geográficas
def normalizar_geografia(df):
    # Se normalizan las Direcciones
    normalizar_valores(df['Address'],
                        dict_normalizar_direcciones())

    # Se marcan los errores relacionados a ciudad
    cond = df['City'].apply(verificar_existencia_ciudad_argentina)
    df['City'] = df['City'].mask(~(cond), DESCONOCIDO)

    # Se normaliza las provincias
    normalizar_valores(df['State'],
                        dict_normalizar_provincias())

    cond = df['State'].apply(verificar_existencia_provincia_argentina)
    df['State'] = df['State'].mask(~(cond), DESCONOCIDO)


    # Verifico antes que país porque utilizo la falta de departamento(partido), ciudad y provincia para decidir que no es Argentina
    cond = df['departamen'].apply(verificar_existencia_departamento_argentina)
    df['departamen'] = df['departamen'].mask(~(cond), DESCONOCIDO)

    # Pais
    cond = (df[['City', 'State','departamen']] == DESCONOCIDO).all(axis=1)
    df['Country'] = df['Country'].mask(cond, DESCONOCIDO)

    return df


# Paso 4 - Se desnormalizan las sociedades
def desnormalizar_sociedades(df) -> DataFrame:
    # Se agrega la columna Tipo_Sociedad
    df['Tipo_Sociedad'] = encontrar_tipo_sociedad(df['Nombre'])

    # Se agrega columna Sociedad_Completa
    sociedades = dict_tipo_sociedades()
    df['Sociedad_Completa'] = df['Tipo_Sociedad'].apply(lambda x : sociedades.get(x, None)
                                                                                if sociedades.get(x) != None else DESCONOCIDO)
    return df


# Paso 5 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdProveedor_Descartado']=0
    df['IdProveedor_Correcto']=df['IDProveedor']

    return df


# Paso 6 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['IDProveedor', 'Nombre', 'Tipo_Sociedad', 'Sociedad_Completa', 
                            'Address', 'City', 'State', 'Country',
                            'departamen', 'IdProveedor_Actualizado',
                            'IdProveedor_Descartado', 'IdProveedor_Correcto', 'Error_IdProveedor']

    if comprobar_columnas(df, columnas_correctas, True, True):
        # Se aplica el orden establecido
        return df[columnas_correctas]
        
    raise SystemError('ERROR ::: Hubo un error y no se pudo comprobar las columnas finales!')


def main():
    try:
        # Paso 0 - Se carga el DataFrame
        df = cargar_dfs()
        
        # Paso 1 - Aplicar métodos generales
        df = aplicar_metodos_generales_df(df) 

        # Paso 2 - Almacenar errores
        df = almacenar_errores(df)
        df = procesar_duplicados_identicos(df=df, 
                                           columna_id='IDProveedor')
        # Se renombran algunas columnas
        df.rename({'IDProveedor_Actualizado' : 'IdProveedor_Actualizado',
                   'Error_IDProveedor' : 'Error_IdProveedor'}, 
                   axis=1, inplace=True)
        
        # Paso 3 - Se normaliza las columnas Pais, Ciudad, Provincia, etc
        df = normalizar_geografia(df)

        # Paso 4 - Se agrega las columnas referentes a las sociedades
        df = desnormalizar_sociedades(df)

        # Paso 5
        df = agregar_columnas(df)

        # Paso 6
        df = comprobar_columnas_finales(df)

        # Paso 7 - Se renombran las columnas
        df.rename({
            'IDProveedor' : 'IdProveedor',
            'Name': 'Nombre', 
            'Address': 'Direccion', 
            'City': 'Ciudad', 
            'State': 'Provincia', 
            'Country': 'Pais',
            'departamen' : 'Partido'  #'departamen' : 'Departamento'  
        },inplace=True, axis=1)

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

    # print(df.head())
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Proveedores.csv'
    dir_csv     : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia el proceso de transformación
    main()
