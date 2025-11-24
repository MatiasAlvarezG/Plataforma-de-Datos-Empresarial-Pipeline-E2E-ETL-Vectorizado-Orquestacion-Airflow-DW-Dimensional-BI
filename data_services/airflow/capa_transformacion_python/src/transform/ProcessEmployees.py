###
# Script para procesar la información sobre empleados.
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
from ETL_tools.methods.ClientMethods import *
from ETL_tools.methods.GeneralTransformationMethods import *
from ETL_tools.methods.VerificationMethods import verificar_columna_int, comprobar_existencia_columnas
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
    columnas_correctas = ['ID_empleado', 'Apellido', 'Nombre', 'Sucursal', 'Sector', 'Cargo', 'Salario']
    comprobar_columnas(df=df, 
                       columnas_correctas=columnas_correctas, 
                       obtener_log=True, strict_mode=True)
    return df


# Paso 1 - Aplicar métodos generales
def aplicar_metodos_generales_df(df) -> DataFrame:

    rellenar_nulos =  {'ID_empleado' : DESCONOCIDO_INT, 
                          'Apellido' : DESCONOCIDO,
                          'Nombre' : DESCONOCIDO,
                          'Sucursal' : DESCONOCIDO,
                          'Sector' : DESCONOCIDO,
                          'Cargo' : DESCONOCIDO,
                          'Salario' : DESCONOCIDO_INT}
    df = aplicar_metodos_generales(df=df, 
                                   rellenar_nulos=rellenar_nulos, 
                                   aplicar_transformaciones_de_cadena=True)
    
    return df
                
# Paso 2 - Almacenar errores
def almacenar_errores(df) -> DataFrame:
    # Se cambia despues el la palabra ID por IdSucursal
    CASOS = [('ID_empleado', 'Error_ID_empleado', '[^\\d]', 'integer', DESCONOCIDO_INT)
            ]
    
    df['Error_ID_empleado'] = DESCONOCIDO
    df['Error_Salario'] = DESCONOCIDO
    df = verificar_y_transformar_col_enteras(df=df, CASOS=CASOS)

    try:
        df['Salario'] = df['Salario'].str.replace(',','.').astype(float)
    except:
        df = limpiar_y_castear_columna(df=df, 
                                        columna='Salario', 
                                        columna_error='Error_Salario',
                                        filtrar='[^0-9.]',
                                        new_type='float',
                                        rellenar_nulos=DESCONOCIDO_INT
            )
    return df
            

# Paso 3 - Se desnormaliza las sucursales
def desnormalizar_sucursales(df)-> DataFrame:
        # Se agrega la columna Sucursal_Numero
        erroneos = df[df['Sucursal'].str.contains('[$\\d]',regex=True)]

        # Se agrega la columna Numero_Sucursal
        df['Numero_Sucursal'] = 1
        df.loc[erroneos.index, 'Numero_Sucursal'] = erroneos['Sucursal'].apply(lambda x : int(extraer_numeros(x)))

        # Se verifica y solucionan problemas en la columna Sucursal
        mask = df['Sucursal'].apply(tiene_numeros_sin_separar)

        df['Error_Sucursal'] = DESCONOCIDO
        almacenar_incosistencia(df=df, 
                                col='Sucursal',
                                mask=mask, 
                                col_error='Error_Sucursal')
        # Se normaliza Sucursal
        df['Sucursal'].replace('[^A-Za-z\s]',
                                '',
                                regex=True, 
                                inplace=True)

        # Se agrega la columna Sucursal_Completa
        df['Sucursal_Completa'] = df['Sucursal'] + ' ' + df['Numero_Sucursal'].astype(str)

        return df


# Paso 5 - Se normaliza la columna Cargo
def normalizar_cargo(df) -> DataFrame:
    # Se Normaliza los valores de Cargo
    dicc = {'Adm' : 'Administrativo', 'Aux' : 'Auxiliar', 'Tec' : 'Tecnico', 'Vend' : 'Vendedor'}

    df['Cargo'] = df['Cargo'].str.replace('[\W]',' ',regex=True)
    normalizar_valores(df['Cargo'], dicc)
    return df

# Paso 6 - Se agregan columnas necesarias para la base de datos
def agregar_columnas(df) -> DataFrame:
    df['IdEmpleado_Descartado']=0
    df['IdEmpleado_Correcto']=df['ID_empleado']

    return df

 
# Paso 7 - Se comprueban las columnas
def comprobar_columnas_finales(df) -> DataFrame:
    columnas_correctas = ['ID_empleado', 'Nombre_y_Apellido','Apellido', 'Nombre', 'Sucursal', 
                        'Numero_Sucursal','Sucursal_Completa', 'Sector', 'Cargo',
                        'Salario', 'IdEmpleado_Actualizado', 'IdEmpleado_Descartado', 'IdEmpleado_Correcto', 'Error_IdEmpleado', 
                        'Error_Sucursal', 'Error_Salario']

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

        # Paso 2 - Almacenar errores, procesar duplicados y renombrar columnas
        df = almacenar_errores(df)
        # Se eliminan los IDs duplicados directos
        df = procesar_duplicados_identicos(df=df, columna_id='ID_empleado', procesar_desconocidos=False)
        # Se renombran algunas columnas
        df.rename({'ID_empleado_Actualizado' : 'IdEmpleado_Actualizado',
                   'Error_ID_empleado' : 'Error_IdEmpleado'}, 
                    axis=1, inplace=True)
    
        # Paso 3 - Se agrega la columna Nombre_y_Apellido
        df['Nombre_y_Apellido'] = df['Nombre'] + ' ' + df['Apellido']
        
        # Paso 4 - Descartar y normalizar productos
        df = desnormalizar_sucursales(df)

        # Paso 5 - normalizar_cargo
        df = normalizar_cargo(df)

        # Paso 6 - Se agregan las columnas necesarias para la base de datos
        df = agregar_columnas(df)

        # Paso 7
        df = comprobar_columnas_finales(df)
        # Se renombran las columnas
        df.rename({'ID_empleado' : 'IdEmpleado'}, inplace=True, axis=1)
    except ValueError as e:
        print(f'ERROR ::: {e}')
        exit(2)
    except TypeError as e:
        print(f'ERROR ::: {e}')
        exit(3)
    except Exception as e:
        ERROR = type(e)
        print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame de Empleados \nERROR:{e})')
        exit(4)

    # Se almacena el CSV
    exit(0) if almacenar_dataframe(df, NAME) else exit(7)


if __name__ == '__main__':
    NAME = 'Empleado.csv'
    dir_csv  : str = join(DIR_SIN_PROCESAR, NAME)

    # Se inicia la transformación
    main()

