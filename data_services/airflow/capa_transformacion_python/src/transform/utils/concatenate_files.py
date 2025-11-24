###
# Script que concatena la información de dos o mas dataset y lo almacena en un csv separado por comas
# Modo de uso:
#   + Los archivos a procesar deben estar adentro de la carpeta /data del proyecto
#   + Los argumentos deben seguir el siguiente orden: ubicacion_archivo_1, ... ,ubicacion_archivo_N, ubicacion_de_almacenamiento
#   Por Ejemplo:
#       python src/concatenate_files.py SinProcesar/test.csv, Procesados/test1.csv, SemiProcesados/test_concat.csv
#       En este ejemplo se concatena los dataset test y test1 y el resultado es almacenado en la carpeta SemiProcesados con el nombre test_concat.csv
###

from os.path import join #, dirname, abspath, 
from sys import path, argv
path.append('src')

from ETL_tools.methods.FileUploadTools import *
from transform.utils.transformation_methods import concat, almacenar_dataframe, leer_archivo
from transform.utils.constants import DIR_DATASET


def validar_argumentos(argumentos):
    if len(argumentos) < 3: 
        raise ValueError('ERROR ::: Debe pasar por lo menos dos ubicaciones y estas deben estar en la carpeta /data, además se debe pasar la ubicación de almacenamiento'
                         'Los argumentos deben seguir el siguiente orden: ubicacion_archivo_1, ubicacion_archivo_2, ..., ubicacion_de_almacenamiento'
                         'Por ejemplo: python src/concatenate_files.py SinProcesar/test.csv, Procesados/test1.csv, SemiProcesados/test_concat.csv')


def main(argumentos) -> None:
    try:
        validar_argumentos(argumentos)
        dfs = []

        # Se crea una lista con todos los df a procesar
        for ubicacion in argumentos[:-1]: 
            df = leer_archivo(DIR_DATASET, ubicacion)
            dfs.append(df)

        # Se concatena en un único DF
        df = concat(dfs, ignore_index=True)

        # Se almacenan
        almacenar = join(DIR_DATASET, argumentos[-1])
        almacenar_dataframe(df=df, NAME=almacenar, 
                            header=True, index=False, sep=',')
        
    except ValueError as e:
        print(f'{e}')
        exit(2)
    except TypeError as e:
        print(f'{e}')
        exit(3)
    except Exception as e:
        print(f' Un error desconocido no permitio procesar la información : {e}')
        exit(4)


if __name__ == "__main__":
    main(argv[1:])
    print(f'Se unificaron todos los DF solicitados!')
    exit(0)

