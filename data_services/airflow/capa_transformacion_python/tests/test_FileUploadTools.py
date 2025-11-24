import sys
import unittest
from os import  getcwd, name
from os.path import join, dirname,abspath
import shutil

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.FileUploadTools import *
from transform.utils.constants import DIR_DATASET_TEST, DIR_PROJECT, DIR_TEST


class TestFileUploadTools(unittest.TestCase):
    def setUp(self):
        self.dir_project =  DIR_PROJECT
        self.dir_test = DIR_TEST
        self.dir_dataset = DIR_DATASET_TEST


    def test_obtener_separador_archivo_vacio(self):
        '''
        Verifica el comportamiento del método obtener_separador() cuando se pasa un archivo vacío como argumento.
        Se espera que el método genere una excepción de tipo ValueError.
        '''
        archivo = None
        self.assertRaises(ValueError, obtener_separador, path=archivo)


    def test_obtener_separador_coma(self):
        '''
        Verifica si el método obtener_separador() identifica correctamente el separador en un archivo CSV que utiliza coma como separador.
        Se espera que el método devuelva una coma como separador.
        '''
        nombre = 'test_separadores_coma.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo)
        self.assertEqual(',', sep) 


    def test_obtener_separador_punto_y_coma(self):
        '''
        Verifica si el método obtener_separador() identifica correctamente el separador en un archivo CSV que utiliza punto y coma como separador.
        Se espera que el método devuelva un punto y coma como separador.
        '''
        nombre = 'test_separadores_punto_y_coma.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo)
        self.assertEqual(';', sep) 


    def test_obtener_separador_coma_incorrecto(self):
        '''
        Verifica si el método obtener_separador() identifica incorrectamente una coma como separador en un archivo CSV que utiliza punto y coma como separador.
        Se espera que el método no devuelva una coma como separador.
        '''
        nombre = 'test_separadores_punto_y_coma.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo)
        self.assertNotEqual(',', sep) 


    def test_obtener_separador_definido_por_el_usuario(self):
        '''
        Verifica si el método obtener_separador() identifica correctamente un separador definido por el usuario en un archivo CSV.
        Se espera que el método devuelva el separador definido por el usuario.
        '''
        nombre = 'test_separadores_especial.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo,candidatos=['$'])
        self.assertEqual('$', sep) 


    def test_obtener_separador_definido_por_el_usuario_incorrecto(self):
        '''
        Verifica si el método obtener_separador() identifica incorrectamente un separador que no ha sido definido por el usuario en un archivo CSV.
        Se espera que el método no devuelva el separador definido por el usuario.
        '''
        nombre = 'test_separadores_especial.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo,candidatos=[',',';','|',')'],strict_mode=False)
        self.assertNotEqual('$', sep) 


    def test_obtener_separador_candidatos_vacios(self):
        '''
        Verifica si el método obtener_separador() maneja correctamente una lista de candidatos de separadores vacía.
        Se espera que el método devuelva un separador por defecto.
        '''
        nombre = 'test_separadores_punto_y_coma.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo,candidatos=[])
        self.assertEqual(';', sep) 


    def test_obtener_separador_candidatos_en_cadena(self):
        '''
        Verifica si el método obtener_separador() maneja correctamente un candidato de separador pasado como una cadena en lugar de una lista.
        Se espera que el método genere una excepción de tipo TypeError.
        '''
        nombre = 'test_separadores_punto_y_coma.csv'
        archivo = join(self.dir_dataset,nombre)
        self.assertRaises(TypeError, obtener_separador, path=archivo, candidatos=',')


    def test_obtener_separador_archivo_inexistente(self):
        '''
        Verifica el comportamiento del método obtener_separador() cuando se pasa una ruta de archivo inexistente como argumento.
        Se espera que el método genere una excepción de tipo FileNotFoundError.
        '''
        nombre = 'NO_EXISTE.csv'
        archivo = join(self.dir_dataset,nombre)
        self.assertRaises(FileNotFoundError,obtener_separador,archivo)


    def test_obtener_separador_check_single_column(self):
        '''
        Verifica si el método obtener_separador() maneja correctamente un archivo CSV con una sola columna en modo no estricto. 
        Se espera que el método devuelva None.
        '''
        nombre = 'test_separadores_columna_unica.csv'
        archivo = join(self.dir_dataset,nombre)
        sep = obtener_separador(path=archivo, strict_mode=False)
        self.assertEqual(None,sep)

    # STRICT_MODE fue desactivado hasta nueva actualización!
    # strict_mode=True siempre es transformado en False
    # def test_obtener_separador_not_check_single_column(self):
    #     '''
    #     Verifica si el método obtener_separador() lanza una excepción cuando se le pasa un archivo CSV con una sola columna en modo estricto.
    #     Se espera que el método genere una excepción de tipo TypeError.
    #     '''
    #     nombre = 'test_separadores_columna_unica.csv'
    #     archivo = join(self.dir_dataset, nombre)
    #     self.assertRaises(TypeError, obtener_separador, path=archivo, strict_mode=True)


    #Copia los archivos originales para hacer las pruebas de encoding, recordar que solucionar el encoding pisa el archivo
    #Por ende se necesita utilizar los archivos reales para evitar problemas de inconsistencia en las respuestas
    def cargar_archivos_originales(self, original_folder : str = 'ORIGINAL', name : str = None):
        if original_folder is None:
            raise ValueError("ERROR ::: No se paso la carpeta del archivo a copiar")
        
        if name is None:
            raise ValueError("ERROR ::: No se paso el nombre del archivo")
        
        source_file_path = join(self.dir_dataset, original_folder, name)

        try:
            #Nota: .copyfile() me generaba problemas de permisos
            shutil.copy(source_file_path, self.dir_dataset)
        except FileNotFoundError as e:
            print(f'ERROR ::: Archivo no encontrado en {source_file_path}')
        except PermissionError:
            print(f"ERROR ::: Permiso denegado al copiar el archivo desde {source_file_path}.")
        except Exception as e:
            print(f"ERR0R ::: Error inesperado al copiar el archivo: {e}")

    def test_detectar_encoding_archivo_inexistente(self):
        '''
        Prueba si detectar_encoding() devuelve una excepción FileNotFoundError cuando se pasa una ruta de archivo que no existe.
        '''
        nombre = 'NO_EXISTE.csv'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(FileNotFoundError, detectar_encoding, path=archivo)


    def test_detectar_encoding_utf8(self):
        '''
        Prueba si detectar_encoding() devuelve correctamente 'utf-8' cuando se pasa un archivo codificado en UTF-8.
        '''
        nombre = 'test_encoding_utf8.csv'
        original_folder = 'ORIGINAL'
        self.cargar_archivos_originales(original_folder, nombre)

        archivo = join(self.dir_dataset, nombre)
        encoding = str(detectar_encoding(archivo)).lower()
        self.assertEqual('utf-8', encoding)

    def test_detectar_encoding_utf16(self):
        '''
        Prueba si detectar_encoding() devuelve correctamente 'utf-16' cuando se pasa un archivo codificado en UTF-16.
        '''
        nombre = 'test_encoding_utf16.csv'
        original_folder = 'ORIGINAL'
        self.cargar_archivos_originales(original_folder, nombre)

        archivo = join(self.dir_dataset, nombre)
        encoding = str(detectar_encoding(archivo)).lower()
        self.assertEqual('utf-16', encoding)


    def test_toutf8_ruta_none(self ):
        '''
        Prueba si to_utf8() devuelve una excepción ValueError cuando se pasa una ruta de archivo vacía.
        '''
        archivo = None
        self.assertRaises(ValueError,to_utf8,path=archivo)


    def test_toutf8_archivo_inexistente(self):
        '''
        Prueba si to_utf8() devuelve una excepción FileNotFoundError cuando se pasa una ruta de archivo que no existe.
        '''
        nombre = 'NO_EXISTE.csv'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(FileNotFoundError,to_utf8,path=archivo,encoding='utf-16-le')       


    def test_toutf8_encoding_none(self):
        '''
        Prueba si to_utf8() devuelve una excepción ValueError cuando se pasa un valor None como encoding.
        '''
        nombre = 'test_encoding_utf16.csv'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(ValueError, to_utf8, path=archivo, encoding=None)        


    def test_toutf8_ruta_not_isinstance_str(self): 
        '''
        Prueba si to_utf8() devuelve una excepción TypeError cuando se pasa una ruta que no es una cadena.
        '''
        self.assertRaises(TypeError,to_utf8,path=213,encoding='UTF-16')


    def test_toutf8_encoding_not_isinstance_str(self): 
        '''
        Prueba si to_utf8() devuelve una excepción TypeError cuando se pasa un valor de encoding que no es una cadena.
        '''
        nombre = 'test_encoding_utf16.csv'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(TypeError,to_utf8,path=archivo,encoding=1234)        


    def test_toutf8_transformacion_exitosa(self): 
        '''
        Prueba si to_utf8() transforma exitosamente un archivo del encoding UTF-16 a UTF-8.
        '''
        nombre = 'test_encoding_utf16.csv'
        original_folder = 'ORIGINAL'
        self.cargar_archivos_originales(original_folder, nombre)

        archivo = join(self.dir_dataset, nombre)
        to_utf8(archivo,'UTF-16')
        encoding = str(detectar_encoding(archivo)).lower()
        self.assertEqual('utf-8',encoding)


    def test_toutf8_encoding_equivocado_usuario(self): 
        '''
        Prueba si to_utf8() devuelve una excepción UnicodeDecodeError cuando se pasa un encoding incorrecto.
        '''
        nombre = 'test_encoding_utf8.csv'
        original_folder = 'ORIGINAL'
        self.cargar_archivos_originales(original_folder, nombre)

        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(UnicodeDecodeError, to_utf8,path=archivo, encoding='utf-16-le')


    def test_leer_archivo_archivo_no_encontrado(self):
        '''
        Prueba si leer_archivo() devuelve una excepción FileNotFoundError cuando se pasa la ruta de un archivo que no existe.
        '''
        archivo = 'NO_EXISTE.csv'
        self.assertRaises(FileNotFoundError, leer_archivo, path=archivo)


    def test_leer_archivo_ruta_vacia(self):
        '''
        Prueba si leer_archivo() devuelve una excepción ValueError cuando se pasa una ruta vacía.
        '''
        archivo = None
        self.assertRaises(ValueError,leer_archivo,path=archivo)


    def test_leer_archivo_ruta_not_instance_str(self):
        '''
        Prueba si leer_archivo() devuelve una excepción TypeError cuando se pasa una ruta que no es una cadena.
        '''
        archivo = ['test_encoding_utf8.csv']
        self.assertRaises(TypeError,leer_archivo,path=archivo)


    def test_leer_archivo_separador_salto_linea(self):
        '''
        Prueba si leer_archivo() devuelve una excepción ValueError cuando se pasa un separador de salto de línea.
        '''
        separador = '\n'
        nombre = 'test_encoding_utf8.csv'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(ValueError, leer_archivo, path=archivo, sep=separador)


    def test_leer_archivo_csv_exitoso(self):
        '''
        Prueba si leer_archivo() lee exitosamente un archivo CSV y devuelve un DataFrame.
        '''
        nombre = 'test_encoding_utf8.csv'
        archivo = join(self.dir_dataset, nombre)
        separador = None
        df_obtenido = leer_archivo(path=archivo, sep=separador)
        self.assertIsInstance(df_obtenido, DataFrame)


    def test_leer_archivo_csv_exitoso_usecols_vacio(self):
        '''
        Prueba si leer_archivo() lee exitosamente un archivo CSV con la opción usecols como una lista vacía y devuelve un DataFrame.
        '''
        nombre = 'test_encoding_utf8.csv'
        archivo = join(self.dir_dataset, nombre)
        usecols = []
        separador = None
        df_obtenido = leer_archivo(path=archivo, sep=separador,usecols=usecols)
        self.assertIsInstance(df_obtenido, DataFrame)


    def test_leer_archivo_xlsx_exitoso(self):
        '''
        Prueba si leer_archivo() lee exitosamente un archivo Excel (xlsx) y devuelve un DataFrame.
        '''
        nombre = 'test_leer_archivo_excel_simple.xlsx'
        archivo = join(self.dir_dataset, nombre)
        tipo_archivo = 'xlsx'
        df_obtenido = leer_archivo(path=archivo, type_file=tipo_archivo)
        self.assertIsInstance(df_obtenido, DataFrame)


    def test_leer_archivo_csv_errado(self):
        '''
        Prueba si leer_archivo() devuelve una excepción TypeError cuando se intenta leer un archivo CSV pero no se pasa 'type_file' como 'csv'.
        '''
        nombre = 'test_leer_archivo_excel_simple.xlsx'
        archivo = join(self.dir_dataset, nombre)
        self.assertRaises(TypeError, leer_archivo, path=archivo, type_file='csv')


    def test_leer_archivo_solucionando_encoding_utf16_toutf8(self):
        '''
        Prueba si leer_archivo() puede leer correctamente un archivo CSV codificado en UTF-16 y convertirlo a UTF-8.
        '''
        nombre = 'test_encoding_utf16.csv'
        original_folder = 'ORIGINAL'
        self.cargar_archivos_originales(original_folder, nombre)

        archivo = join(self.dir_dataset, nombre)
        tipo_archivo = 'csv'
        df_obtenido = leer_archivo(path=archivo,sep=',',type_file=tipo_archivo)
        self.assertIsInstance(df_obtenido, DataFrame)

if __name__ == '__main__':
    unittest.main()
