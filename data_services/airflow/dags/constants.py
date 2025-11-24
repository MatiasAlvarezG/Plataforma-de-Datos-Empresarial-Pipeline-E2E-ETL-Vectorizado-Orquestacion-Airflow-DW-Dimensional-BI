from os import  getcwd, name
from os.path import join, abspath, dirname

# Nombre carpetas y archivos
NAME_FERIADOS = 'Feriados.csv'
DIR_NAME_DATA = 'data'
DIR_NAME_DATA_TEST = 'Test_Dataset'
DIR_DATA_NO_PROCESADO = 'SinProcesar'
DIR_DATA_PROCESAR = 'Procesados'
DIR_DATA_SEMI_PROCESAR = 'SemiProcesados'
DIR_DATA_TEST='tests'
DESCONOCIDO = 'Desconocido'
DESCONOCIDO_INT = -1
DESCONOCIDO_FECHA = '9999-12-31'
DESCONOCIDO_FLOAT = -1.0


# Directorios
# Tener en cuenta que DIR_PROJECT esta calculado para la configuración de carpetas en el contendor y no para ser ejecutado en el host.
DIR_PROJECT : str = join(abspath(dirname(__file__)), '../../..') 
DIR_DATASET : str = join(DIR_PROJECT, DIR_NAME_DATA)
DIR_TEST : str = join(DIR_PROJECT, DIR_DATA_TEST)
DIR_DATASET_TEST : str = join(DIR_TEST, DIR_NAME_DATA_TEST)
DIR_SIN_PROCESAR : str = join(DIR_DATASET, DIR_DATA_NO_PROCESADO)
DIR_SEMI_PROCESADO :str = join(DIR_DATASET, DIR_DATA_SEMI_PROCESAR)
DIR_PROCESAR : str = join(DIR_DATASET, DIR_DATA_PROCESAR)
DIR_FERIADO : str = join(DIR_PROCESAR, NAME_FERIADOS)

# Nombre tablas temporales
TMP_TIPO_PRODUCTO = 'tmp_tipo_producto'
TMP_PRODUCTO = 'tmp_producto'
TMP_CANAL_VENTA = 'tmp_canal_venta'
TMP_TIPO_GASTO = 'tmp_tipo_gasto'
TMP_PROVEEDOR = 'tmp_proveedor'
TMP_SUCURSAL = 'tmp_sucursal'
TMP_CLIENTE = 'tmp_cliente'
TMP_EMPLEADO = 'tmp_empleado'
TMP_GASTO = 'tmp_gasto'
TMP_COMPRA = 'tmp_compra'
TMP_VENTA = 'tmp_venta'


TABLES_TMP = [
    TMP_TIPO_PRODUCTO,
    TMP_PRODUCTO,
    TMP_CANAL_VENTA,
    TMP_TIPO_GASTO,
    TMP_PROVEEDOR,
    TMP_SUCURSAL,
    TMP_CLIENTE,
    TMP_EMPLEADO,
    TMP_GASTO,
    TMP_COMPRA,
    TMP_VENTA
]
