# Métodos para cálculos de outliers

from pandas import DataFrame
from math import isnan
from .FileUploadTools import *
from os.path import abspath, join, dirname
from sys import path
path.append('src')


def calcular_quartiles(df, col, quartiles_valores=[]) -> tuple:
    '''
    Calcula los cuartiles de una columna específica de un DataFrame.

    Parameters:
    -----------
    - df : DataFrame
        El DataFrame del cual se calcularán los cuartiles.
    - col : str
        El nombre de la columna del DataFrame para la cual se calcularán los cuartiles.
    - quartiles_valores : list, optional
        Una lista de valores de cuartiles entre 0 y 1 para calcular. Por defecto es una lista vacía.

    Returns:
    --------
    tuple
        Una tupla que contiene los valores calculados de los cuartiles especificados.
    
    Raises:
    -------
    ValueError
        - Si `df` o `col` son None.
    TypeError
        - Si `df` no es un DataFrame.

    Notes:
    ------
    - Si `quartiles_valores` no se proporciona, se devolverá una tupla vacía.
    - Si se proporciona un valor de cuartil fuera del rango [0, 1], se tratará como 0 si es menor que 0 y como 1 si es mayor que 1.
    - Si un cuartil no puede ser calculado debido a un valor no válido, se devuelve -1 en su lugar.
    - Se espera que los valores de cuartil sean floats entre 0 y 1, inclusive.

    Example:
    ---------
    >>> calcular_quartiles(df, 'Edad', [0.25, 0.5, 0.75])
        Retorna: (30, 40, 50), donde el primer valor es el primer cuartil, el segundo valor es la mediana, y el tercer valor es el tercer cuartil de la columna 'Edad' del DataFrame 'df'.
    '''
    if df is None or col is None: 
        raise ValueError('ERROR ::: Se esperaba que el parámetro df o col no fueran NoneType.')
   
    if not isinstance(df, DataFrame): 
        raise TypeError('ERROR ::: el parámetro df no es un DataFrame.')
   
    if len(quartiles_valores)==0:
        print('INFO ::: No se pasaron valores de cuartiles.')
        return ()

    resultado_quartiles = []
   
    for quartil in quartiles_valores:
        try:
            quartil = float(quartil) if (0 <= quartil <= 1) else (0 if quartil<0 else 1)
            quartil_calculado = df[col].quantile(quartil) 
            resultado_quartiles.append(quartil_calculado)
        except Exception as e:
            print(f'INFO ::: Se proporcionó un cuartil no válido ({quartil}), se devuelve -1')
            resultado_quartiles.append(-1)

    return tuple(resultado_quartiles)


def encontrar_outliers_IQR(df,
                           col,
                           filtrar : bool = False,
                           valor = 10000, 
                           quartiles : list = [0.25,0.75], 
                           rango_IQR = 1.5) -> DataFrame:
    '''
    Encuentra los valores atípicos (outliers) en un DataFrame utilizando el método de rango intercuartílico (IQR).

    Parameters
    ----------
    df : DataFrame
        El DataFrame en el que se buscarán los outliers.
    col : str
        El nombre de la columna en el DataFrame donde se buscarán los outliers.
    filtrar : bool
        Indica si se deben filtrar los outliers con un valor mínimo dado 
        por defecto su valor es False
    valor : float
        El valor mínimo para filtrar los outliers 
        por defecto su valor es 10000
    quartiles : list
        Una lista de dos valores que representan los percentiles para calcular los cuartiles 
        por defecto su valor es [0.25, 0.75] para Q1 y Q3).
    rango_IQR : float
        El factor multiplicativo que se utilizará para determinar los límites del rango intercuartílico 
        por defecto su valor es 1.5

    Returns
    -------
    DataFrame
        Un DataFrame que contiene los outliers encontrados en la columna especificada.

    Raises
    ------
    ValueError
        Si el DataFrame o la columna son None, o si los valores de cuartiles no son una lista de dos elementos.
    TypeError
        Si el parámetro df no es un DataFrame.
    KeyError
        Si la columna especificada no es una cadena o si no existe en el DataFrame.

    Notes
    -----
    Los outliers se determinan utilizando el rango intercuartílico (IQR), que es la diferencia entre el tercer y primer cuartil (Q3 - Q1).
    Los valores que están por debajo de (Q1 - rango_IQR * IQR) o por encima de (Q3 + rango_IQR * IQR) se consideran outliers.

    Example
    -------
    >>> encontrar_outliers_IQR(df, 'Precio', filtrar=True, valor=500, quartiles=[0.1, 0.9], rango_IQR=2.0)
    Retorna un DataFrame con los outliers encontrados en la columna 'Precio', filtrando aquellos que tienen un valor mayor o igual a 500, utilizando los percentiles 10 y 90 para calcular los cuartiles, y un rango intercuartílico multiplicado por 2.0.

    '''
    
    if df is None or col is None: 
        print('ERROR ::: No se paso un DataFrame y/o una columna')
        raise ValueError('ERROR ::: El parámetro df debe ser un DataFrame y no un NoneType.')

    if not isinstance(df, DataFrame): 
        raise TypeError('ERROR ::: el parámetro df no es un DataFrame.')

    if not isinstance(col, str) or col not in df.columns:
       raise KeyError(f'ERROR ::: La columna pasada no es de tipo cadena o no existe como columna en el DataFrame pasado.')
       
    if len(quartiles) !=2 :
        print('INFO :::Se necesitan solo 2 valores para calcular quartiles, se usara Q1(0.25) y Q3(0.75)')
        quartiles = [0.25,0.75]
        
    Q1,Q3 = calcular_quartiles(df,col,quartiles)
    IQR = Q3 - Q1 

    outliers = df[((df[col] < (Q1 - rango_IQR * IQR)) | (df[col] > (Q3 + rango_IQR * IQR)))]
    return  outliers[outliers[col] >= valor] if filtrar else outliers


# Método que calcula el precio correspondiente utilizando el DataFrame Producto
def precio_correspondiente(df_outlier,
                           df_col_id : str = 'Idproducto', #Id del df
                           df_col_outlier : str = 'Outlier', #columna que marca si es outlier
                           df_col_fecha : str = 'Fecha_Año', #columna fecha para buscar el año
                           df_col_fecha_aux : str = None,
                           df_col_precio : str = 'Precio', # columna precio del df
                           id : int = None,
                           año : int = None,
                           unknown_values = [-1, '-1', 'Desconocida', 'Desconocido', None],
                           not_found = '-1') -> int:
    '''
    Esta función devuelve el precio correspondiente de un producto identificado por su IdProducto según su año asociado. 
    Cuando el parámetro año es igual a 2020, la función utiliza la información del DataFrame Producto. 
    La lógica subyacente se basa en la exclusión de valores atípicos (outliers) para proporcionar una estimación más precisa del precio en ese período, 
    aprovechando aquellos valores que no son atípicos para ofrecer una aproximación más precisa a la tendencia general.

    df_col_fecha_aux hace referencia a otra columna de Fecha para intentar resolver el valor atípico si por alguna razón la columna Fecha no tiene un valor asociado (o es desconocido: -1)

    Parameters:
    -----------
    - df_outlier : DataFrame
        DataFrame que contiene información sobre precios y posibles valores atípicos.
    - df_col_id : str
        Nombre de la columna que contiene los identificadores de los productos.
        Por defecto su valor es 'IdProducto'.
    - df_col_outlier : str
        Nombre de la columna que marca si un valor es atípico (outlier).
        Por defecto su valor es 'Outlier'.
    - df_col_fecha : str 
        Nombre de la columna que contiene los años asociados a los precios.
        Por defecto su valor es 'Fecha_Año'
    - df_col_fecha_aux : str
        Nombre de otra columna de fecha para intentar resolver el valor atípico si la columna principal de fecha no tiene un valor asociado.
        Por defecto es None.
    - df_col_precio : str 
        Nombre de la columna que contiene los precios.
        Por defecto su valor es 'Precio'
    - id : int, optional
        Identificador del producto.
        Por defecto es None.
    - año : int
        Año asociado al precio del producto.
        Por defecto su valor es None.
    - unknown_values : list 
        Lista de valores que se consideran desconocidos o no válidos.
        Por defecto su valor es [-1, '-1', 'Desconocida', 'Desconocido', None]
    - not_found : str 
        Valor a devolver cuando no se encuentra el precio correspondiente.
        Por defecto su valor es '-1'.
    Returns:
    --------
    int
        Precio correspondiente del producto para el año especificado.

    Notes:
    ------
    - Si el año es menor a 2020, se busca el precio normalizado excluyendo los valores atípicos (outliers) del DataFrame proporcionado.
    - Si el año es mayor o igual a 2020, o si no se encuentra un precio normalizado válido, se utiliza el precio del DataFrame Producto.
    - Si no se encuentra el precio correspondiente, se devuelve el valor especificado por `not_found`.
    '''

    def cargar_producto():
        PROCESADOS = 'Producto.csv'
        SEMIPROCESADOS = 'producto.csv'
        SINPROCESAR = 'producto.csv'
        try: # Busca en procesados
            dir_project = os.getcwd()
            dir_dataset = os.path.join(dir_project, 'data/Procesados')
            dir_producto = os.path.join(dir_dataset, PROCESADOS)

            df_producto = leer_archivo(dir_producto, strict_mode=False, sep=',')
        except FileNotFoundError as e:
            try:
                dir_dataset = os.path.join(dir_project, 'data/SemiProcesados')
                dir_producto = os.path.join(dir_dataset, SEMIPROCESADOS)
                df_producto = leer_archivo(dir_producto, strict_mode=False, sep=',')
            except: 
                # Próximamente se conectará a la base de datos
                print(f'ERROR ::: Producto todavía no fue procesado. dir: {dir_producto}')
                exit(1)
        except Exception as e:
            ERROR = type(e)
            print(f'ERROR ::: ({ERROR} no se pudo procesar el DataFrame producto\nERROR:{e})') 
        return df_producto
   
    if df_outlier is None:
        raise NameError("El DataFrame solicitado no tienen referencia valida")

    if unknown_values is None:
        unknown_values = [-1,'-1','Desconocida','Desconocido',None]

    if not_found is None:
        not_found = '-1'

    #Id desconocido
    if id in unknown_values:
        return not_found

    #Fecha desconocida
    if df_col_fecha in unknown_values:
        if df_col_fecha_aux is not None and df_col_fecha not in unknown_values:
            df_col_fecha = df_col_fecha_aux
        else: return not_found

    df_producto = cargar_producto()

    if (int(año) < 2020):
    #Funciones vectorizadas
        if df_col_outlier is None:
            mask = (df_outlier[df_col_id] == id) & (df_outlier[df_col_fecha] == año)
        else:
            mask = (df_outlier[df_col_id] == id) & (df_outlier[df_col_outlier] == 0) & (df_outlier[df_col_fecha] == año) #serie de booleanos
        precio_normalizado = df_outlier.loc[mask, df_col_precio].mean() 

    if int(año) >= 2020 or isnan(precio_normalizado):
        precio_producto = df_producto.loc[df_producto['IdProducto'] == id, 'Precio']
        return precio_producto.iloc[0] if not precio_producto.empty else -1
        
    return precio_normalizado if precio_normalizado is not None else -1


if __name__ == '__main__':
    pass












































































# Matias Alvarez - Data Engineer