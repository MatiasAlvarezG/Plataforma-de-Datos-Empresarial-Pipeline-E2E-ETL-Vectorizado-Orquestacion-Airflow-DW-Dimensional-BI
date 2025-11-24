# Métodos generales que son aplicados a todos o a la mayoría de los DataFrame
from pandas import DataFrame, Series, to_numeric
from re import findall, match
from unidecode import unidecode
from .IdTransformationMethods import convertir_caracteres_en_posiciones_consecutivas, renovar_id


def eliminar_espacios(cadena : str, 
                      strict_mode : bool = False) -> str:
    '''
    Elimina todos los espacios sobrantes (mayor de 1) de una cadena.
    El modo estricto retorna la cadena o None (según el caso) si su valor es falso, caso contrario retornara una excepción.
    
    Example
    ------ 
    >>> eliminar_espacios(' Matias                 Alvarez ') Retorna: 'Matias Alvarez'
    >>> eliminar_espacios('  esto   es un        ejemplo  ') Retorna: 'esto es un ejemplo'
    >>> eliminar_espacios('     ') Retorna: ''
    >>> eliminar_espacios(object(), strict_mode=False) Retorna: None
    >>> eliminar_espacios(object(), strict_mode=True) Retorna: TypeError

    '''
    if cadena is None:
        if strict_mode:
            raise ValueError('ERROR ::: La variable cadena no puede ser del tipo NoneType.')
        return None
    
    if not isinstance(cadena, str):
        if strict_mode:
            raise TypeError('ERROR ::: La variable pasada como parámetro no es una cadena.')
        return cadena
    try:
        return (' '.join(cadena.split()))
    except Exception as e:
        if not strict_mode:
            return None
        raise e
        

def detectar_nulos(obj : DataFrame | Series, 
                   strict_mode : bool = False) -> dict|int:
    '''
    Retorna la cantidad de nulos de un objeto de tipo DataFrame o Series
    Si el objeto a verificar es de tipo DataFrame entonces se retorna un diccionario que contiene como claves 
    los nombres de las columnas que contienen valores Nulos y como valores la cantidad de nulos encontrados
    Si el objeto a verificar es una Series retorna un entero con la cantidad de valores Nulos de la misma

    Example
    -----
    >>> df = DataFrame({ 'A' : [1,2,3], 'B' : [1,2,3], 'C' : [1,2,3], 'D' : [1,2,3], 'E' : [1,2,3] })
        detectar_nulos(df,strict_mode=False) Retorna: {}

    >>> df1 = DataFrame({ 'A' : [1,NaN,3], 'B' : [1,2,3], 'C' : [1,2,NaN], 'D' : [1,2,3], 'E' : [1,2,3] })
        detectar_nulos(df1,strict_mode=False) Retorna: {'A' : 1, 'C' : 1}
    
    >>> serie1 = Serie([1,2,3])
        detectar_nulos(serie1,strict_mode=False) Retorna: 0

    >>> serie2 = Serie([1,NaN,3])
        detectar_nulos(serie2,strict_mode=False) Retorna: 1

    >>> detectar_nulos(None,strict_mode=False) Retorna : {}

    >>> detectar_nulos(None,strict_mode=True) Retorna : ValueError

    '''
    if obj is None:
        if strict_mode:
            raise ValueError('ERROR ::: Debe pasar un DataFrame o una Serie y no un tipo NoneType.')
        return dict()
    
    if not isinstance(obj, (DataFrame,Series)):
        if strict_mode:
            raise TypeError('ERROR ::: Debe pasar un DataFrame o una Series como parametro.')
        return dict()
    
    try:
        if isinstance(obj, Series):
            return obj.isna().sum() 
        return {x : obj[x].isna().sum().sum() for x in obj if obj[x].isna().sum().sum()>0}
    except Exception as e:
        if not strict_mode:
            return dict()
        raise e


def metodos_generales(df : DataFrame, 
                      rellenar_nulos : str | dict,
                      transformaciones:bool = False) -> DataFrame:
    '''
    Método que hace transformaciones generales a un DataFrame.
    Elimina Duplicados y rellena nulos según lo define el usuario con el parámetro rellenar_nulos, 
    además si se pasa el parámetro transformaciones como True entonces aplica:\n\t
        + Formato Title a todas las palabras\n\t
        + Eliminación de acentos
        + Elimina espacios inecesarios.
    NOTES
    -----
    Como python hace pasaje por referencia con objetos la variable df va a sufrir cambios aunque no  asigne la variable al df mismo.
    
    Pasar un diccionario para rellenar nulos es util en casos donde se tiene columnas de distintos dtype y se necesita personalizar cada respuesta según el caso. Vea en Ejemplos un 
    caso de uso para este caso.

    Example
    ------
    df = DataFrame({ 'A' : ['MATIAS ALVAREZ','CLAUDIO Alvarez', 'mAtías      AlVAREZ   ','MATIAS ALVAREZ G'],
                               'B' : [1,2,3,1], 
                               'C' : [1,NaN,NaN,1], 
                               'D' : ['caracter',NaN,'Ver','caracter'], 
                               'E' : [4,3,2,4] })
    >>> metodos_generales(df,rellenar_nulos={'D' : 'TEST', 'C' : -1})
        Retorna el siguiente DataFrame:
                        A	        B	 C	    D	        E
            0	Matias Alvarez	    1	 1.0   Caracter	    4
            1	Claudio Alvarez	    2	-1.0   Test	        3
            2	Matias Alvarez G	3	-1.0   Ver	        2
    '''
    if df is None:
        raise ValueError('ERROR ::: El parámetro df debe corresponder a un DataFrame y no a un NoneType.')

    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: Debe pasarse un DataFrame en el parámetro df.')
    
    if not isinstance(rellenar_nulos, (int, str, dict)):
        raise TypeError('ERROR ::: Debe pasarse una cadena que represente el valor a rellenar los valores nulos o un diccionario \
                        que indique con su clave la columna a modificar del DataFrame y con su clave el valor a reemplazar en el mismo.')

    try:
        df = df.drop_duplicates(keep='first', 
                        inplace=False, 
                        ignore_index=True)

        df = df.fillna(rellenar_nulos, inplace=False)

        if transformaciones:
            df = df.map(lambda x : eliminar_espacios(unidecode(x))
                        .title() if isinstance(x, str) else x ) 
            
        return df
            
    except Exception as e:
        raise e


#Metodos para normalizar valores con un diccionario sin usar str.replace o Series|DataFrame.replace()
def normalizar_valores(serie : Series, diccionario : dict) -> None:
    '''
    Normaliza valores de una serie a través del parámetro diccionario, se utiliza en casos donde el uso de 
    los métodos .str.replace() o .replace() genera problemas al matchear varias coincidencias.
    Recorre cada palabra y reemplaza la palabra si esta existe en el diccionario.
    Complejidad: O(N*M)
    '''
    if serie is None:
        raise ValueError('ERROR ::: El parámetro serie debe corresponder a una Serie y no a un NoneType.')
    
    if not isinstance(serie,Series):
        raise TypeError('ERROR ::: El parámetro serie debe corresponder a una Serie.')
    
    if diccionario is None:
        raise ValueError('ERROR ::: El parámetro diccionario debe corresponder a una diccionario y no a un NoneType')
    
    if not isinstance(diccionario,dict):
        raise TypeError('ERROR ::: El parámetro diccionario debe corresponder a un diccionario.')

    try:
        for idx, fila in enumerate(serie):
            palabras = fila.split()
            palabras_normalizadas = [diccionario.get(palabra, palabra) for palabra in palabras]
            oracion_normalizada = ' '.join(palabras_normalizadas)
            serie.at[idx] = oracion_normalizada
    except Exception as e:
        raise e


def almacenar_incosistencia(df : DataFrame, 
                            col: str, 
                            mask : Series, 
                            col_error : str) -> DataFrame:
    '''
    Se almacenan los datos considerados erróneos de la columna pasada en el parámetro ´col´ basados 
    en la series ´mask´ en la columna proporcionada ´col_error´
    Es útil en casos donde se van a modificar datos pero se quiere tener una copia original para 
    algún caso de uso del cliente.
    Si bien se puede utilizar el método como un comando se recomienda asignar el retorno de la función.

    Example
    -----

        >>> df = DataFrame({'A' : [0,1], 'B' : [1,1], 'Error' : [0,0]})\n
        mask = df['A'].eq(1) \n
        df = almacenar_incosistencia(df, 'A', mask, 'Error')\n

        print(df) 
        # DataFrame: {'A' : [0,1], 'B' : [1,1], 'Error' : [0,1]}

        >>> df = DataFrame({'Precio' : [1510394.00,151.03], 'Fecha' : [12-4-2024,13-4-2024], 'Precio_ERRONEO' : [-1,-1]})\n
        mask = df['Precio'].ge(10000) \n
        df = almacenar_incosistencia(df, 'Precio', mask, 'Precio_ERRONEO')\n

        print(df) 
        # DataFrame: {'A' : [1510394.00,151.03], 'B' : [12-4-2024,13-4-2024], 'Error' : [1510394.00,-1]}

       >>> df = DataFrame({ 'A' : ['MATIAS ALVAREZ','CARLOS ALBERTO Fernandez', 'mAtías      AlVAREZ   ','MATIAS ALVAREZ'],
                               'B' : [1,2,3,1], 
                               'C' : [1,NaN,NaN,1], 
                               'D' : [NaN,NaN,NaN,NaN], 
                               'E' : [4,3,2,4],
                               'Error' : [None, None, None, None]}
        mask = df['A'].eq('MATIAS ALVAREZ')

        IMPORTANTE: 
            Si se pasa df[['A','Error']] como parámetro de df entonces hay que almacenar el resultado en la variable df[['A','Error']]. 
            Ya que si se utiliza como un comando no va a realizar cambios en los valores de las columnas, esto se debe a como pandas funciona 
            al utilizar slice en un DataFrame.

        df[['A','Error']] = almacenar_inconsistencia(df[['A','Error']], 'A', mask, 'Error')

        # Aunque puede utilizar 
        almacenar_inconsistencia(df, 'A', mask, 'Error') ó df = almacenar_inconsistencia(df, 'A', mask, 'Error') con el mismo resultado.
    '''
    if df is None:
        raise ValueError('ERROR ::: El parámetro df debe corresponder a un DataFrame y no a un NoneType.')

    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: Debe pasarse un DataFrame en el parámetro df.')
    
    if col is None:
        raise ValueError('ERROR ::: El parámetro col debe corresponder a una cadena y no a un NoneType.')

    if not isinstance(col,str):
        raise TypeError('ERROR ::: Se esperaba una cadena que represente una columna valida del DataFrame.') 
    if mask is None:
        raise ValueError('ERROR ::: El parámetro mask debe corresponder a un DataFrame y no a un NoneType.')

    if not isinstance(mask,Series):
        raise TypeError('ERROR ::: El parámetro serie debe corresponder a una Serie.') 
    
    if col_error is None:
        raise ValueError('ERROR ::: El parámetro col_error debe corresponder a un DataFrame y no a un NoneType.')

    try:
        errores = df[col][mask]
        if not errores.empty: 
            df.loc[errores.index, col_error] = errores  
            return df

    except KeyError as e:
        raise KeyError(f'ERROR ::: La columna {col} no existe en el DataFrame')
    except Exception as e:
        raise type(e)(f'ERROR ::: Hubo un error al procesar la solicitud: {e}')
    

def limpiar_y_castear_columna(df : DataFrame, 
        columna : str, 
        columna_error : str,
        filtrar : str = '[^\\d]',
        surrogate_key : bool = False,
        mode_sk : str = None,
        new_type : str = 'integer', 
        rellenar_nulos : int|float|str|dict = None ):
    '''
    Castea los valores de una columna especificada en un DataFrame a un tipo de datos específico (int y float), filtrando y rellenando los valores incorrectos.
    El requerimiento del cliente actualizar los nuevos valores en tablas que utilicen los IDs a actualizar

    Parameters:
    -----------
        + df : DataFrame 
            El DataFrame que contiene los datos.
        + columna : str
            El nombre de la columna que se va a castear.
        + columna_error : str
            El nombre de la columna donde se almacenarán los valores incorrectos.
        + filtrar : str
            La expresión regular utilizada para filtrar los valores incorrectos.
            Por defecto, '[^\\d]', que filtra cualquier cosa que no sea dígito.
        + surrogate_key : str
            Hace que en vez de aplicar valores NaN y luego aplicarles los valores de rellenar_nulos se utilicen algunos de los métodos provistos
            para quitar los elementos no enteros por otros.
            Por defecto, es Falso.
        + mode_sk : str
            String que hace referencia al tipo de Surrogate Key a utilizar
            Puede ser 'consecutive' o 'auto_incremental'
                'consecutive' : Utiliza el metodo convertir_caracteres_en_posiciones_consecutivas(...) esto hace que los IDs a arreglar sigan un orden.
                                Es decir, si se tiene los siguientes IDs: [1,2,'a',4,'b','a'] entonces los IDs serán ahora: [1,2,3,4,5,3].
                                Notar que 'a' esta 2 veces y mantiene el mismo valor y además sigue el orden de los valores.
                'auto_incremental' : Utiliza el metodo renovar_id(...) esto hace que se utilice como nuevo ID el siguiente del ultimo ID valido (entero)
                                     Es decir, si se tiene los siguientes IDs: [1,2,'a',4,'b','a'] entonces los IDs serán ahora: [5,6,7,8,9,7]
                                     Se pasa el parámetro add_row_empty como False haciendo que si no hay IDs para modificar el DataFrame no agregue ningún registro inicial.
            Por defecto es None
        + new_type : str
            El tipo de datos al que se va a castear la columna.
            Puede ser  'integer', 'signed', 'unsigned' o 'float'.
            Por defecto, 'integer'.
        + rellenar_nulos : int | float | str | dict 
            El valor utilizado para rellenar los valores nulos después del cast.
            Por defecto, None, lo que significa que no se rellenarán los valores nulos.

    Raises
    --------
        ValueError: Si df es None, columna o columna_error es None, new_type es None o filtrar es None.
        TypeError: Si new_type no es una cadena o filtrar no es una cadena que hace referencia a una expresión regular.
    '''
    if df is None:
        raise ValueError('ERROR ::: El parámetro df debe ser un DataFrame y no NoneType.')
    
    if None in [columna, columna_error]:
        raise ValueError('ERROR ::: El parámetro columna o columna_error es/son  NoneType.')
    
    if new_type is None:
        raise ValueError('ERROR ::: El parámetro new_type no puede ser NoneType.')
    
    if not isinstance(new_type, str):
        raise TypeError('ERROR ::: El parámetro new_type debe ser una cadena. Por ejemplo: \'integer\'.')
    
    if filtrar is None:
        raise ValueError('ERROR ::: El parámetro filtrar no puede ser NoneType.')
    
    if not isinstance(filtrar, str):
        raise TypeError('ERROR ::: El parámetro filtrar debe ser una cadena referenciando un regex. Por Ejemplo: \'[^\\d]\'.')

    if isinstance(surrogate_key,bool):
        if mode_sk is None:
            mode_sk = 'consecutive'
    else: 
        surrogate_key=False

    # En caso de no ser entera se eliminan los datos no numéricos.
    mask = df[columna].str.contains(filtrar, 
                                    regex=True,
                                    na=False) 

    # Se almacena los datos erróneos en la columna para ese fin. (Error_*).
    almacenar_incosistencia(df=df, 
                            col=columna, 
                            mask=mask, 
                            col_error=columna_error)

    # Se aplica una clave artificial 
    if surrogate_key:
        if mode_sk=='consecutive':
            df[columna] = df[columna].apply(convertir_caracteres_en_posiciones_consecutivas).astype('int64', errors='ignore')
        elif mode_sk=='auto_incremental':
            renovar_id(df=df, 
                      columna_id_df=columna, 
                      id_desactualizado=df[columna].values(),
                      add_row_empty=False,
                      df_actualizar=None, 
                      columna_df_actualizar=None)

    # Se reemplazan los datos erróneos por -1 y se castea a dtype entero.
    else:
        # se utiliza 'coerce' para que convierta los datos inválidos en NaN y luego esto son modificados por rellenar nulos
        df[columna] = to_numeric(df[columna], 
                                errors='coerce', 
                                downcast=new_type)\
                       .fillna(rellenar_nulos)
    
    return df


def tiene_numeros_sin_separar(cadena: str)  -> bool:
    '''
    Retorna un booleano que representa si una cadena contiene números que están separados.

    Nota
    -------
    No puede detectar aquellos casos donde hay números entre medio de letras.
    Por ejemplo: 
    >>> tiene_numeros_sin_separar('Sucursal a2a 2') , Retorna: False

    Example
    --------
    >>> tiene_numeros_sin_separar('a') Retorna: False
    >>> tiene_numeros_sin_separar('a1') Retorna: True
    >>> tiene_numeros_sin_separar('a 1') Retorna: False
    >>> tiene_numeros_sin_separar('a1 1') Retorna: False
    >>> tiene_numeros_sin_separar('') Retorna: False
    >>> df[col].apply(tiene_numeros_sin_separar) : Retorna: Series[bool]
    '''
    return bool(match(r'^[A-Za-z]+\d+[A-Za-z]*$', cadena))


def extraer_numeros(texto) -> str:
    '''
    Extrae números de un texto.

    Example
    ------
    >>> extraer_numeros('abc 23') , Retorna: '23' 
    >>> extraer_numeros('xyz 45') , Retorna: '45'
    >>> extraer_numeros('123 def') , Retorna: '123'
    >>> extraer_numeros('123 def 456') , Retorna: '123,456' 
    >>> extraer_numeros('def') , Retorna: '' 

    '''
    if not isinstance(texto, str):
        raise TypeError('ERROR ::: El parámetro texto no es de tipo cadena.')
    numeros = findall(r'\d+', texto)
    
    return ','.join(numeros)














































# Matias Alvarez - Data Engineer