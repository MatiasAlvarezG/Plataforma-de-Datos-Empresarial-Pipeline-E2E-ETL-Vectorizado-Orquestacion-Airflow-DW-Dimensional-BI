# Métodos que actualizan valores en los DataFrames.

from pandas import DataFrame,Series
from numpy import ndarray, isnan, random


def buscar_posicion_a_insertar(lista : list, numero : int) -> int | None:
    '''
    Retorna la posición de la lista donde debe insertarse numero de forma ordenada.
    Si el numero existe en la lista entonces se retorna None.
    La lista debe tener un orden ya que utiliza la logica de busqueda binaria.
    
    >>> buscar_posicion([1,24,52,100], 34)
        Retorna: 2 , ya que el valor 24 (pos: 1) es menor a 34 y el valor 52 (pos: 3) es mayor que 34.
    '''
    def __buscar_posicion__(lista, valor, pos_ini, pos_fin) -> int | None:
        if pos_ini <= pos_fin:
            pos = (pos_ini + pos_fin) // 2
            if lista[pos] == valor:
                return None
            if lista[pos] < valor:
                return __buscar_posicion__(lista, valor, pos + 1, pos_fin)
            else:
                return __buscar_posicion__(lista, valor, pos_ini, pos - 1)
        else:
            return pos_ini

    if lista is None:
        raise ValueError('ERROR ::: El parámetro lista debe ser una lista y no un NoneType.')
    
    if not isinstance(lista, (list,ndarray)):
        raise TypeError('ERROR ::: Debe pasar una lista un array para utilizar el metodo.')
    
    if numero is None:
        raise ValueError('ERROR ::: El parámetro numero no puede ser de tipo NoneType.')

    if not isinstance(numero, int):
        raise TypeError('ERROR ::: Numero debe ser un entero.')
    return __buscar_posicion__(lista, numero, 0, len(lista) - 1)


def convertir_caracteres_en_posiciones_consecutivas( lista : Series | list | ndarray) -> list:
    '''
    Convierte los elementos que no son números enteros en números consecutivos validos de los elementos de la lista.
    Un numero consecutivo válido es aquel valor siguiente al ultimo elemento entero encontrado.
    Logra que no haya duplicados.

    Caso de uso
    ----- 
    Cuando se tiene IDs que solo representan el ID único de una tabla pero contienen valores alfanuméricos o números no enteros y se requiere que solo sean enteros auto incrementales.
    No actualizan valores en otras tablas.
    Por ende NO SE RECOMIENDA su uso para IDs que se utilizaran como FK en otras tablas, para eso hay otras opciones:\n\t
    + ´actualizar_id_desactualizados()´ : Actualiza cada IDs incorrecto pasado como parámetro (un escalar si es un único valor o una lista si es un conjunto  de IDs) por el valor del tamaño de la tabla aumentado en 1. Se asume que la tabla sigue un patrón de tabla surrogada (auto incremental).
    + ´descartar_registros_duplicados_con_id_distintos()´ : Detecta IDs con distintos valores que corresponden a valores duplicados (por un conjunto de columnas pasadas como un parámetro) y las actualizan por el ID correcto (que corresponde a la primera coincidencia, el resto son IDs incorrectos). 
                                                            Luego se actualizan la/s tabla/s que contienes el/los ID/s como FK (si es que se pasaron como parámetros).

    Parameters
    --
    lista : Series | list | ndarray
        Variable a corregir, puede ser una Series, una lista o un ndarray.

    Return
    ---
    resultado : list
        Lista con los elementos corregidos.

    Examples:
    ---

    df_ejemplo = DataFrame( {'ID' : [ 0, 2, 3, 4, 5, 6, 7, 8, 9, 'hola', 11, 123, 1456, 'e', 'f', 'g' ]} )
    >>> convertir_caracteres_en_posiciones_consecutivas(df_ejemplo.ID) 
    Retorna : [0, 2, 3, 4, 5, 6, 7, 8, 9, 1, 11, 123, 1456, 10, 12, 13]

    >>> convertir_caracteres_en_posiciones_consecutivas([1, 2, 3, 'hola']) Retorna : [1, 2, 3, 0]
    >>> convertir_caracteres_en_posiciones_consecutivas(['a', 'b', 'c', 1]) Retorna : [0, 2, 3, 1]
    >>> convertir_caracteres_en_posiciones_consecutivas([]) Retorna : []
    >>> convertir_caracteres_en_posiciones_consecutivas(['a','b','c',12.4,18.4,18.9]) Retorna : [0,1,2,3,4,5]
    '''
    if lista is None:
        return None
    
    if not isinstance(lista, (list,Series)):
        raise TypeError('ERROR ::: Se esperaba que el parámetro lista sea de tipo List o una Series')
    
    lista_ordenada = []
    posicion_arreglar = dict() #{}

    posibles_valores = len(lista)

    for idx,valor in enumerate(lista):
        if not isinstance(valor,int):
            try:
                str(valor)
            except Exception as e:
                ERROR = type(e)
                raise ERROR(f'ERROR ::: No se pudo procesar la solicitud \
                            porque el tipo de dato no puede ser transformado a cadena (pos: {idx}).\n \
                            ERROR : {e}')
            #Veo si existe en el diccionario
            if posicion_arreglar.get(valor, None) is None:
                posicion_arreglar[valor] = [idx]
            else:
                posicion_arreglar[valor].append(idx)

        else:
            # almaceno el valor de forma ordenada en la lista de valores validos
            posicion = buscar_posicion_a_insertar(lista_ordenada, valor)
            lista_ordenada = lista_ordenada[0:posicion] + [valor] + lista_ordenada[posicion:] \
                                if isinstance(posicion, int) else lista_ordenada

    #cantidad de valores que necesito
    claves_incorrectas = list(posicion_arreglar.items()) 

    # Empiezo a reemplazar los valores
    for valor in range(0,posibles_valores):
        # Si el valor todavía no se uso
        if valor not in lista_ordenada:
                # entonces lo uso como nuevo primer valor
                if len(claves_incorrectas)>0:
                    claves = claves_incorrectas.pop(0) 
                else: continue

                for clave in claves:
                    for posicion in posicion_arreglar[clave]:
                        lista[posicion] = valor
                    break
    return lista


def propagar_valores(dfs : list | DataFrame, 
                     columna_actualizar,
                     old_value : int,
                     new_value)-> None:
        '''
        Actualiza los valores de un o varios DataFrame en una columna especificada con un valor especificado
        Si la columna especificada no existe simplemente ignorara la actualización.

        Parameters
        -----
        + dfs : list , DataFrame
            Contiene el o los DataFrame donde se debe actualizar el valor viejo.
        + columna_actualizar
            Columna que contiene los valores a actualizar.
        + old_value : Scalar | list
            Representa el/los viejos valor/es que debe actualizarse.
        + new_value : int
            Representa el nuevo valor a actualizar.

        Notes
        --------
        Asumamos que existen una lista que contiene un conjuntos de DataFrame que contienen tres columnas: 'ID', 'col2' y 'col3' con distintos valores.
        Supongamos que cada elementos de la lista tiene un nombre como df<1..N> donde N indica el numero de elementos de la lista.
        Si se llama al método haciendo un slice del mismo esto generará que no se modifiquen los valores de este ya que internamente
        pandas trata distinto a una variable df con slice que una sin slice.
        Entonces:
        >>> propagar_valores([df1,df2,df3],'ID', 123, 9991)
            Este metodo modificara el valor interno de los 3 DataFrame en la columna ID donde exista el valor 123 por el valor 9991

        >>> propagar_valores([df1[['ID']], df2[['ID']], df3[['ID']]], 'ID', 123, 9991)
        >>> propagar_valores([df1[['ID', 'col2']],
                              df2[['ID', 'col2']],
                              df3[['ID', 'col2']]
                             ],'ID', 123, 9991)
        >>> propagar_valores([df1[['ID', 'col2', 'col3']],
                              df2[['ID', 'col2', 'col3']],df3[['ID', 'col2', 'col3']]
                             ],'ID', 123, 9991)
        En estos 3 casos la variables ´df<1..N>´ no fueron modificadas internamente ya que la referencia que existe en la misma es distinta que 
        cuando se pasa un slice del mismo por ende no se modifica los valores internos.
        '''
        if not isinstance(dfs, (DataFrame,list,ndarray)):
            raise TypeError('ERROR ::: Se espera que el parámetro dfs sea un DataFrame o una lista que contiene DataFrame.')
        
        if isinstance(dfs, DataFrame):
            dfs = [dfs]

        # Permite utilizar el método .isin() aunque solo sea un valor escalar
        if not isinstance(old_value, (list,ndarray)):
            old_value = [old_value]

        for df in dfs:
            if isinstance(df, DataFrame):
                if columna_actualizar not in df:
                    continue
                df[columna_actualizar].mask(df[columna_actualizar].isin(old_value), new_value, inplace=True)

  
def renovar_id(df : DataFrame, 
               columna_id_df : str, 
               old_id : int|str|list, 
               add_row_empty : bool = False,
               df_actualizar : DataFrame | list = None, 
               columna_df_actualizar : str = None):
    '''
        Renueva los ID enteros desactualizados en la columna especificada.
        El valor utilizado para la actualización es el siguiente valor del ultimo id valido del DataFrame. 
        Dada esta característica se recomienda utilizar como parámetro ´df´ el DataFrame que contenga mayor cantidad de valores de IDs priemero si quiere propagar valores.
        La renovación se puede propagar a otro/s DataFrames utilizando el parámetro df_actualizar (donde puede pasarse una lista de DataFrame o un simple DataFrame).
        En próxima actualización se agregará para que revise la información con la base de datos (con la tabla tbl_ultimo_id).

        id_desactualizado : es el id viejo que hay que actualizar. Requiere que si es una lista los elementos de los mismos no sean listas.

        Nota:
            Cuando se quiere actualizar registros que tienen valores duplicados pero distintos IDs (lo que llamo duplicación no directa) se recomienda utilizar el método `descartar_registros_duplicados_con_id_distintos`
            que descarta los registros que tienen la misma información pero por algún problema no tienen el ID correcto.
            Este método mantiene IDs repetidos con su nuevo valor, por ejemplo, si tiene los IDs: ['a',2,'a'] al renovar IDs estos quedaran: [3,2,3].
            Se modifica el dtype de la columna a int64.
            
        Parameters
        ---
        + df : DataFrame
            DataFrame que contiene los IDs originales desactualizados.
        + columna_id_df : str
            Columna del DataFrame que contiene los IDs .
        + old_id : int | str | list
            Id desactualizado puede ser un Escalar o puede ser una lista. Si es una lista entonces quiere decir que son un conjuntos de IDs que deben actualizarse.
        + add_row_empty : bool
            Indica que en caso de pasarse un columna del DataFrame que no contiene valores que ingrese un primer valor con ID 1 y ponga en el resto de las columna NaN.
            En caso que se pase como False simplemente se ignora y el DataFrame queda igual.
            Por defecto su valor es False.
        + df_actualizar (optional) : DataFrame | list 
            Indica el DataFrame (o los DataFrames si es una lista) a actualizar.
            Por defecto su valor es None.
        + columna_df_actualizar (optional) : str
            Indica la columna que contiene los Id desactualizado.\n\t   REQUIERE QUE LA COLUMNA TENGA EL MISMO NOMBRE SI SE PASA UN CONJUNTO DE DATAFRAMES.
            Por defecto su valor es None.
        Example
        -----
        df_devolucion = DataFrame({'IdCliente' : [1,2,3,4,5,2,6,1,3],
                        'Devolucion' :[True,False,False,False,False,False,False,False,True]})

        df_compra = DataFrame({'IdCliente' : [3,6,2,4,5,3,2,1,1],
                  'IdCompra' : [['1AB','01B'],['00B'],['DD1'],['DD2','DD1','1AB'],
                               ['BBA'],['BCA','DD1'],['1AB','DD2'],['DD3'],['00B','B10']]
                })

        df_info = DataFrame({'IdCliente' : [1,2,3,4,5,6,7,8,9,10,11,12],
                 'Nombre' : ['Matias Alvarez','NN1','NN2','NN3','NN4','NN5',
                            'NN6','NN7','NN8','NN9','NN10','NN11']
                })

            #Modifico el ID con valor 1
        >>> renovar_id(df_info, 'IdCliente', 1, [df_devolucion, df_compra], 'IdCliente')
            Los DataFrames quedan como los siguiente:  
                DataFrame({'IdCliente' : [13,2,3,4,5,2,6,13,3],
                           'Devolucion' :[True,False,False,False,False,False,False,False,True]})

                DataFrame({'IdCliente' : [3,6,2,4,5,3,2,13,13],
                        'IdCompra' : [['1AB','01B'],['00B'],['DD1'],['DD2','DD1','1AB'],
                                      ['BBA'],['BCA','DD1'],['1AB','DD2'],['DD3'],['00B','B10']]
                        })

                DataFrame({'IdCliente' : [13,2,3,4,5,6,7,8,9,10,11,12],
                           'Nombre' : ['Matias Alvarez','NN1','NN2','NN3','NN4','NN5',
                                       'NN6','NN7','NN8','NN9','NN10','NN11']
                        })
        >>> renovar_id(None,'IdCliente',9, [df_devolucion, df_compra], 'IdCliente')
            Retorna: TypeError

        >>> renovar_id(df_info,'NOEXISTE',100, [df_devolucion, df_compra], 'IdCliente')
            Retorna: TypeError

        >>> renovar_id(DataFrame({'ID' : [1,2,3]}), 'ID', 2, None, None})
                Retorna: DataFrame({'ID' : [1,4,3]})

        #Casos donde se pasa un ID inexistente
        >>> renovar_id(DataFrame({'ID' : [1,2,3]}), 'ID', 9, None, None})
                Retorna: DataFrame({'ID' : [1,2,3]})

        >>> renovar_id(DataFrame({'ID' : []}), 'ID', 9, None, None})
                Retorna: DataFrame({'ID' : [1], 'col1' : [NaN]})

        #Casos donde se modifican varios IDs
        >>> renovar_id(DataFrame({'ID' : []}), 'ID', df[ID].values, add_row_empty=True, None, None})
            Retorna: DataFrame({'ID' : [1]})

        >>> renovar_id(DataFrame({'ID' : []}), 'ID', df[ID].values, add_row_empty=False, None, None})
            Retorna: DataFrame({'ID' : []})

        >>> renovar_id(DataFrame({'ID' : [1,2,3]}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [4,5,6]})

        >>> renovar_id(DataFrame({'ID' : [1]}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [2]})

        >>> renovar_id(DataFrame({'ID' : ['A']}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [1]})

        >>> renovar_id(DataFrame({'ID' : ['a','b','c','d']}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [1,2,3,4]})

        >>> renovar_id(DataFrame({'ID' : [1,'A']}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [2,3]})

        >>> renovar_id(DataFrame({'ID' : ['a',1]}), 'ID', df[ID].values, None, None})
            Retorna: DataFrame({'ID': [2,3]})

        # IDs inexistentes a actualizar
        >>> renovar_id(DataFrame({'ID' : ['a',1]}), 'ID', [8,6,2], None, None})
            Retorna: DataFrame({'ID': ['a',1]})
    '''

    def actualizar_valor(old_id, max_id):
        '''Actualiza los registros donde existe el id desactualizado'''

        #Indices a modificar los valores viejos en el DF principal
        idx_old_id = df[columna_id_df] == old_id
        indices_df = df[idx_old_id].index

        # No existen IDs para reemplazar en el DataFrame principal
        if indices_df.empty:
            return

        # Modifico los índices
        df.loc[indices_df, columna_id_df] = max_id + 1

        # Actualizo la información en los otros DFs requeridos
        if df_actualizar is not None:
            propagar_valores(df_actualizar, columna_df_actualizar, old_id, max_id + 1)

    # Bloque principal     
    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: El parámetro df no es un DataFrame.')
    
    if not (columna_id_df in df.columns):
        raise KeyError(f'ERROR ::: La columna {columna_id_df} no existe en el DataFrame.')

    # DataFrame que contiene la columna provista no contiene elementos
    if df[columna_id_df].empty:
        # Si se especifico que se agregue un valor inicial
        if add_row_empty:
            df.loc[0, columna_id_df] = 1
            df[columna_id_df] = df[columna_id_df].astype('int64', copy=False, errors='raise')
        return 
    
    if not isinstance(old_id, (list,ndarray)):
        old_id = [old_id]

    # No contiene valores enteros
    ids_enteros = None
    if df[columna_id_df].dtype != 'int64':
        #Los filtro
        ids_enteros = df[columna_id_df].str.contains('[^\\d]', 
                                                     regex=True,
                                                     na=False)
        # Casteo a enteros
        ids_enteros = df[columna_id_df][~ids_enteros].astype('int64',errors='raise')

    # Obtengo el max_id
    max_id = ids_enteros.max() if ids_enteros is not None else df[columna_id_df].max()

    # Casos donde no hay mayor valor
    if isnan(max_id): 
        max_id = 0

    for id in old_id:
        actualizar_valor(id, max_id)
        max_id += 1


def descartar_registros_duplicados_con_id_distintos(df : DataFrame, 
                                                    columnas_duplicadas_df, 
                                                    columna_id_df : str, 
                                                    df_actualizar : str | list = None, 
                                                    columna_df_actualizar : str = None):
    '''
    Descarta (no elimina) los registros duplicados que contienen distintos IDs por error y actualiza los IDs incorrectos.
    Agrega las columnas '{columna_id_df}_Descartado' y '{columna_id_df}_Correcto' que permiten identificar si un ID es incorrecto
    y a que ID real pertenece, respectivamente.
    
    Identifica como duplicados aquellos registros que se repiten con las columnas especificadas en 'columnas_duplicadas_df'.
    Luego, asigna el primer ID encontrado como el correcto y los demás como IDs erróneos, marcándolos para descartar.
    Si se proporciona un DataFrame para actualizar ('df_actualizar') y una columna ('columna_df_actualizar'), los IDs incorrectos 
    serán actualizados con el valor correcto en ese DataFrame.

    Example
    -------
    Por ejemplo, si se tiene el siguiente DataFrame:
    >>> df = DataFrame({'id' : [1, 2, 3, 4], 'direccion' : ['aaa', 'bbb', 'ccc', 'aaa']})
    Aplicar directamente df.drop_duplicated(inplace=True) no eliminará registros.
    Aplicar directamente df.drop_duplicates(subset=['Direccion'], inplace=True) Eliminará el último registro.
    
    >>> descartar_registros_duplicados_con_id_distintos(df, 'direccion')}
     Ahora se procesará y modificará el DataFrame agregando columnas como '{columna_id_df}_Descartado' y '{columna_id_df}_Correcto'.
    '{columna_id_df}_Descartado' indica con el valor 1 que un registro está descartado y con 0 en caso contrario.
    '{columna_id_df}_Correcto' indica el ID correcto al que pertenece el valor; si el ID es correcto, su valor es su propio ID.
    
    El objeto quedara de la siguiente forma: 
	DataFrame({'id' : [1, 2, 3, 4], 'direccion' : ['aaa', 'bbb', 'ccc', 'aaa'], 'id_Descartado' : [0,0,0,1], 'id_Correcto' : [1,2,3,1})

    Parameters:
    ---
    + df : DataFrame
        DataFrame principal que contiene los duplicados.
    + columnas_duplicadas_df : list
        Lista de nombres de columnas utilizadas para identificar duplicados en el DataFrame principal 'df'.
    + columna_id_df : str
        Nombre de la columna de identificación en el DataFrame principal.
    + df_actualizar : DataFrame | list
        DataFrame opcional o lista de DataFrames a actualizar.
	Requiere que si es una lista los elementos sean instancias de DataFrame.
    + columna_df_actualizar : str
        Nombre de la columna de identificación en el DataFrame a actualizar. 
        Es obligatorio que la columna sea la misma en todos los DataFrames si 'df_actualizar' es una lista que contiene DataFrames en sus elementos.

    Notes:
    ---
    - Se asume que los valores de las columnas de identificación en ambos DataFrames son numéricos.
    - Las columnas '{columna_id_df}_Descartado' y '{columna_id_df}_Correcto' se agregarán al DataFrame principal si no existen.
    - La función maneja casos en los que las dimensiones de los objetos no son compatibles.  
    '''
    if df is None:
        raise ValueError('ERROR ::: El parámetro df no puede ser de tipo NoneType.')
    
    if columnas_duplicadas_df is None:
        raise ValueError('ERROR ::: El parámetro columnas_duplicadas_df no puede ser de tipo NoneType.')

    if columna_id_df is None:
        raise ValueError('ERROR ::: El parámetro columna_id_df no puede ser de tipo NoneType.')

    duplicados = df[df.duplicated(subset=columnas_duplicadas_df, keep=False)]

    #No se utiliza comprobar_existencia() porque no quiero un log
    columa_descartada = (f'{columna_id_df}_Descartado')
    if not columa_descartada in df.columns:
        df[columa_descartada] = 0

    # Se agrega columna para almacenar el valor correcto de los IDs propuestos
    # Se asume inicialmente que todos los IDs son correctos
    columna_id_original = f'{columna_id_df}_Correcto'
    if not columna_id_original in df.columns:
        df[columna_id_original] = df[columna_id_df]

    # Existen duplicados
    if not duplicados.empty:
        for _, grupo_duplicados in duplicados.groupby(by=columnas_duplicadas_df):
            idx_mantener = grupo_duplicados.index[0]
            id_mantener = grupo_duplicados.at[idx_mantener, columna_id_df]

            if not df_actualizar is None:
                try:
                    # Todos los Ids a descartar en otro/s DataFrame/s. (se empieza en 1 porque el primer elemento es el valor a mantener)
                    id_descartar = grupo_duplicados[columna_id_df].values[1:]

                    propagar_valores(df_actualizar, 
                                     columna_df_actualizar, 
                                     old_value=id_descartar, 
                                     new_value=id_mantener)
                except Exception as e:
                    ERROR = type(Exception)
                    raise ERROR(f'ERROR ::: No se pudo procesar duplicados por un error desconocido: {e}.')

        # Índices a descartar
        idx_descartar = grupo_duplicados.index[1:]

        # Almaceno el ID correcto al que hace referencia el ID duplicado
        df.loc[idx_descartar, columna_id_original] = grupo_duplicados.at[idx_mantener, columna_id_df]

        # Marco que los IDs incorrecto fueron descartados
        df.loc[idx_descartar, columa_descartada] = 1

        # Para asegurar que las nuevas columnas queden con valores enteros
        df[columa_descartada] = df[columa_descartada].astype(int, errors='raise')
        df[columna_id_original] = df[columna_id_original].astype(int, errors='raise')


def actualizar_id_duplicado_serie(serie : Series, 
                                  error : int = -1, 
                                  rnd : int = 5000) -> int:
    '''
    Retorna un nuevo ID ALEATORIO (entero) a cada registro de una serie QUE CONTIENE SOLO DUPLICADOS DIRECTOS verificando que no exista en la misma.

    Parameters
    ---------
    + serie : Series
        Serie que contiene los IDs a MODIFICAR.
    + error : int
        Referencia al valor a retornar si hay un error.
        Por defecto su valor es -1.
    + rnd : int
        Se utiliza como rango para calcular un valor aleatorio.
        Por defecto es 5000.

    Notes
    ---------
    El calculo para el nuevo ID es el siguiente: new_id = (maximo_id_de_la_serie + 1) + random.randint(rnd)

    Example
    --------
    serie = Series([1,2,3,4,5,1,1])

    >>> serie.apply(lambda x : actualizar_id_duplicado_serie(serie=test))
    Retorna:
        0    2281
        1     347
        2    4251
        3    1079
        4    4242
        5    4412
        6     123
        dtype: int64
    '''
    if serie is None:
        raise ValueError('ERROR ::: Se esperaba una Series y no un NoneType.')

    if not isinstance(serie, Series):
        raise TypeError('ERROR ::: Se esperaba una Serie.')

    if not isinstance(error, int):
        error=-1

    if not isinstance(rnd, int):
        try:
            rnd = abs(int(rnd))
        except:
            rnd = 5000

    max_id = serie.max() 

    # Si la serie no tiene un máximo o no contiene enteros
    if ( isinstance(max_id, float) and isnan(max_id) ): #or not isinstance(max_id, int): 
        max_id = 1

    while max_id in serie.values:
        try:
            max_id = int(max_id) + 1 + random.randint(rnd)
        except:
            return error

    return max_id


def actualizar_id_duplicado(df : DataFrame, 
                        col : str, 
                        error : int = -1, 
                        rnd : int = 5000) -> DataFrame:
    '''
    Actualiza un DataFrame que contiene todos valores duplicados en su registro y los actualiza con un valor ALEATORIO no utilizado.

    Parameters
    ---------
    + df : DataFrame
        DataFrame que contiene los IDs a MODIFICAR.
    + col : str
        Columna que contiene los IDs duplicados a modificar.
    + error : int
        Referencia al valor a retornar si hay un error.
        Por defecto es -1
    + rnd : int
        Se utiliza como rango para calcular un valor aleatorio.
        Por defecto es 5000.

    Notes
    ---------
    El calculo para el nuevo ID es el siguiente: new_id = (maximo_id_de_la_serie + 1) + random.randint(rnd)

    Examples
    --------
    df = DataFrame({'id' : [1,2,3,2,1,1,4,5,5,4,3,6,7,8,9,10], 'foo' : [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]})
    duplicados = df[df['id'].duplicated(keep='first')]
    La variable duplicado seria como lo siguiente: DataFrame({'id' : [1,2,3,4,5,1,1], 'foo' : [0,0,0,0,0,0,0]})
    >>> df.loc[duplicado.index, 'id'] = actualizar_id_duplicado(df=duplicados, col='id', error=-1, rnd = 5000)
    Retorna: DataFrame({'id' : [1,2,3,2421,4512,1567,4,5,5512,4997,3221,6,7,8,9,10], 'foo' : [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]})
    '''
    if df is None:
        raise ValueError('ERROR ::: Se esperaba que el parametro fuera un DataFrame y no un NoneType.')
    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: Se esperaba que el parametro df fuera un DataFrame.')

    df[col] = df[col].apply(lambda x : actualizar_id_duplicado_serie(serie=df[col], error=error, rnd=rnd))
    return df


def procesar_duplicados_identicos(df : DataFrame, 
                                  columna_id : str,
                                  procesar_desconocidos : bool = False):
    '''
    Procesa los duplicados de un DataFrame.
    Se recomienda utilizarlo con columnas que serán reconocidas como IDs.
    Tener en cuenta que los métodos generales eliminan los registros/filas duplicados esto hace que si hay IDs duplicados
    los mismos sean por un error de procesamiento del mismo ya que sus datos almacenados son distintos al del registro duplicado.
    Puede evitar procesar aquellos IDs considerados desconocidos (-1) con el parámetro procesar_desconocidos en False.

    Notes
    ---------
    
    + Agrega o actualiza la siguiente columna: 
        {columna_id}_Actualizado : Referencia que el ID fue modificado.
    + Modifica las columna:
        {columna_id}_Actualizado : Almacena los valores 1 y 0. 
        Error_{columna_id} : Se almacena el valor original que se encuentra duplicado.
    
    + Tener en cuenta:
        Los IDs que representan valores desconocidos si se procesan se darán nuevos valores a todos los casos a excepción del primero.
        
    Parameters
    ---------
    df : DataFrame
        DataFrame a utilizar
    columna_id : str
        Columna que contiene los valores duplicados
    procesar_desconocidos : bool
        Si es True le dará un nuevo IDs a todos los duplicados al ID -1 (a excepción del primero).
        Si es False se filtran los IDs desconocidos. 
        Por defecto su valor es False.
    '''
    if df is None:
        raise ValueError('ERROR ::: Se esperaba el que el parámetro df fuera un DataFrame y no un NoneType.')
    
    if not isinstance(df, DataFrame):
        raise TypeError('ERROR ::: Se esperaba que el parámetro df fuera un DataFrame.')
    
    if not isinstance(columna_id, str):
        raise TypeError('ERROR ::: Se esperaba que el parámetro columna_id fuera una cadena.')
    
    if columna_id not in df.columns:
        raise ValueError('ERROR ::: Se esperaba que la columna {columna_id} existiera en el DataFrame.')

    # Defino las columnas que serán utilizadas
    col_actualizada = f'{columna_id}_Actualizado'
    col_error = f'Error_{columna_id}'

    if col_actualizada not in df.columns:
        df[col_actualizada] = 0

    if col_error not in df.columns:
        df[col_error] = 'Desconocido'

    repetidos = df[columna_id].loc[(df[columna_id] != -1) & df[columna_id].duplicated(keep='first')] \
                    if not procesar_desconocidos \
                    else df[columna_id].loc[df[columna_id].duplicated(keep='first')]
    
    if not repetidos.empty:
        # Almaceno previamente el valor original
        df.loc[repetidos.index, col_error] = repetidos
        df.loc[repetidos.index, columna_id] = df[columna_id].apply(
            lambda x : actualizar_id_duplicado_serie(serie=repetidos, error=-1, rnd=5000)
        )
        df.loc[repetidos.index, col_actualizada] = 1
    
        try:
            df[columna_id] = df[columna_id].astype(int)
        except: 
            pass
    return df


# Matias Alvarez - Data Engineer
