# Métodos para aplicar en DataFrame relacionados a Productos.
from pandas import DataFrame
from .StandardizationDictionary import dict_normalizacion_marcas
from .VerificationMethods import comprobar_existencia_columnas


def desnormalizar_df_producto(df : DataFrame, not_found : str = 'Desconocido') -> None:
    '''
    Completa las columnas 'Marca', 'Modelo' y 'Componente' en el DataFrame proporcionado, df, utilizando la información de la columna 'Producto'. 
    Es un requisito esencial proporcionar el DataFrame correspondiente (df_producto) al llamar a esta función.

    Esta función solo operará si las columnas 'Marca', 'Modelo' y 'Componente' ya existen en el DataFrame; de lo contrario, devolverá None.
    La lógica de la función se basa en identificar inicialmente la marca del producto. 
    Una vez que se encuentra la marca, utiliza las partes posteriores de la descripción para completar la información del modelo y las partes anteriores para describir el componente.

    En caso de no encontrar la marca, la función asigna 'Desconocido' a las tres columnas mencionadas. 
    Este enfoque garantiza una información completa y coherente incluso cuando la marca no está disponible.

    Parameters
    -------
    df : DataFrame
        DataFrame Producto que contiene la columna a desnormalizar

    '''
    def desnormalizar_valores_producto(idx_producto, idx_elemento, marca):
        '''
        Metodo encargado en desnormalizar los valores en las columnas correspondientes 
        '''
        # Se asigna la marca
        df.Marca.at[idx_producto] = marca

        descripcion = df.Producto.at[idx_producto].split(' ')
        
        # Se asigna el Modelo
        df.Modelo.at[idx_producto] = " ".join(descripcion[idx_elemento + 1:])

        # Se asigna el Componente
        df.Componente.at[idx_producto] = " ".join(descripcion[:idx_elemento])

        # Se verifican si hay casos donde no se pudo resolver y se aplica 'Desconocido'
        if df.Modelo.at[idx_producto] == '' :
            df.Modelo.at[idx_producto] = not_found
        elif df.Componente.at[idx_producto] == '':
            df.Componente.at[idx_producto] = not_found 

    if df is None:
        raise ValueError('ERROR ::: El parámetro df debe ser un DataFrame y no NoneType')
    
    if not_found is None:
        not_found = 'Desconocido'

    # Se verifica si el DataFrame tiene las columnas necesarias
    columnas_obligatorias = ['Producto', 'Marca', 'Componente', 'Modelo']
    if not comprobar_existencia_columnas(df, columnas_obligatorias):
        raise ValueError(f"ERROR ::: Se aplica el método [obtener_marca] a un DataFrame incorrecto o faltan algunas de las columnas: {', '.join(columnas_obligatorias)}")
    
    # Se obtiene los diccionarios que se utilizaran para desnormalizar las columnas
    marcas = dict_normalizacion_marcas(get_simple=True)
    marcas_varios_nombres = dict_normalizacion_marcas(get_simple=False)

    # Se obtiene todos los productos a iterar
    for idx_producto, producto in enumerate(df['Producto'].str.split(' ')):
        # Se itera por cada producto
        for idx_elemento, elemento in enumerate(producto):
            try:
                # Intentar obtener la marca directamente. La función de la variable ´tipo´ es simplemente ver si cae en la excepción.
                tipo = marcas[elemento]
                desnormalizar_valores_producto(idx_producto, idx_elemento, marca=elemento)
                break  

            except KeyError: #No existe la marca, puede deberse a que sea un nombre mas largo
                try:
                    # Se verifica que no exista en el diccionario con marcas_varios_nombres
                    valores = marcas_varios_nombres[elemento]

                    # Como existe la primera parte del nombre, obtengo la segunda parte
                    siguiente_elemento = producto[idx_elemento + 1] if idx_elemento < len(producto) - 1 else None

                    if siguiente_elemento and siguiente_elemento in valores.get('Posibles', []):
                        # Es continuación del nombre
                        desnormalizar_valores_producto(idx_producto, idx_elemento, marca=f'{elemento} {siguiente_elemento}')
                        break  

                except KeyError: #No se encontro que fuera una marca
                    df['Componente'].at[idx_producto] = not_found
                    df['Marca'].at[idx_producto] = not_found
                    df['Modelo'].at[idx_producto] = not_found
                    continue


























































# Matias Alvarez - Data Engineer