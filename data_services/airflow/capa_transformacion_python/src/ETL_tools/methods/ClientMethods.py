# Métodos utilizados solamente en df_clientes
from sys import path, exit
path.append('src')
from os.path import join

from .FileUploadTools import leer_archivo
from transform.utils.constants import DIR_PROCESAR


def obtener_grupo_etario(edad : int, error = 'Desconocido') -> None:
    '''
    Retorna una cadena que representa el grupo etario de una edad entera
    Los posibles valores son:
        'Niñez' : Menores de 12 años.
        'Adolescencia' : Desde los 12 hasta los 17.
        'Juventud' : Desde los 18 hasta 30.
        'Adultez' : Desde los 31 hasta los 60.
        'Vejez' : Mayor de 60.
    '''
    if edad is None:
          return error
    
    if not isinstance(edad,int):
            try:
                edad = int(edad)
            except Exception as e:
                    return error
            
    if edad < 0:
          return error
    
    return 'Niñez' if (0 <= edad < 12) else \
            ( 'Adolescencia' if 12 <= edad < 18 else \
                ('Juventud' if 18 <= edad <= 30 else \
                    ('Adultez' if 30 < edad <= 60 else \
                        ('Vejez' if edad>60 else error) 
                ))) 


def separar_nombre_y_apellidos(PERSONA : list) -> tuple: 
    '''
    Retorna una tupla que contiene como primer elemento el/los nombres de una persona y como segundo elemento su o sus apellido/s
    El método es capaz de interpretar los conectores de los nombres o los apellidos, por ejemplo: 'Alberto De La Fuente' retornara una tupla que
    contiene los valores ('Alberto', 'De La Fuente'). El mismo resultado será retornado para el caso de 'De La Fuente Alberto'.

    El parámetro PERSONA es un split del valor real pasado por parámetro, siguiendo el ejemplo de 'Alberto De La Fuente' para llamar al método
    se debe pasar la siguiente lista: ['Alberto', 'De', 'La', 'Fuente']

    Parameters
    ---------
    PERSONA : list
        Lista que contiene como elementos partes del nombre y apellido

    Returns
    --------
    tuple 
        Tupla de listas que contiene como primer elemento el o los nombres y como segundo elemento el o los apellidos de la persona solicitada.

    Example
    --------
    >>> separar_nombre_y_apellidos(['Matias', 'Alvarez'])
        Retorna: (['Matias'], ['Alvarez'])

    >>> separar_nombre_y_apellidos(['Alvarez', 'Matias'])
        Retorna: (['Matias'], ['Alvarez'])

    >>> separar_nombre_y_apellidos(['De','La','Fuente','Alberto'])
        Retorna: (['Alberto'], ['De La Fuente'])

    >>> separar_nombre_y_apellidos(['Alfredo','Ruis','De', 'Los', 'Andes'])
        Retorna: (['Alfredo'], ['Ruis De Los Andes'])

    >>> separar_nombre_y_apellidos(['Lionel', 'Messi'])
        Retorna: (['Lionel'], ['Messi'])

    >>> separar_nombre_y_apellidos(['Messi', 'Lionel'])
        Retorna: (['Lionel'], ['Messi'])
    '''
    # Método que resuelve casos donde un valor corresponde a un nombre y a un apellido retornando -si puede- cual es el caso correcto
    def resolver_inconsistencia(_addFail = True):
        candidatos_restantes = len(PERSONA) - (len(nombre) + no_contar) #Tamaño de PERSONA - Cantidad de conectores encontrados
        if len(nombre) == MAX_NAME or candidatos_restantes == 1:
            apellido.append(valor)
        elif len(nombre) < MAX_NAME:
            nombre.append(valor)
        else:
            if _addFail:
                no_resuelto.append(valor)
            else:
                print(f"ERROR ::: NO SE PUEDE RESOLVER EL VALOR NO RESUELTO {valor}")
            return None
        
    def concatenar_conectores():
        # Lógica para concatenar conectores como 'De', 'Del', 'Los', 'Las', 'La'
        for conector, idx in concatenados:
            conector_concatenado = len(conector.split(' ')) > 1
            # Calculo el índice correcto 
            idx_correcto = idx if not conector_concatenado else idx + 1

            try:
                # Verifico si el siguiente elemento después del conector esta en apellido 
                if PERSONA[idx_correcto + 1] in apellido: 
                    posModificar = apellido.index(PERSONA[idx_correcto + 1]) # Consigo el índice
                    apellido[posModificar] = (
                        f"{conector} {apellido[posModificar]}"
                        if posModificar + 1 == len(apellido) # Si el apellido es el último en la lista, se añade el conector al final
                        else f"{conector} {apellido[posModificar]} {apellido.pop(posModificar + 1)}" # hay mas apellido entonces añado el conector y se eliminan los apellidos siguientes de la lista
                    )
                elif PERSONA[idx_correcto + 1] in nombre: # Verifico si el siguiente conector esta en la lista de nombres
                    posModificar = nombre.index(PERSONA[idx_correcto + 1]) # Consigo su índice
                    # Actualizo el nombre concatenando el conector y elimina el nombre anterior de la lista (porque si no queda de forma redundante)
                    nombre[posModificar] = f"{nombre[posModificar-1]} {conector} {nombre[posModificar]}"
                    nombre.pop(posModificar - 1)
            except ValueError:
                print(
                    f"INFO ::: NO SE PUEDE RESOLVER EL VALOR CONCATENADO {valor} PORQUE NO EXISTE EN SU RESPECTIVO DOMINIO"
                )

    if PERSONA is None:
        return None
    
    if not isinstance(PERSONA,list):
        raise TypeError('ERROR ::: El parametro PERSONA debe ser una lista. Ejemplo: PERSONA=[\'Alvarez\', \'Matias]\'')
    
    MAX_NAME = 3
    no_contar = 0
    no_resuelto = []
    apellido = []
    nombre = []
    concatenados = [] #almacenará tuplas de la siguiente forma: (conector, indice_correspondiente_en_PERSONA)
                      # Ejemplo : 'Matias Alvarez De Apellido2' concatenados seria : [('De', 2)]

    for idx, valor in enumerate(PERSONA):
        if valor in ['Desconocido', 'nan', 'Nan', 'NaN', 'None', 'NAN', None]:
            continue

        es_nombre = (df_nombres.nombre == valor).any()
        es_apellido = (df_apellidos.apellido == valor).any()

        #Se detecta un conector
        if valor in ['De', 'Del', 'Los', 'Las', 'La', 'El']:
            # si no hay concatenados es que es el primer conector encontrado
            # o si no, verifica si el ultimo conector (su índice) no esta consecutivamente antes del índice actual asegurando 
            # que el conector actual no es continuación del anterior, por lo que es un nuevo caso a resolver.
            # resumen: decide si el conector actual (valor) debe ser considerado como uno nuevo o si debe concatenarse con el conector anterior.
            if not concatenados or concatenados[-1][1] != idx - 1:
                concatenados.append([f"{valor}", idx])
            else:
                # Si ya hay un conector almacenado en concatenados y el conector actual está consecutivamente después del anterior, 
                # se actualiza el último conector concatenando el conector actual
                concatenados[-1][0] = f"{concatenados[-1][0]} {valor}"
            no_contar += 1

        # Se detecta un nombre
        elif es_nombre and not es_apellido:
            nombre.append(valor)

        # Se detecta un apellido
        elif not es_nombre and es_apellido:
            apellido.append(valor)

        # Se detecta que puede ser un nombre o un apellido
        elif es_nombre and es_apellido:
            resolver_inconsistencia()

        else: # No existe existencia del valor como nombre ni como apellido, se dejará para el final
            no_resuelto.append(valor)

    # Se resuelven los casos no resueltos
    for idx, valor in enumerate(no_resuelto):
        resolver_inconsistencia(_addFail=False)

    # Agrego los conectores 'De', 'Del', 'Los', 'Las', 'La' según corresponda
    concatenar_conectores()

    # Si sigue habiendo inconsistencia -No hay nombres- o -No hay apellidos- entonces:
    # Si no existe apellidos y hay mas de un nombre entonces el ultimo elemento se tomara como apellido
    apellido.append(nombre.pop()) if not apellido and len(nombre) > 1 else None
    
    # Caso contrario, no existe nombre pero hay mas de un apellido entonces se almacena el primer elemento como nombre
    nombre.append(apellido.pop(0)) if not nombre and len(apellido) > 1 else None

    return nombre, apellido

dir_nombre = join(DIR_PROCESAR, 'Nombres.csv')
dir_apellidos = join(DIR_PROCESAR, 'Apellidos.csv')
                        
try:
    df_nombres = leer_archivo(dir_nombre, sep=',')
    df_apellidos = leer_archivo(dir_apellidos, sep=',')
except Exception as e:
    print(f'ERROR ::: Hubo un problema al cargar los DataFrame nombres y apellidos\n{e}')
    exit(1)





















































# Matías Alvarez - Data Engineer
