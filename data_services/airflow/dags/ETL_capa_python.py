from airflow.decorators import task, dag
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor, ExternalTaskMarker

from ErrorTypes.ErrorTypes import EventFailedError
from utils import utils 


@dag(dag_id='procesamiento_y_carga_all_dataset', default_args=utils.default_args, description='ETL Inicial para todos los dataset.', schedule=None, catchup=False, tags=['ETL', 'PYTHON'])
def procesamiento_y_carga_all_dataset():
    '''
    DAG especializado en procesar todos los dataset y cargandolos al área de preparación de MSSQL.
    Si la base de datos o las tablas no estan creadas se manda un mensaje a MSSQL para que las genere.
    '''

    @task.branch(show_return_value_in_logs=True)
    def _check_db() -> str:
        '''
        Método que verifica que existe:
        1) La base de datos "proyecto_matias_alvarez"
        2) Todas las tablas necesarias.
        En caso de fallo de algunas de las opciones retorna el task-id: create_db_and_tables
        En caso de éxito retorna el task-id: load_tables
        '''

        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
        from constants import TABLES_TMP

        DATABASE = utils.__obtener_base_de_datos('DATABASE_SQL')

        ## Se crean las consultas de verificación
        # Consulta para verificar que exista la DB
        QUERY_DB = f"SELECT name FROM master.dbo.sysdatabases WHERE name = '{DATABASE}'"

        # Consulta para verificar que existan las tablas
        QUERY_TABLAS = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='{DATABASE}' AND TABLE_NAME LIKE 'tmp_%';"

        try:
            # Se realiza la conexion para verificar la existencia de la base de datos. 
            # Recordar que todas las conexiones fueron definidas en el Dockerfile
            # Se utiliza un usuario con permisos minimos de visualizacion de nombre de bases de datos 
            #  para asegurar que si la bd del proyecto no existe se puede consultar igual.
            hook_verificar_db = MsSqlHook(mssql_conn_id='MSSQL_CONN_VER')

            RESPUESTA_DB = hook_verificar_db.get_records(QUERY_DB)

            if RESPUESTA_DB is None or (isinstance(RESPUESTA_DB, list) and len(RESPUESTA_DB) == 0):
                print('INFO ::: La base de datos no fue creada!')
                return '_crear_bd_y_tablas' #'create_db_and_tables'

            # Se realiza la conexión para la base de datos del proyecto.
            hook = MsSqlHook(mssql_conn_id='MSSQL_CONN')
   
            # Si existe la DB se verifica que existan las tablas. Ahora se u
            RESPUESTA_TABLAS = hook.get_records(QUERY_TABLAS)
            TABLAS_CREADAS = {table[0].lower() for table in RESPUESTA_TABLAS} 
            
            return 'load_tables' if all(table.lower() in TABLAS_CREADAS for table in TABLES_TMP) else  'create_db_and_tables'
        except Exception as e:
            raise EventFailedError(f'ERROR ::: No se pudo procesar la solicitud: {type(e)} | {e}')


    @task(show_return_value_in_logs=True)
    def _crear_bd_y_tablas():
        from time import sleep
        RESPUESTA = utils._enviar_mensaje_mssql(mensaje='crear_db')
        if RESPUESTA not in ('ALREADY DONE', 'OK'):
            raise EventFailedError(f'ERROR ::: No se pudo crear la Base de datos!. Respuesta: {RESPUESTA}')

        sleep(30)
        return 'load_tables'

    ######
    # Tareas
    ######

    inicio_procesamiento =  EmptyOperator(
        task_id='inicio_procesamiento'
    )


    fin_procesamiento =  EmptyOperator(
        task_id='fin_procesamiento',
        trigger_rule='all_success'
    )


    fin_carga_a_db =  EmptyOperator(
        task_id='fin_carga_a_db',
        trigger_rule='all_done'
    )

    # Tarea para marcar que todos las tareas se ejecutaron correctamente

    mark_success_task = ExternalTaskMarker(
        task_id='mark_success_task',
        external_dag_id='procesamiento_area_preparacion_y_carga_modelo_dimensional',  
        external_task_id='wait_for_etl',
        execution_date='{{ execution_date }}'  
    )

    # Ruta del proyecto
    RUTA_PROJ = '/home/proyecto_matias_alvarez'
    BASH_PROCESAMIENTO = f'cd {RUTA_PROJ} && python3 src/transform' 

    # Se procesa tipo_producto
    process_product_type_task = BashOperator(
        task_id='process_product_type_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessProductType.py'
    )


    #Se procesa producto
    process_product_task = BashOperator(
        task_id='process_product_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessProduct.py'
    )


    # Se procesa Proveedores
    process_supplier_task = BashOperator(
        task_id='process_supplier_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessSupplier.py'
    )


    # Se procesa compra
    process_purchase_task = BashOperator(
        task_id='process_purchase_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessPurchases.py'
    )


    # Se procesa sucursales
    process_branches_task = BashOperator(
        task_id='process_branches_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessBranches.py'
    )


    # Se procesa tipo gasto
    process_expense_type_task = BashOperator(
        task_id='process_expense_type_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessExpenseType.py'
    )


    # Se procesa gasto
    process_expenses_task = BashOperator(
        task_id='process_expenses_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessExpenses.py'
    )


    # Se procesa canal de venta
    process_sales_channel_task = BashOperator(
        task_id='process_sales_channel_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessSalesChannel.py'
    )


    # Se procesa clientes
    process_customers_task = BashOperator(
        task_id='process_customers_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessCustomers.py'
    )


    # Se procesa empleados
    process_employees_task = BashOperator(
        task_id='process_employees_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessEmployees.py'
    )


    # Se procesa ventas
    process_sales_task = BashOperator(
        task_id='process_sales_task',
        bash_command=f'{BASH_PROCESAMIENTO}/ProcessSales.py'
    )


    ####
    # Chequeo de la base de datos
    ####
    # Se verifica que existan la base de datos y tablas antes de proceder
    # En caso que no exista la DB se crea todo lo necesario
    # En caso que exista se inicia el proceso de carga

    verificar_db = _check_db()

    # Se crea la base de datos, tablas, funciones y procedimientos, triggers, etc
    create_db_and_tables = _crear_bd_y_tablas()

    # Se define la tarea para iniciar la carga de las tablas
    load_tables = EmptyOperator(
        task_id='load_tables',
        trigger_rule='one_success'
    )


    ####
    # Cargar datos a sus correspondiente tablas
    ####
    RUTA_SCRIPT = 'src/transform/utils/load_db_tmp.py' # Recordar que load_db_tmp recibe dos argumentos: "nombre_tabla_tmp" "ubicacion_archivo"
    RUTA_DATA = f'{RUTA_PROJ}/data/SemiProcesados'
    BASH_CARGA = f'cd {RUTA_PROJ} && python3 {RUTA_SCRIPT}'

    # Se carga tipo_producto
    load_product_type_task = BashOperator(
        task_id='load_product_type_task',
        bash_command=f'{BASH_CARGA} tmp_tipo_producto {RUTA_DATA}/tipo_producto.csv'
    )


    # Se carga producto
    load_product_task = BashOperator(
        task_id='load_product_task',
        bash_command=f'{BASH_CARGA} tmp_producto {RUTA_DATA}/producto.csv'
    )


    load_supplier_task = BashOperator(
        task_id='load_supplier_task',
        bash_command=f'{BASH_CARGA} tmp_proveedor {RUTA_DATA}/Proveedores.csv'
    )


    load_purchase_task = BashOperator(
        task_id='load_purchase_task',
        bash_command=f'{BASH_CARGA} tmp_compra {RUTA_DATA}/Compra.csv'
    )


    load_branches_task = BashOperator(
        task_id='load_branches_task',
        bash_command=f'{BASH_CARGA} tmp_sucursal {RUTA_DATA}/Sucursales.csv'
    )


    load_expense_type_task = BashOperator(
        task_id='load_expense_type_task',
        bash_command=f'{BASH_CARGA} tmp_tipo_gasto {RUTA_DATA}/TiposDeGasto.csv'
    )


    load_expenses_task = BashOperator(
        task_id='load_expenses_task',
        bash_command=f'{BASH_CARGA} tmp_gasto {RUTA_DATA}/Gasto.csv'
    )


    load_sales_channel_task = BashOperator(
        task_id='load_sales_channel_task',
        bash_command=f'{BASH_CARGA} tmp_canal_venta {RUTA_DATA}/CanalDeVenta.csv'
    )


    load_customers_task = BashOperator(
        task_id='load_customers_task',
        bash_command=f'{BASH_CARGA} tmp_cliente {RUTA_DATA}/Clientes.csv'
    )


    load_employees_task = BashOperator(
        task_id='load_employees_task',
        bash_command=f'{BASH_CARGA} tmp_empleado {RUTA_DATA}/Empleado.csv'
    )


    load_sales_task = BashOperator(
        task_id='load_sales_task',
        bash_command=f'{BASH_CARGA} tmp_venta {RUTA_DATA}/Ventas.csv'
    )


    ####
    ## Se crean las dependencias de las tareas
    ###
    # Primero se procesa tipo producto para asi procesar producto
    inicio_procesamiento >> process_product_type_task >> process_product_task

    # Luego de process_product_task se procesan las demas tareas en paralelo
    process_product_task >> [
        process_supplier_task, process_purchase_task,
        process_branches_task, process_expense_type_task, process_expenses_task,
        process_sales_channel_task, process_customers_task, process_employees_task,
        process_sales_task] >> fin_procesamiento


    fin_procesamiento >> verificar_db >> [create_db_and_tables, load_tables]
    create_db_and_tables >> load_tables 

    # Se carga a la base de datos
    load_tables >> [load_product_task,
                    load_product_type_task, load_supplier_task, load_purchase_task,
                    load_branches_task, load_expense_type_task, load_expenses_task,
                    load_sales_channel_task, load_customers_task,
                    load_employees_task, load_sales_task]


    # Se marca que se completaron todas las tareas
    [load_product_task,
    load_product_type_task,
    load_supplier_task,
    load_purchase_task,
    load_branches_task,
    load_expense_type_task,
    load_expenses_task,
    load_sales_channel_task,
    load_customers_task,
    load_employees_task,
    load_sales_task] >> fin_carga_a_db >> mark_success_task

procesamiento_y_carga_all_dataset()

