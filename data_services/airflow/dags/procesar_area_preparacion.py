from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from ErrorTypes.ErrorTypes import EventFailedError
from utils import utils


@task(show_return_value_in_logs=True)
def _procesar():
    from time import sleep

    RESULTADO = utils._enviar_mensaje_mssql('procesar_area_procesamiento')

    if RESULTADO not in ('ALREADY DONE', 'OK'):
        raise EventFailedError(f'ERROR ::: La solicitud de procesamiento y carga falló. El valor de RESULTADO es: {RESULTADO}')

    # Se esperan 100 segundos para asegurarse que se procesaron todas las tablas
    sleep(100)
    return 'end_task'


# Se crea el DAG
with DAG(
    dag_id='procesamiento_area_preparacion',
    default_args=utils.default_args,
    description='Procesamiento del área de preparación de MSSQL. Permite controlar de forma manual.',
    schedule=None,
    catchup=False,
    tags=['PROCESAMIENTO', 'AREA-PREPARACION', 'MSSQL'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    processing_task = _procesar()

    end_task = EmptyOperator(
        task_id='end_task',
        trigger_rule='all_done'
    )

    ###################
    # Ejecución Tareas
    ###################
    start_task >> processing_task >> end_task
    