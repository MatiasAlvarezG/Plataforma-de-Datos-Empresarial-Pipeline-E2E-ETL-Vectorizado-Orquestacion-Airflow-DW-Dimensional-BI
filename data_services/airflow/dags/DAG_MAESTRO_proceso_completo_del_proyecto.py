from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils import utils

with DAG(
    dag_id='proceso_completo_del_proyecto',
    default_args=utils.default_args,
    description='DAG Maestro especifico para ejecutar todos los DAGs realizado. Permite automatizar todo el proceso del proyecto en un único DAG.',
    schedule=None,
    catchup=False,
    tags=['PROCESO_COMPLETO', 'PROCESAMIENTO', 'PYTHON', 'MSSQL', 'CARGA_TBL_TEMPORALES', 'CARGA_MODELO_DIMENSIONAL'],
) as dag:
    
    start_task_maestro = EmptyOperator(
        task_id='start_task_maestro'
    )


    end_task_maestro = EmptyOperator(
        task_id='end_task_maestro',
        trigger_rule='all_done'
    )

    # Se define una lista que contiene como elementos diccionarios con el dag_id y task_id
    # De esta manera se puede procesar los dags.
    orden_de_dags = [
        {'dag_id': 'procesamiento_y_carga_all_dataset', 'task_id': 'inicio_procesamiento'},
        {'dag_id': 'procesamiento_area_preparacion_y_carga_modelo_dimensional', 'task_id': 'start_task'}
    ]

    # Puntero para iniciarlizar la primera tarea
    procesar_tarea = start_task_maestro
    for dag_a_procesar in orden_de_dags:
            trigger_task = TriggerDagRunOperator(
                task_id=dag_a_procesar['task_id'],
                trigger_dag_id=dag_a_procesar['dag_id'],
                wait_for_completion=True,  
                execution_date='{{ ds }}',
                reset_dag_run=True,
                poke_interval=30,
            )
            procesar_tarea >> trigger_task
            procesar_tarea = trigger_task

    procesar_tarea >> end_task_maestro 

