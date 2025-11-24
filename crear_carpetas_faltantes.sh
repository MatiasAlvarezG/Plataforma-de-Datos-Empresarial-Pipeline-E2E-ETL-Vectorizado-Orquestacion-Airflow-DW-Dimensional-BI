#!/bin/bash
# crear_carpetas_faltantes.sh

# Script para crear carpetas que pueden faltar.
# Si bien se utiliza la estrategía de creación de .gitkeep, ejecute
# este script para crear cualquier carpeta faltante.
# Las carpetas a crear son:
# ./data_services/{logs,plugins} & ./data/SemiProcesados

LISTA=("data_services/airflow/logs"
       "data_services/airflow/plugins" 
       "data/SemiProcesados")
DIR_PROYECTO=$(dirname "$(realpath "$0")")

echo "INFO ::: Iniciando proceso..." && sleep 0.3

for RUTA in ${LISTA[@]}; do
  RUTA_MKDIR="${DIR_PROYECTO}/${RUTA}"
  echo "  Verificando: ${RUTA_MKDIR}"
  mkdir -p "${RUTA_MKDIR}"
  if [[ $? -eq 0 ]]; then
    echo "  [OK]"
  else
    echo "  [FAIL]"
  fi
done

echo "INFO ::: Proceso finalizado!"
