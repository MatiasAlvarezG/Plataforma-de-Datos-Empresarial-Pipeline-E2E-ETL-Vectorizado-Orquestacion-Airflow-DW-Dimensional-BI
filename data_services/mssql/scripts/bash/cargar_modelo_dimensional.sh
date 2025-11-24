#!/bin/bash
# cargar_modelo_dimensional.sh

MSG="correctamente la carga al modelo dimensional"

echo "INFO ::: Ejecutando scripts SQL..."
echo "INFO ::: Cargando modelo dimensional..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i /home/scripts/6_Carga_a_Modelo_Dimensional.sql 

if [[ $? -eq 0 ]]; then
  echo "INFO ::: Se procesó $MSG"
  exit 0
else
  echo "ERROR ::: No se procesó $MSG"
  exit 1
fi
