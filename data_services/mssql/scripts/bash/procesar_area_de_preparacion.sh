#!/bin/bash
# procesar_area_de_preparacion.sh

MSG="correctamente el procesamiento del área de preparación"

echo "Ejecutando scripts SQL..."
echo "Tranformandos los datos del área de preparación..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i /home/scripts/5_Transformacion_Datos_tablas_temporales.sql

if [[ $? -eq 0 ]]; then
  echo "INFO ::: Se procesó $MSG"
  exit 0
else
  echo "ERROR ::: No se procesó $MSG"
  exit 1
fi
