#!/bin/bash
# crear_usuario_verificacion.sh

MSG="crear el usuario de verificación!"
/opt/mssql/bin/sqlservr &

echo "INFO ::: Esperando a que SQL Server esté listo..."
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -Q "SELECT 1" > /dev/null 2>&1; do
  sleep 5
done

echo "INFO :::Creando usuario de verificacion de bases de datos creadas..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i /home/scripts/usuario_general_para_consultar_db/usuario_general.sql 

if [[ $? -ne 0 ]]; then
  echo "ERROR ::: No se pudo $MSG"
  exit 1
fi

echo "INFO ::: Se pudo $MSG"
exit 0
