#!/bin/bash
# crear_db_para_eventos.sh

/opt/mssql/bin/sqlservr &
echo "INFO ::: Esperando a que SQL Server esté listo..."
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -Q "SELECT 1" > /dev/null 2>&1; do
  sleep 5
done

DIR_SCRIPT="/home/scripts/comunicacion_entre_servicios/"

ejecutar_script(){
  # ARGs $NOMBRE_SCRIPT

  SCRIPT="${DIR_SCRIPT}$1"

#  $BIN_MSSQL $SCRIPT
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i "$SCRIPT"
  if [[ $? -eq 0 ]]; then
    echo "  [OK]"
  else
    echo "  [ERROR] - $SCRIPT"
    exit 1
  fi
}

echo "INFO ::: Creando base de datos para la comunicación entre eventos..."
ejecutar_script "DB.sql"

echo "INFO ::: Creando la tabla, esquema y usuarios..."
ejecutar_script "DDL_y_user.sql"

exit 0


