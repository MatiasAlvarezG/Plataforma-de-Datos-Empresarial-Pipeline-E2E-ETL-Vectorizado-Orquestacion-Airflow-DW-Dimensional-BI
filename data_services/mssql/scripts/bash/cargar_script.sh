#!/bin/bash
# cargar_script.sh
/opt/mssql/bin/sqlservr &

echo "Esperando a que SQL Server esté listo..."
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -Q "SELECT 1" > /dev/null 2>&1; do
  sleep 5
done

DIR_SCRIPT="/home/scripts/"
#BIN_MSSQL="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i "

ejecutar_script(){
  # ARGs $NOMBRE_SCRIPT

  SCRIPT="${DIR_SCRIPT}$1"
  
  # $BIN_MSSQL $SCRIPT
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Sqlserver2024)' -C -i "$SCRIPT"

  if [[ $? -eq 0 ]]; then
    echo "  [OK]"
  else
    echo "  [ERROR] - $SCRIPT"
    exit 1
  fi
}

echo "INFO ::: Creando el almacen de datos..."
ejecutar_script "0_DB.sql"

echo "INFO ::: Creando las tablas del área de preparación y modelo dimensional..."
ejecutar_script "1_DDL.sql"

echo "INFO ::: Creando los usuarios necesarios..."
ejecutar_script "2_Usuarios_y_Roles.sql"

echo "INFO ::: Creando las utilidades (funciones y procedimientos almacenados)..."
ejecutar_script "3_Funciones_y_Procedimientos.sql"

echo "INFO ::: Creando los triggers..."
ejecutar_script "4_Triggers.sql"

exit 0

