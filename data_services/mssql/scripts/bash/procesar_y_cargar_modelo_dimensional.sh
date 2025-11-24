#!/bin/bash
# procesar_y_cargar_modelo_dimensional.sh

DIR_SCRIPT=$(dirname "$(realpath "$0")")

$DIR_SCRIPT/procesar_area_de_preparacion.sh

if [[ $? -eq 0 ]]; then
  $DIR_SCRIPT/cargar_modelo_dimensional.sh
  if [[ $? -ne 0 ]]; then
    echo "ERROR ::: No se pudo cargar el modelo dimensional basado en el procesamiento actual del área de preparación!"
    exit 1
  fi
else
  echo "ERROR ::: No se pudo procesar el área de preparación! No se puede continuar con la carga del modelo"
  exit 1
fi

exit 0


