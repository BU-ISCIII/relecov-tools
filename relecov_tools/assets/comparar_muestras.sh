#!/bin/bash

# Verificar si se proporcionaron dos argumentos
if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <fichero1> <fichero2>"
    exit 1
fi

# Asignar los argumentos a variables
fichero1="$1"
fichero2="$2"

# Verificar si los ficheros existen
if [ ! -f "$fichero1" ] || [ ! -f "$fichero2" ]; then
    echo "Ambos ficheros deben existir."
    exit 1
fi

# Buscar duplicados en el segundo fichero
repetidas_en_segundo=$(sort "$fichero2" | uniq -d)

if [ -n "$repetidas_en_segundo" ]; then
    echo "El segundo fichero tiene muestras repetidas. Estas muestras son:"
    echo "$repetidas_en_segundo"
    exit 0
fi

# Buscar muestras repetidas entre los dos ficheros
repetidas=$(grep -Fxf "$fichero1" "$fichero2")

if [ -n "$repetidas" ]; then
    # Si hay muestras repetidas entre los dos ficheros
    echo "El segundo fichero tiene muestras que ya están en el primero. Estas muestras son:"
    echo "$repetidas"
else
    # Si todas las muestras son nuevas
    echo "Todas las muestras han sido añadidas al primer fichero."
    cat "$fichero2" >> "$fichero1"
fi
