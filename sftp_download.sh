#!/bin/bash

echo -n Password:

read -s password

usuario=bioinfoadm
host=sftprelecov.isciii.es

SSHPASS=$password sshpass -e sftp $usuario@$host <<EOF
lcd ./
get -r ./COD-*
exit
EOF
