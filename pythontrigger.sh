#!/bin/bash
date_variable=$(date -d "1 day ago" '+%d/%b/%Y')
echo $date_variable
file_path=$1
echo $file_path
python3 /home/shwethar99/Desktop/Audit/file_transfer_python.py $file_path $date_variable ","
