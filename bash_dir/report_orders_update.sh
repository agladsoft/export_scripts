#!/bin/bash

xls_path="${XL_IDP_PATH_EXPORT}/report_orders/flat_update_report_orders"

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" -or -name "*.XLS*" -or -name "*.xml" \) ! -newermt '60 seconds ago' -print0 | while read -d $'\0' file
do

  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

	mime_type=$(file -b --mime-type "$file")
  echo "'${file} - ${mime_type}'"

	# Will convert csv to json
	python3 ${XL_IDP_ROOT_EXPORT}/scripts/report_orders_update.py "${file}" "${json_path}"

  if [ $? -eq 0 ]
	then
	  mv "${file}" "${done_path}"
	else
	  mv "${file}" "${xls_path}/error_$(basename "${file}")"
	fi

done