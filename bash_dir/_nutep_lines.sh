while true;
do
	${XL_IDP_ROOT_EXPORT}/bash_dir/flat_export.sh;
	${XL_IDP_ROOT_EXPORT}/bash_dir/export_grain.sh;
	${XL_IDP_ROOT_EXPORT}/bash_dir/report_order.sh;
	${XL_IDP_ROOT_EXPORT}/bash_dir/report_orders_update.sh;
	sleep 1;
done