import timeframe_queries as tframe

#tframe.timeframe_table("table_name", "24May2014", "24May2016", "start_date", "end_date")

import basicqueries as bq

bq.count_vals_column_for_id("table_name", "id_col", "val_cols", ["ems" ,"mh", "jims"])
