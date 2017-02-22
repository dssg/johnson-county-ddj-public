def timeframe_table_start_end_date(table_name, start_date, end_date, 
                start_date_field, end_date_field):
    tframe_query = """SELECT *
                      FROM {table_name_} as t
                      WHERE t.{start_date_column_} >= \'{start_date_}\' and 
                            t.{end_date_column_} < \'{end_date_}\'
                   """.format(table_name_= table_name, 
                              start_date_column_ = start_date_field,
                              start_date_ = start_date,
                              end_date_column_ = end_date_field,
                              end_date_ = end_date)

    return tframe_query

def timeframe_table_end_date(table_name,  end_date, end_date_field):
    tframe_query = """SELECT *
                      FROM {table_name_} as t
                      WHERE t.{end_date_column_} < \'{end_date_}\'
                   """.format(table_name_= table_name,
                              end_date_column_ = end_date_field,
                              end_date_ = end_date)
    return tframe_query
