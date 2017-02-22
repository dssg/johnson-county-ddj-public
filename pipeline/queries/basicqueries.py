def full_table_retrieval(table_name):
    query = 'SELECT * FROM ({})'.format(table_name)
    return query

def count_vals_column_for_id(table_name,id_col, value_column, values_to_count):
    query = ('SELECT t.{id_} ').format(id_ = id_col)
    for value in values_to_count:
        case_query = (', ( sum(case when t.{value_column_} = \'{value_}\' then 1 else 0 end)) as {value_}_sum ').format(
                                        value_column_ = value_column, value_ = value )
        query = query + case_query
    from_statement = ('from ({table_name_}) as t ').format(table_name_ = table_name)
    groupby_statement = ('group by t.{id_col_}').format(id_col_ = id_col)
    query = query + from_statement + groupby_statement
    return query

