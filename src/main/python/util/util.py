def get_dictionary(dataframe, key):
    data_list = map(lambda row: row.asDict(), dataframe.collect())
    final_data = {team[key]: team for team in data_list}

    return final_data
