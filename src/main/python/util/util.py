def get_dictionary(dataframe, key):
    '''

    :param dataframe: any dataframe with that has json format column
    :param key: key for the dictionary
    :return: return python dictionary
    '''
    data_list = map(lambda row: row.asDict(), dataframe.collect())
    final_data = {team[key]: team for team in data_list}

    return final_data
