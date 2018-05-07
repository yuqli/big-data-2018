
def Join_2_datasets(set1, set2, col):
    """
    Given two datasets and a column, return the outer joined dataset.
    """
    if set1 == 'ad_feature':
        if set2 == 'user_profile':
            return None
        if set2 == 'behavior_log':
            return spark.sql("SELECT * FROM {0} one FULL OUTER JOIN {1} two ON one.{2}=two.{3}".format(set1, set2, col1, col2)).toJSON().collect()
    if set1 == 'user_profile':
        if set2 == 'ad_feature':
            return None
        if set2 == 'behavior_log':
            return spark.sql("SELECT * FROM {0} one FULL OUTER JOIN {1} two ON one.{2}=two.{3}".format(set1, set2, col1, col2)).toJSON().collect()

    if set1 == 'behavior_log':
        return spark.sql("SELECT * FROM {0} one FULL OUTER JOIN {1} two ON one.{2}=two.{3}".format(set1, set2, col1, col2)).toJSON().collect()
