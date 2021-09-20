import numpy as np
import pandas as pd
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from env import host, password, username

def get_db_url(db_name, username=username, hostname=host, password=password):
    return f'mysql+pymysql://{username}:{password}@{hostname}/{db_name}'

def wrangle_mall():
    """ Acquires and Prepares Codeup's mall_customers database """
    ### Acquire mall_customers database ###
    url = get_db_url(db_name='mall_customers')
    query = """ SELECT * FROM customers """
    df = pd.read_sql(query, url)

    ### Encode ###
    df = pd.get_dummies(df, columns=['gender'], drop_first=True)

    ### Split data ###
    train_validate, test = train_test_split(df, test_size=0.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=0.25, random_state=123)

    ### Isolate target ###
    X_train, y_train = train.drop(columns=['customer_id','spending_score']), train.spending_score
    X_validate, y_validate = validate.drop(columns=['customer_id','spending_score']), validate.spending_score
    X_test, y_test = test.drop(columns=['customer_id','spending_score']), test.spending_score

    ### Scale data ###
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_validate_scaled = scaler.transform(X_validate)
    X_test_scaled = scaler.transform(X_test)

    ### Return everything ###
    return df, X_train, y_train,\
            X_validate, y_validate,\
            X_test, y_test,\
            X_train_scaled, X_validate_scaled, X_test_scaled

