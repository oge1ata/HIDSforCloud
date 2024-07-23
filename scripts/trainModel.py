import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import os

def load_data(directory):
    df_main = pd.DataFrame()
    chunksize = 50000
    for dirname, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirname, filename)
            if file_path.endswith('.csv'):
                print(f"Loading file: {file_path}")
                for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
                    df_main = pd.concat([df_main, chunk], ignore_index=True)
    return df_main

def clean_data(df):
    df.columns = df.columns.str.strip()
    if 'Label' in df.columns:
        df['Label'] = df['Label'].apply(lambda x: 0 if isinstance(x, str) and x.lower().startswith("benign") else 1)
    else:
        raise KeyError("The 'Label' column is missing from the dataset.")
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    return df

def preprocess_data(df):
    df.columns = df.columns.str.strip()
    if 'Timestamp' in df.columns:
        df.drop(['Timestamp'], axis=1, inplace=True)
    df_numeric = convert_to_numeric(df.copy())
    if 'Protocol' in df_numeric.columns:
        df_numeric = pd.get_dummies(df_numeric, columns=['Protocol'])
    protocol_cols = ["Protocol_0", "Protocol_17", "Protocol_6"]
    for col in protocol_cols:
        if col in df_numeric.columns:
            df_numeric[col] = df_numeric[col].astype('int64')
    if 'Dst Port' in df_numeric.columns:
        df_numeric['Dst Port'] = pd.to_numeric(df_numeric['Dst Port'], errors='coerce')
        df_numeric['Dst Port'].fillna(0, inplace=True)
        df_numeric['Dst Port'] = df_numeric['Dst Port'].astype('int64')
    df_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_numeric.fillna(df_numeric.mean(), inplace=True)
    numeric_cols = df_numeric.columns.difference(['Label'])
    scaler = MinMaxScaler()
    df_numeric[numeric_cols] = scaler.fit_transform(df_numeric[numeric_cols])
    if 'Label' in df_numeric.columns:
        df_numeric.insert(len(df_numeric.columns) - 1, 'Label', df_numeric.pop('Label'))
    return df_numeric

def convert_to_numeric(df):
    numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Label'])
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def train_model(directory, model_path):
    df = load_data(directory)
    df = clean_data(df)
    df = preprocess_data(df)
    feature_columns = [col for col in df.columns if col != 'Label']
    X = df[feature_columns]
    y = df['Label']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    smote = SMOTE(random_state=42)
    X_train, y_train = smote.fit_resample(X_train, y_train)
    model = RandomForestClassifier(random_state=42)
    
    # Hyperparameter tuning (optional)
    from sklearn.model_selection import GridSearchCV
    param_grid = {
        'n_estimators': [100, 200],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    grid_search = GridSearchCV(model, param_grid, cv=5, n_jobs=-1, verbose=2)
    grid_search.fit(X_train, y_train)
    model = grid_search.best_estimator_
    
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    print(f"Model trained and evaluated. Accuracy: {accuracy:.2f}, Precision: {precision:.2f}, Recall: {recall:.2f}, F1 Score: {f1:.2f}")
    print(f"Best hyperparameters: {grid_search.best_params_}")
    joblib.dump(model, model_path)
    print("Model saved successfully.")



if __name__ == "__main__":
    # Example usage
    dataset_directory = 'C://Users//Admin//Documents//Final//HIDSProject//data//cicids2017'
    model_output_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_random_forest_model.pkl'
    train_model(dataset_directory, model_output_path)

    dataset_directory = 'C://Users//Admin//Documents//Final//HIDSProject//data//cicids2018'
    model_output_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//cicids2018_random_forest_model.pkl'
    train_model(dataset_directory, model_output_path)