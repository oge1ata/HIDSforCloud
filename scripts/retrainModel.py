import pandas as pd
import numpy as np
import os
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from imblearn.over_sampling import SMOTE
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.ensemble import RandomForestClassifier
from keras.models import Sequential
from keras.layers import Dense
from keras.optimizers import Adam

# Load custom preprocessing and feature selection functions
# from data import preprocess_data as custom_preprocess_data
# from feature_selection import tree_based_selection

def load_data(directory):
    """Load data from all CSV files in the specified directory."""
    df_main = pd.DataFrame()
    chunksize = 50000  # Adjust the chunk size as needed
    for dirname, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirname, filename)
            if file_path.endswith('.csv'):
                print(f"Loading file: {file_path}")
                for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
                    df_main = pd.concat([df_main, chunk], ignore_index=True)
    return df_main

def clean_data(df):
    """Clean the dataset by dropping duplicates, handling NaN values, and converting labels."""
    print("Columns before cleaning:", df.columns)
    
    df.columns = df.columns.str.strip()
    print("Columns after stripping spaces:", df.columns)
    
    if 'Label' in df.columns:
        df['Label'] = df['Label'].apply(lambda x: 0 if isinstance(x, str) and x.lower().startswith("benign") else 1)
    else:
        raise KeyError("The 'Label' column is missing from the dataset.")
    
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    
    return df

def custom_preprocess_data(df):
    """Preprocess the dataset by converting non-numeric data, normalizing numeric data, and splitting."""
    print("Initial columns:", df.columns)
    
    df.columns = df.columns.str.strip()
    
    if 'Timestamp' in df.columns:
        print("Dropping Timestamp column...")
        df.drop(['Timestamp'], axis=1, inplace=True)
    else:
        print("Timestamp column not found. Proceeding without dropping.")
    
    print("Converting object types to numeric...")
    df_numeric = convert_to_numeric(df.copy())
    print("Columns after conversion to numeric:", df_numeric.columns)
    
    if 'Protocol' in df_numeric.columns:
        print("One-hot encoding Protocol column...")
        df_numeric = pd.get_dummies(df_numeric, columns=['Protocol'])
        print("Columns after one-hot encoding:", df_numeric.columns)
    else:
        print("Protocol column not found. Skipping one-hot encoding.")
    
    protocol_cols = ["Protocol_0", "Protocol_17", "Protocol_6"]
    print("Converting Protocol columns to int64...")
    for col in protocol_cols:
        if col in df_numeric.columns:
            df_numeric[col] = df_numeric[col].astype('int64')
    
    if 'Dst Port' in df_numeric.columns:
        print("Converting Dst Port column to numeric...")
        df_numeric['Dst Port'] = pd.to_numeric(df_numeric['Dst Port'], errors='coerce')
        print("Handling NaNs in Dst Port column...")
        df_numeric['Dst Port'].fillna(0, inplace=True)
        df_numeric['Dst Port'] = df_numeric['Dst Port'].astype('int64')
    
    print("Handling infinite and very large values...")
    df_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_numeric.fillna(df_numeric.mean(), inplace=True)
    
    print("Normalizing numeric columns...")
    numeric_cols = df_numeric.columns.difference(['Label'])
    
    print("Numeric columns being normalized:", numeric_cols)
    print("Shape of numeric data before normalization:", df_numeric[numeric_cols].shape)
    
    scaler = MinMaxScaler()
    df_numeric[numeric_cols] = scaler.fit_transform(df_numeric[numeric_cols])
    
    if 'Label' in df_numeric.columns:
        df_numeric.insert(len(df_numeric.columns) - 1, 'Label', df_numeric.pop('Label'))
    
    print("Preprocessing complete. Final columns:", df_numeric.columns)
    return df_numeric

def tree_based_selection(df, target_col='Label', num_features=20):
    """
    Perform feature selection using a tree-based method (e.g., Random Forest).
    Args:
        df (pd.DataFrame): The dataset including features and target.
        target_col (str): The name of the target column.
        num_features (int): The number of top features to select.
    Returns:
        pd.DataFrame: Dataset with selected features.
    """
    # Split the data into features and target
    X = df.drop(columns=[target_col])
    y = df[target_col]
    
    # Initialize the Random Forest model
    rf = RandomForestClassifier(n_estimators=100, random_state=42)
    
    # Fit the model
    rf.fit(X, y)
    
    # Get feature importances
    importances = rf.feature_importances_
    
    # Create a DataFrame for feature importances
    feature_importances = pd.DataFrame({'Feature': X.columns, 'Importance': importances})
    
    # Sort the features by importance
    feature_importances = feature_importances.sort_values(by='Importance', ascending=False)
    
    # Select the top features
    top_features = feature_importances['Feature'].head(num_features).tolist()
    
    # Return the dataset with only the top features and the target column
    return df[top_features + [target_col]]


def convert_to_numeric(df):
    """Convert object-type features (except 'Label') to numeric."""
    numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Label'])
    for col in numeric_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except:
            pass
    return df

def build_ann_model(input_dim):
    """Build and compile an ANN model using Keras."""
    model = Sequential()
    model.add(Dense(64, input_dim=input_dim, activation='relu'))
    model.add(Dense(32, activation='relu'))
    model.add(Dense(16, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))
    
    model.compile(optimizer=Adam(learning_rate=0.001), loss='binary_crossentropy', metrics=['accuracy'])
    return model

def retrain_model(existing_model_path, new_data_directory, updated_model_path):
    """Retrain an existing model with new data and save the updated model."""
    # Load the existing model (if necessary, otherwise just build a new ANN)
    print("Building a new ANN model...")
    ann_model = build_ann_model(input_dim=20)  # Adjust input_dim based on your data
    
    # Load and preprocess the new data
    print("Loading new data...")
    df_new = load_data(new_data_directory)
    print("New data loaded. Shape:", df_new.shape)
    
    print("Cleaning new data...")
    df_new = clean_data(df_new)
    print("New data cleaned. Shape:", df_new.shape)
    
    print("Preprocessing new data...")
    df_new = custom_preprocess_data(df_new)
    
    # Feature selection
    print("Selecting features using tree-based method...")
    df_new = tree_based_selection(df_new)
    
    # Prepare features and labels
    feature_columns = [col for col in df_new.columns if col != 'Label']
    X_new = df_new[feature_columns]
    y_new = df_new['Label']
    
    # Split the new data into training and test sets
    X_train_new, X_test_new, y_train_new, y_test_new = train_test_split(X_new, y_new, test_size=0.2, random_state=42, stratify=y_new)
    
    # Balance the training set using SMOTE
    print("Balancing the new training set using SMOTE...")
    smote = SMOTE(random_state=42)
    X_train_new, y_train_new = smote.fit_resample(X_train_new, y_train_new)

    # Retrain the model with the new data
    print("Retraining the model with new data...")
    ann_model.fit(X_train_new, y_train_new, epochs=10, batch_size=32, validation_split=0.2)
    
    # Evaluate the updated model
    print("Evaluating the updated model...")
    y_pred_new = (ann_model.predict(X_test_new) > 0.5).astype("int32")
    accuracy = accuracy_score(y_test_new, y_pred_new)
    precision = precision_score(y_test_new, y_pred_new)
    recall = recall_score(y_test_new, y_pred_new)
    f1 = f1_score(y_test_new, y_pred_new)
    
    print(f"Updated model evaluated. Accuracy: {accuracy:.2f}, Precision: {precision:.2f}, Recall: {recall:.2f}, F1 Score: {f1:.2f}")
    
    # Save the updated model
    print(f"Saving the updated model to {updated_model_path}...")
    ann_model.save(updated_model_path)
    print("Updated model saved successfully.")

if __name__ == "__main__":
    existing_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_random_forest_model.pkl'
    new_data_directory = 'C://Users//Admin//Documents//Final//HIDSProject//data//cicids2017'
    updated_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_updated_ann_model.h5'
    retrain_model(existing_model_path, new_data_directory, updated_model_path)
