import tensorflow as tf
from tensorflow.keras.models import load_model

# Load the SavedModel
saved_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//saved_model.pb'
model = tf.saved_model.load(saved_model_path)

# Save the model in HDF5 format
tf.keras.models.save_model(model, 'C://Users//Admin//Documents//Final//HIDSProject//models//saved_model.h5', save_format='h5')

def load_keras_model(model_path):
    """Load a Keras model saved in HDF5 format."""
    try:
        model = load_model(model_path)
        print("Model loaded successfully.")
        return model
    except Exception as e:
        print(f"Error loading the model: {e}")
        return None

if __name__ == "__main__":
    model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_updated_ann_model.h5'
    model = load_keras_model(model_path)
    
    # Use 'model' for further operations or testing
