import pandas as pd
import tensorflow as tf

def prepare_batches(df: pd.DataFrame, dce, num_samples: int, validation_split: float, batch_size: int, random_state: int):
    def prepare_batch_char(input, target):
        input = input.to_tensor()
        target_inputs = target[:, :-1].to_tensor()  # Drop end boundary (\n)
        target_labels = target[:, 1:].to_tensor()   # Drop start boundary (\t)
        return (input, target_inputs), target_labels

    def to_batches(ds):
        return (ds
            .shuffle(BUFFER_SIZE)
            .batch(batch_size)
            .map(prepare_batch_char, tf.data.AUTOTUNE)
            .prefetch(buffer_size=tf.data.AUTOTUNE))
    
    BUFFER_SIZE = 20000

    train_smpl = df.sample(num_samples, random_state=random_state)
    val_smpl = train_smpl.sample(frac=validation_split, random_state=random_state)
    train_smpl = train_smpl.drop(val_smpl.index)

    train_input = train_smpl['input'].tolist()
    train_target = train_smpl['target'].tolist()
    val_input = val_smpl['input'].tolist()
    val_target = val_smpl['target'].tolist()

    # Label encoding
    train_input_ids = dce.to_ids(train_input, insert_markers=True)
    train_target_ids = dce.to_ids(train_target, insert_markers=True)
    val_input_ids = dce.to_ids(val_input, insert_markers=True)
    val_target_ids = dce.to_ids(val_target, insert_markers=True)

    # Construct datasets from tensors
    train_input_tensors = tf.ragged.constant(train_input_ids)
    train_target_tensors = tf.ragged.constant(train_target_ids)
    val_input_tensors = tf.ragged.constant(val_input_ids)
    val_target_tensors = tf.ragged.constant(val_target_ids)
    train_dataset = tf.data.Dataset.from_tensor_slices((train_input_tensors, train_target_tensors))
    val_dataset = tf.data.Dataset.from_tensor_slices((val_input_tensors, val_target_tensors))

    train_batches = to_batches(train_dataset)
    val_batches = to_batches(val_dataset)

    return train_batches, val_batches

