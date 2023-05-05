import pandas as pd
import tensorflow as tf
import keras_nlp

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


def construct_model_w_teacher_forcing(num_encoder_layers: int, num_decoder_layers: int, unique_tokens: int, max_seq_length: int, embedding_dim: int, intermediate_dim: int, encoder_heads: int, decoder_heads: int, dropout: float):
    encoder_inputs = tf.keras.Input(shape=(max_seq_length+2,), name='encoder_inputs')
    token_embeddings = tf.keras.layers.Embedding(input_dim=unique_tokens, output_dim=embedding_dim)(encoder_inputs)
    position_embeddings = keras_nlp.layers.PositionEmbedding(sequence_length=max_seq_length+2)(token_embeddings)
    encoder_outputs = token_embeddings + position_embeddings

    # Encoder
    for i in range(num_encoder_layers):
        encoder_outputs = keras_nlp.layers.TransformerEncoder(intermediate_dim=intermediate_dim, num_heads=encoder_heads, dropout=dropout)(inputs=encoder_outputs)

    encoder = tf.keras.Model(encoder_inputs, encoder_outputs)

    decoder_inputs = tf.keras.Input(shape=(None,), dtype='int64', name='decoder_inputs')
    encoded_seq_inputs = tf.keras.Input(shape=(None, embedding_dim), name='decoder_state_inputs')

    token_embeddings = tf.keras.layers.Embedding(input_dim=unique_tokens, output_dim=embedding_dim)(decoder_inputs)
    position_embeddings = keras_nlp.layers.PositionEmbedding(sequence_length=max_seq_length+2)(token_embeddings)
    decoder_outputs = token_embeddings + position_embeddings

    # Decoder
    for i in range(num_decoder_layers):
        decoder_outputs = keras_nlp.layers.TransformerDecoder(intermediate_dim=intermediate_dim, num_heads=decoder_heads, dropout=dropout)(decoder_sequence=decoder_outputs, encoder_sequence=encoded_seq_inputs)

    # decoder_outputs = tf.keras.layers.Dropout(0.25)(decoder_outputs)
    decoder_outputs = tf.keras.layers.Dense(unique_tokens, activation="softmax")(decoder_outputs)


    decoder = tf.keras.Model([decoder_inputs, encoded_seq_inputs], decoder_outputs)
    decoder_outputs = decoder([decoder_inputs, encoder_outputs])

    transformer = tf.keras.Model(
        [encoder_inputs, decoder_inputs],
        decoder_outputs,
        name='transformer_w_tf',
    )

    return transformer
