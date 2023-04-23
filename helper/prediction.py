import numpy as np

from tensorflow import keras
from .character_encoder import DictionaryCharacterEncoder


def predict_sequence(dce: DictionaryCharacterEncoder, encoder_model: keras.Model, decoder_model: keras.Model, input_str: str):
    """_summary_

    Arguments:
        dce -- _Instance of the DictionaryCharacterEncoder used for decoding._
        encoder_model -- _Keras encoder model._
        decoder_model -- _Keras decoder model._
        input_seq -- _description_

    Returns:
        _The decoded output sequence._
    """
    input_seq = dce.encode([input_str])

    # Encode the input as state vectors.
    states_value = encoder_model.predict(input_seq, verbose=0)

    # Generate empty target sequence of length 1.
    target_seq = np.zeros((1, 1, len(dce.charset)))
    # Populate the first character of target sequence with the start character.
    target_seq[0, 0, dce.char_index['\t']] = 1.0

    # Sampling loop for a batch of sequences
    # (to simplify, here we assume a batch of size 1).
    stop_condition = False
    decoded_seq = ''
    while not stop_condition:
        output_tokens, h, c = decoder_model.predict([target_seq] + states_value, verbose=0)

        # Sample a token
        sampled_token_index = np.argmax(output_tokens[0, -1, :])
        sampled_char = dce.inverse_char_index[sampled_token_index]
        decoded_seq += sampled_char

        # Exit condition: either hit max length
        # or find stop character.
        if sampled_char == '\n' or len(decoded_seq) > dce.max_seq_length:
            stop_condition = True

        # Update the target sequence (of length 1).
        target_seq = np.zeros((1, 1, len(dce.charset)))
        target_seq[0, 0, sampled_token_index] = 1.0

        # Update states
        states_value = [h, c]
    return decoded_seq

def predict_sequence_label(dce: DictionaryCharacterEncoder, encoder_model: keras.Model, decoder_model: keras.Model, input_str: str):
    """_summary_

    Arguments:
        dce -- _Instance of the DictionaryCharacterEncoder used for decoding._
        encoder_model -- _Keras encoder model._
        decoder_model -- _Keras decoder model._
        input_seq -- _description_

    Returns:
        _The decoded output sequence._
    """
    input_seq = dce.to_ids([input_str])

    # Encode the input as state vectors.
    states_value = encoder_model.predict(input_seq, verbose=0)

    # Generate empty target sequence of length 1.
    target_seq = np.zeros((1, 1))
    # Populate the first character of target sequence with the start character.
    target_seq[0, 0] = dce.char_index['\t']

    # Sampling loop for a batch of sequences
    # (to simplify, here we assume a batch of size 1).
    stop_condition = False
    decoded_seq = ''
    while not stop_condition:
        output_tokens, h, c = decoder_model.predict([target_seq] + states_value, verbose=0)

        # Sample a token
        sampled_token_index = np.argmax(output_tokens[0, -1, :])
        sampled_char = dce.inverse_char_index[sampled_token_index]
        decoded_seq += sampled_char

        # Exit condition: either hit max length
        # or find stop character.
        if sampled_char == '\n' or len(decoded_seq) > dce.max_seq_length:
            stop_condition = True

        # Update the target sequence (of length 1).
        target_seq = np.zeros((1, 1))
        target_seq[0, 0] = sampled_token_index

        # Update states
        states_value = [h, c]
    return decoded_seq