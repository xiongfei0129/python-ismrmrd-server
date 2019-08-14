

import constants
import ismrmrd
import ctypes

import logging
import socket
import numpy as np


class Connection:
    """
    This is a docstring. It should be a good one.
    """

    def __init__(self, socket):
        self.socket = socket
        self.is_exhausted = False
        self.handlers = {
            constants.MRD_MESSAGE_CONFIG_FILE:         self.read_mrd_message_config_file,
            constants.MRD_MESSAGE_CONFIG_SCRIPT:       self.read_mrd_message_config_script,
            constants.MRD_MESSAGE_PARAMETER_SCRIPT:    self.read_mrd_message_parameter_script,
            constants.MRD_MESSAGE_CLOSE:               self.read_mrd_message_close,
            constants.MRD_MESSAGE_ISMRMRD_ACQUISITION: self.read_mrd_message_ismrmrd_acquisition,
            constants.MRD_MESSAGE_ISMRMRD_WAVEFORM:    self.read_mrd_message_ismrmrd_waveform,
            constants.MRD_MESSAGE_ISMRMRD_IMAGE:       self.read_mrd_message_ismrmrd_image
        }

    def __iter__(self):
        while not self.is_exhausted:
            yield self.next()

    def __next__(self):
        return self.next()

    def read(self, nbytes):
        return self.socket.recv(nbytes, socket.MSG_WAITALL)

    def send_image(self, image):
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_IMAGE))
        self.socket.send(image.getHead())
        self.socket.send(constants.MrdMessageAttribLength.pack(len(image.attribute_string)))
        self.socket.send(image.attribute_string)
        self.socket.send(bytes(image.data))

    def send_acquisition(self, acquisition):
        logging.info("Received MRD_MESSAGE_ISMRMRD_ACQUISITION (1008)")
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_ACQUISITION))
        acquisition.serialize_into(self.socket.send)

    def send_waveform(self, waveform):
        logging.info("Received MRD_MESSAGE_ISMRMRD_WAVEFORM (1026)")
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_WAVEFORM))
        waveform.serialize_into(self.socket.send)

    def send_close(self):
        logging.info("Sending MRD_MESSAGE_CLOSE (4)")
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_CLOSE))

    def next(self):
        id = self.read_mrd_message_identifier()
        handler = self.handlers.get(id, lambda: Connection.unknown_message_identifier(id))
        return handler()

    @staticmethod
    def unknown_message_identifier(identifier):
        logging.error("Received unknown message type: %d", identifier)
        raise StopIteration

    def read_mrd_message_identifier(self):
        identifier_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_IDENTIFIER)
        return constants.MrdMessageIdentifier.unpack(identifier_bytes)[0]

    def read_mrd_message_length(self):
        length_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_LENGTH)
        return constants.MrdMessageLength.unpack(length_bytes)[0]

    def read_mrd_message_config_file(self):
        logging.info("Received MRD_MESSAGE_CONFIG_FILE (1)")
        config_file_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_CONFIGURATION_FILE)
        config_file = constants.MrdMessageConfigurationFile.unpack(config_file_bytes)[0]

        return config_file

    def read_mrd_message_config_script(self):
        logging.info("Received MRD_MESSAGE_CONFIG_SCRIPT (2)")
        length = self.read_mrd_message_length()
        return self.read(length)

    def read_mrd_message_parameter_script(self):
        logging.info("Received MRD_MESSAGE_PARAMETER_SCRIPT (3)")
        length = self.read_mrd_message_length()
        return self.read(length)

    def read_mrd_message_close(self):
        logging.info("Received MRD_MESSAGE_CLOSE (4) -- Stopping session")
        self.is_exhausted = True
        raise StopIteration

    def read_mrd_message_ismrmrd_acquisition(self):
        return ismrmrd.Acquisition.deserialize_from(self.read)

    def read_mrd_message_ismrmrd_waveform(self):
        return ismrmrd.Waveform.deserialize_from(self.read)

    def read_mrd_message_ismrmrd_image(self):
        logging.info("Received MRD_MESSAGE_ISMRMRD_IMAGE (1022)")
        return ismrmrd.Image.deserialize_from(self.read)
