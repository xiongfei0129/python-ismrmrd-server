

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
        logging.info("Sending MRD_MESSAGE_ISMRMRD_IMAGE (1022)")
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_IMAGE))
        # image.serialize_into(self.socket.send)

        # Don't use serialize_into because we send attributes as null-terminated string
        self.socket.send(image.getHead())
        self.socket.send(constants.MrdMessageAttribLength.pack(len(image.attribute_string)+1))
        self.socket.send(bytes(image.attribute_string, 'utf-8'))
        self.socket.send(bytes('\0',                   'utf-8'))
        self.socket.send(bytes(image.data))

    def send_acquisition(self, acquisition):
        logging.info("Sending MRD_MESSAGE_ISMRMRD_ACQUISITION (1008)")
        self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_ACQUISITION))
        acquisition.serialize_into(self.socket.send)

    def send_waveform(self, waveform):
        logging.info("Sending MRD_MESSAGE_ISMRMRD_WAVEFORM (1026)")
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
        logging.info("Received MRD_MESSAGE_CLOSE (4)")
        self.is_exhausted = True
        return

    def read_mrd_message_ismrmrd_acquisition(self):
        logging.info("Received MRD_MESSAGE_ISMRMRD_ACQUISITION (1008)")
        return ismrmrd.Acquisition.deserialize_from(self.read)

    def read_mrd_message_ismrmrd_waveform(self):
        logging.info("Received MRD_MESSAGE_ISMRMRD_WAVEFORM (1026)")
        return ismrmrd.Waveform.deserialize_from(self.read)

    def read_mrd_message_ismrmrd_image(self):
        logging.info("Received MRD_MESSAGE_ISMRMRD_IMAGE (1022)")
        # return ismrmrd.Image.deserialize_from(self.read)

        # Explicit version of deserialize_from() for more verbose debugging
        logging.info("Reading in %d bytes of image header", ctypes.sizeof(ismrmrd.ImageHeader))
        header_bytes = self.read(ctypes.sizeof(ismrmrd.ImageHeader))

        attribute_length_bytes = self.read(ctypes.sizeof(ctypes.c_uint64))
        attribute_length = ctypes.c_uint64.from_buffer_copy(attribute_length_bytes)
        logging.info("Reading in %d bytes of attributes", attribute_length.value)

        attribute_bytes = self.read(attribute_length.value)
        logging.info("Attributes: %s", attribute_bytes)

        image = ismrmrd.Image(header_bytes, attribute_bytes.decode('utf-8'))

        def calculate_number_of_entries(nchannels, xs, ys, zs):
            return nchannels * xs * ys * zs

        nentries = calculate_number_of_entries(image.channels, *image.matrix_size)
        nbytes = nentries * ismrmrd.get_dtype_from_data_type(image.data_type).itemsize

        logging.info("Reading in %d bytes of image data", nbytes)
        data_bytes = self.read(nbytes)

        image.data.ravel()[:] = np.frombuffer(data_bytes, dtype=ismrmrd.get_dtype_from_data_type(image.data_type))

        return image

