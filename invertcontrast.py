
import ismrmrd
import os
import itertools
import logging
import numpy as np
import numpy.fft as fft


def groups(iterable, predicate):
    group = []
    for item in iterable:
        group.append(item)

        if predicate(item):
            yield group
            group = []


# Continuously parse incoming data parsed from MRD messages
#  - For raw k-space data, collect data that meets criteria "predicateAccept"
#    and pass along when "predicateFinish" is satisfied.
#  - For image data, pass along immediately.
def process_data(iterable, predicateAccept, predicateFinish):
    group = []
    try:
        for item in iterable:
            if item is None:
                break

            elif isinstance(item, ismrmrd.Acquisition):
                logging.info("Lin: %s", item.idx.kspace_encode_step_1)
                if predicateAccept(item):
                    group.append(item)

                if predicateFinish(item):
                    yield group
                    group = []

            elif isinstance(item, ismrmrd.Image):
                group.append(item)
                yield group
                group = []
            else:
                logging.info("Unsupported data type %s", type(item).__name__)

    finally:
        iterable.send_close()


def process(connection, config, params):
    logging.info("Processing connection.")
    logging.info("Config: \n%s", config.decode("utf-8"))
    logging.info("Params: \n%s", params.decode("utf-8"))

    # Discard phase correction lines and accumulate lines until "ACQ_LAST_IN_SLICE" is set
    for group in process_data(connection, lambda acq: not acq.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA), lambda acq: acq.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE)):
        if isinstance(group[0], ismrmrd.Image):
            logging.info("Processing an image")
            image = process_image(group[0], config, params)
        else:
            logging.info("Processing a group of k-space data")
            image = process_raw(group, config, params)

        logging.info("Sending image to client:\n%s", image)
        connection.send_image(image)


def process_raw(group, config, params):
    # Folder for debug output files
    debugFolder = "/tmp/share/dependency"

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.info("Created folder " + debugFolder + " for debug output files")

    # Sort by line number (incoming data may be interleaved)
    lin = [acquisition.idx.kspace_encode_step_1 for acquisition in group]
    logging.info("Incoming lin ordering: " + str(lin))

    group.sort(key = lambda acq: acq.idx.kspace_encode_step_1)
    sortedLin = [acquisition.idx.kspace_encode_step_1 for acquisition in group]
    logging.info("Sorted lin ordering: " + str(sortedLin))

    # Format data into single [cha RO PE] array
    data = [acquisition.data for acquisition in group]
    data = np.stack(data, axis=-1)

    logging.info("Raw data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "raw.npy", data)

    # Fourier Transform
    data = fft.fftshift(data, axes=(1, 2))
    data = fft.ifft2(data)
    data = fft.ifftshift(data, axes=(1, 2))

    # Sum of squares coil combination
    data = np.abs(data)
    data = np.square(data)
    data = np.sum(data, axis=0)
    data = np.sqrt(data)

    logging.info("Image data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "img.npy", data)

    # Normalize and convert to int16
    data *= 32768/data.max()
    data = np.around(data)
    data = data.astype(np.int16)

    # Invert image contrast
    data = 32768-data
    data = np.abs(data)
    data = data.astype(np.int16)
    np.save(debugFolder + "/" + "imgInverted.npy", data)

    # Remove phase oversampling
    nRO = np.size(data,0);
    data = data[int(nRO/4):int(nRO*3/4),:]
    logging.info("Image without oversampling is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "imgCrop.npy", data)

    # Format as ISMRMRD image data
    image = ismrmrd.Image.from_array(data, acquisition=group[0])
    image.image_index = 1

    # Set ISMRMRD Meta Attributes
    meta = ismrmrd.Meta({'DataRole':               'Image',
                         'ImageProcessingHistory': ['FIRE', 'PYTHON'],
                         'WindowCenter':           '16384',
                         'WindowWidth':            '32768'})
    xml = meta.serialize()
    logging.info("Image MetaAttributes: %s", xml)
    logging.info("Image data has %d elements", image.data.size)

    image.attribute_string = xml
    return image


def process_image(image, config, params):
    # Folder for debug output files
    debugFolder = "/tmp/share/dependency"

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.info("Created folder " + debugFolder + " for debug output files")

    logging.info("Incoming image data of type %s", ismrmrd.get_dtype_from_data_type(image.data_type))

    # Extract image data itself
    data = image.data
    logging.info("Original image data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "imgOrig.npy", data)

    # Normalize and convert to int16
    data = data.astype(np.float64)
    data *= 32768/data.max()
    data = np.around(data)
    data = data.astype(np.int16)

    # Invert image contrast
    data = 32768-data
    data = np.abs(data)
    data = data.astype(np.int16)
    np.save(debugFolder + "/" + "imgInverted.npy", data)

    # Create new MRD instance for the inverted image
    imageInverted = ismrmrd.Image.from_array(data)
    data_type = imageInverted.data_type;

    # Copy the fixed header information
    oldHeader = image.getHead();
    oldHeader.data_type = data_type;
    imageInverted.setHead(oldHeader)

    # Set ISMRMRD Meta Attributes
    meta = ismrmrd.Meta({'DataRole':               'Image',
                         'ImageProcessingHistory': ['FIRE', 'PYTHON'],
                         'WindowCenter':           '16384',
                         'WindowWidth':            '32768'})
    xml = meta.serialize()
    logging.info("Image MetaAttributes: %s", xml)
    logging.info("Image data has %d elements", image.data.size)

    imageInverted.attribute_string = xml

    return imageInverted