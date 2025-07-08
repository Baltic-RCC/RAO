import io
import uuid
import logging
from zipfile import ZipFile, ZIP_DEFLATED


def package_for_pypowsybl(opdm_objects, return_zip: bool = False):
    """
    Method to transform OPDM objects into sufficient format binary buffer or zip package
    :param opdm_objects: list of OPDM objects
    :param return_zip: flag to save OPDM objects as zip package in local directory
    :return: binary buffer or zip package file name
    """
    output_object = io.BytesIO()
    if return_zip:
        output_object = f"{uuid.uuid4()}.zip"
        logging.info(f"Adding files to {output_object}")

    with ZipFile(output_object, "w") as global_zip:
        for opdm_components in opdm_objects:
            for instance in opdm_components['opde:Component']:
                with ZipFile(io.BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                    for file_name in instance_zip.namelist():
                        logging.info(f"Adding file: {file_name}")
                        global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return output_object


def repackage_model_zip(path_or_buffer: str):
    """
    Extracts zipped profiles of a model to global zip.
    Detects automatically if the zipped folder is read from file bath or BytesIO buffer.
    :param path_or_buffer: path of zipped model or BytesIO buffer
    :return: output_zip_buffer
    """
    # Checks if path_or_buffer is string
    if isinstance(path_or_buffer, str):
        with open(path_or_buffer, 'rb') as original_zip_file:
            original_zip_buffer = io.BytesIO(original_zip_file.read())
    elif isinstance(path_or_buffer, io.BytesIO):  # checks if path_or_buffer is BytesIO class
        original_zip_buffer = path_or_buffer
    else:
        raise Exception("Provided variable is nor string nor BytesIO object")

    output_zip_buffer = io.BytesIO()

    # Read the original zipped folder from provided buffer
    with ZipFile(original_zip_buffer, 'r') as original_zip:
        # Create a new zip file where we will store the unzipped XMLs
        with ZipFile(output_zip_buffer, 'w', ZIP_DEFLATED) as new_zip:
            # Iterate trough each file in the original zip
            for file_name in original_zip.namelist():
                # Read the zipped xml file
                with original_zip.open(file_name) as zipped_xml_file:
                    with ZipFile(zipped_xml_file) as zipped_xml:
                        # Extract each file and add it to the new zip
                        for xml_file_name in zipped_xml.namelist():
                            xml_data = zipped_xml.read(xml_file_name)
                            # Add unzipped XML file to the new zip
                            new_zip.writestr(xml_file_name, xml_data)

    output_zip_buffer.seek(0)

    return output_zip_buffer
