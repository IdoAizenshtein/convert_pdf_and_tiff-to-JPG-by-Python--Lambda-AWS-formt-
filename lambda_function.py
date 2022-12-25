#comment12
#comment
import os
from pdf2image import (
    convert_from_path,
    pdfinfo_from_path,
)
from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError,
)
from PIL import Image
import boto3
import uuid
import mimetypes
from urllib.parse import unquote_plus

s3Client = boto3.client('s3')
from boto3.s3.transfer import TransferConfig

KB = 1024
MB = KB * KB
config = TransferConfig(
    multipart_threshold=1 * MB,
    max_concurrency=5,
    multipart_chunksize=1 * MB,
    max_io_queue=10000,
    io_chunksize=1 * MB,
    use_threads=True
)
Image.MAX_IMAGE_PIXELS = None


def convert_pdf(file_path, bucketKey):
    output_path = []
    try:
        info = pdfinfo_from_path(file_path)
        output_path = convert_from_path(file_path, output_folder='/tmp',
                                        fmt="jpeg",
                                        jpegopt={"quality": 100, "progressive": True, "optimize": True},
                                        size=(None, 3508),
                                        dpi=300,
                                        paths_only=True)
        if info['Pages'] > len(output_path):
            print('Error found on any page')
            output_path = convert_from_path(file_path,
                                            output_folder='/tmp',
                                            fmt="jpeg",
                                            jpegopt={"quality": 100, "progressive": True, "optimize": True},
                                            dpi=300,
                                            paths_only=True)
    except PDFInfoNotInstalledError as e_data:
        print('e_data', e_data)
    except PDFPageCountError as e_data1:
        print('e_data1', e_data1)
    except PDFSyntaxError as e_data2:
        print('e_data2', e_data2)
    except Exception as general_error:
        print(general_error)
        print("Something went wrong")
    finally:
        return output_path



def convert_tiff(file_path, bucketKey):
    temp_images = []
    try:
        images = Image.open(file_path)
        for i in range(images.n_frames):
            images.seek(i)
            images.thumbnail(images.size)
            out = images.convert("RGB")
            image_path = f'/tmp/{bucketKey}_{i}.jpg'
            out.save(image_path, "JPEG", quality=100)
            temp_images.append(image_path)
        return temp_images
    except Exception as e:
        print(e)
        return temp_images


def lambda_handler(event, context):
    bucketName = event['bucket']
    bucketKey = unquote_plus(event['key'], encoding='utf-8')
    download_path = '/tmp/{}_{}'.format(uuid.uuid4(), bucketKey)
    s3Client.download_file(bucketName, bucketKey, download_path)
    output_path = []
    names_images_uploaded = []
    bucketKeyClean = ""

    try:
        try:
            mime_type = mimetypes.guess_type(download_path)[0]
            print("The mimetypes is: ", mime_type)
        except Exception as mime_err:
            print(mime_err)
            raise mime_err

        bucketKeyClean = bucketKey.replace(".pdf", "").replace(".", "")
        if mime_type == 'application/pdf':
            output_path = convert_pdf(download_path, bucketKeyClean)
        if mime_type == 'image/tiff' or mime_type == 'image/bmp':
            output_path = convert_tiff(download_path, bucketKeyClean)


    except Exception as e:
        print(e)
        raise e
    finally:
        if os.path.exists(download_path):
            os.remove(download_path)
            print('The files removed download_path !')

    try:
        for index in range(len(output_path)):
            s3Client.upload_file(output_path[index], bucketName, f'{bucketKeyClean}_{index}.jpg',
                                 ExtraArgs={"ContentType": "image/jpeg"},
                                 Config=config)
            output_obj = {'bucketName': bucketName, 'bucketKey': f'{bucketKeyClean}_{index}.jpg'}
            names_images_uploaded.append(output_obj)
            print('Start to remove the file from tmp....')
            if os.path.exists(output_path[index]):
                os.remove(output_path[index])
                print('The file removed from the tmp location!')
            else:
                print("The file does not exist in the path: ", output_path[index])


    except Exception as e:
        print(e)
        print('Error uploading file to output bucket')
        raise e

    return names_images_uploaded
# DEV
# NEW DEV
