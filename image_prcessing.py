import logging
import os
import uuid
import boto3
from PIL import Image, ImageFilter

# Configuration for AWS services
input_bucket = output_bucket = "bench-image-processing-image-bucket-mj"
CREDENTIALS = {'aws_access_key_id': 'xxxxx',
               'aws_secret_access_key': 'xxxxx'}
region = "us-east-1"
for name in ['boto', 'urllib3', 's3transfer', 'boto3', 'botocore', 'nose', 'TiffImagePlugin']:
    logging.getLogger(name).setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

# S3 & Lambda client
s3_client = boto3.client('s3', region_name=region, **CREDENTIALS)
lambda_client = boto3.client('lambda', region_name=region, **CREDENTIALS)

# For information when saving image
FILE_NAME_INDEX = 2
TMP = "/tmp/"
save_path_dir = "/tmp"

# Logging Information
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s - %(lineno)d: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def download_image_from_s3(img_name):
    download_path = f'/tmp/{uuid.uuid4()}{img_name}'
    s3_client.download_file(input_bucket, img_name, download_path)
    return download_path


def download_image_locally(img_name):
    return os.path.join("/tmp", img_name)


def upload_image_to_s3(path):
    file_name_path = path.split("/")[FILE_NAME_INDEX]
    s3_client.upload_file(path, output_bucket, file_name_path)
    return file_name_path


def upload_image_locally(path):
    return path.split("/")[FILE_NAME_INDEX]


def process_flip(download_path, img_name):
    with Image.open(download_path) as image:
        path = f"{TMP}flip-left-right-{img_name}"
        img = image.transpose(Image.FLIP_LEFT_RIGHT)
        img.save(path)

        path = f"{TMP}flip-top-bottom-{img_name}"
        img = image.transpose(Image.FLIP_TOP_BOTTOM)
        img.save(path)
    return path


def process_rotate(download_path, img_name):
    with Image.open(download_path) as image:
        path = f"{TMP}rotate-90-{img_name}"
        img = image.transpose(Image.ROTATE_90)
        img.save(path)

        path = f"{TMP}rotate-180-{img_name}"
        img = image.transpose(Image.ROTATE_180)
        img.save(path)

        path = f"{TMP}rotate-270-{img_name}"
        img = image.transpose(Image.ROTATE_270)
        img.save(path)
    return path


def process_image_filter(download_path, img_name):
    with Image.open(download_path) as image:
        path = f"{TMP}blur-{img_name}"
        img = image.filter(ImageFilter.BLUR)
        img.save(path)

        path = f"{TMP}contour-{img_name}"
        img = image.filter(ImageFilter.CONTOUR)
        img.save(path)

        path = f"{TMP}sharpen-{img_name}"
        img = image.filter(ImageFilter.SHARPEN)
        img.save(path)
    return path


def process_gray_scale(download_path, img_name):
    with Image.open(download_path) as image:
        path = f"{TMP}gray-scale-{img_name}"
        img = image.convert('L')
        img.save(path)
    return path


def process_resize(download_path, img_name):
    with Image.open(download_path) as image:
        path = f"{TMP}resized-{img_name}"
        image.thumbnail((128, 128))
        image.save(path)
    return path


def flip(img_name):
    download_path = download_image_from_s3(img_name)

    path = process_flip(download_path, img_name)

    return upload_image_locally(path)


def rotate(img_name):
    download_path = download_image_locally(img_name)

    path = process_rotate(download_path, img_name)

    return upload_image_locally(path)


def image_filter(img_name):
    download_path = download_image_locally(img_name)

    path = process_image_filter(download_path, img_name)

    return upload_image_locally(path)


def gray_scale(img_name):
    download_path = download_image_locally(img_name)

    path = process_gray_scale(download_path, img_name)

    return upload_image_locally(path)


def resize(img_name):
    download_path = os.path.join(save_path_dir, img_name)

    with Image.open(download_path) as image:
        path = f"{TMP}resized-{img_name}"
        image.thumbnail((128, 128))
        image.save(path)

    img_path = path.split("/")[FILE_NAME_INDEX]
    s3_client.upload_file(path, output_bucket, img_path)

    return img_path


def image_processing(img_name):
    flipped_image = flip(img_name)
    rotated_image = rotate(flipped_image)
    filtered_image = image_filter(rotated_image)
    gray_scaled_img = gray_scale(filtered_image)
    final_img = resize(gray_scaled_img)

    logging.info(f'final image is {final_img}')


def main():
    image_list = {'image.jpg': '4742 x 2667'}
    for each_image in image_list:
        image_processing(each_image)


if __name__ == '__main__':
    main()
