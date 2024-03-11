import os
import tempfile
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import requests
import PyPDF2
import random
import io
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime, timedelta

load_dotenv()

# azure_storage_connection_string = os.environ.get('AZURE_CONNECTION_STRING')
azure_storage_connection_string = 'REPLACE_ME'
print('this is the connection string', azure_storage_connection_string)

if not azure_storage_connection_string:
    raise ValueError("Azure Storage connection string is not set.")

container_name = 'resumes-pdf'


blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)
blobs = [blob.name for blob in container_client.list_blobs()]

def get_random_blob():
    blobs = [blob.name for blob in container_client.list_blobs()]
    print('total documents are :', blobs.count)
    return random.choice(blobs)

def get_random_blob(blobs):
    # blobs = [blob.name for blob in container_client.list_blobs()]
    print('total documents are :', len(blobs))
    return random.choice(blobs)

def get_my_blob(blobs, my_blob):
    # blobs = [blob.name for blob in container_client.list_blobs()]
    for blob in blobs:
        if(blob.name == my_blob):
            return blob
        else: return 'NOT_FOUND'


def download_pdf_from_blob(blob_client, destination_file_path):
    with open(destination_file_path, 'wb') as file:
        file.write(blob_client.download_blob().readall())

def extract_text_from_pdf(pdf_file_path):
    with open(pdf_file_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        for page_num in range(len(pdf_reader.pages)):
            text += pdf_reader.pages[page_num].extract_text()
    return text

def extract_text_from_pdf_via_url(url):
    # You can download the file as a byte stream with requests wrapping it with io.BytesIO()
    # https://stackoverflow.com/questions/45470964/python-extracting-text-from-webpage-pdf

    # url = 'https://surudatahackathon2024.blob.core.windows.net/resumes-pdf/IOS1.pdf?sp=r&st=2024-03-10T21:07:44Z&se=2024-03-11T05:07:44Z&spr=https&sv=2022-11-02&sr=b&sig=BpZiQJMyCifovumvVPzW7CeKEGeGj%2FHaQ4NAVQ4gISA%3D'
    # url = 'https://surudatahackathon2024.blob.core.windows.net/resumes-pdf/data-scientist-1559725114.pdf?sp=r&st=2024-03-11T02:43:43Z&se=2024-03-11T10:43:43Z&spr=https&sv=2022-11-02&sr=b&sig=z7rAzvzm0w0P2nduAfPsU8H8RSMj54H38BHhxWz9iQ8%3D'
    response = requests.get(url)

    my_raw_data = response.content
    
    with BytesIO(my_raw_data) as data:
        # data is a file like object you can use just like you opened a PDF file. this way the file is only in the memory and never saved locally.
        read_pdf = PyPDF2.PdfReader(data)
        text = ''
        for page in range(len(read_pdf.pages)):
            text += read_pdf.pages[page].extract_text()
        return text


# def main():
#     blob_name = get_random_blob()
#     print('blob_name =>', blob_name)
#     # blob_client = container_client.get_blob_client(blob_name)
#     blob_client = container_client.get_blob_client('UIUX_Resume1.pdf')

#     with tempfile.TemporaryDirectory() as temp_dir:
#         # pdf_file_path = os.path.join(temp_dir, 'downloaded_pdf.pdf')
#         pdf_file_path = 'C:/Users/BAJRACHARYA/work/projects/AZURE/GITProjects/resume-vector-search/pdf-to-text/downloaded_pdf.pdf'
#         download_pdf_from_blob(blob_client, pdf_file_path)
#         pdf_text = extract_text_from_pdf(pdf_file_path)

#         print(f"Extracted text from PDF:\n{pdf_text}")

def main():

    # generate a shared access signature for the files and load them into python
    
    my_azure_account_name='surudatahackathon2024'
    my_azure_account_key='REPLACE_ME'
    my_container_name = 'resumes-pdf'
    my_blob_name = 'UIUX_Resume1.pdf'
    sas_uiux_token = generate_blob_sas(account_name=my_azure_account_name,
                      container_name=my_container_name,
                      blob_name=my_blob_name,
                      account_key=my_azure_account_key,
                      permission=BlobSasPermissions(read=True),
                      expiry=datetime.utcnow() + timedelta(hours=1)
                    )
        
    
    sas_uiux_url = 'https://' + my_azure_account_name + '.blob.core.windows.net/' + my_container_name + '/' + my_blob_name + '?'  + sas_uiux_token
    print('SAS_URL is => ', sas_uiux_url)

    pdf_text = extract_text_from_pdf_via_url(sas_uiux_url)
    print('Extracted text from PDF:', pdf_text)


if __name__ == "__main__":
    main()
