import azure.functions as func
import logging
import datetime
import io
import os
import logging
import zipfile
from azure.storage.blob import BlobServiceClient
from azure.functions import HttpRequest, HttpResponse

# app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="httpBlobTrigger")
def httpBlobTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    container_name = req.params.get("containerName")
    if not container_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            container_name = req_body.get("containerName")

    connection_string = os.environ["ConnectionString"]

    if not container_name:
        return HttpResponse(
            "Please pass a container name on the query string", status_code=400
        )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Check if the container exists
    try:
        container_properties = container_client.get_container_properties()
    except Exception as e:
        logging.error(f"Error checking container: {e}")
        return HttpResponse("Container does not exist", status_code=404)

    # Create a temporary in-memory zip file
    zip_stream = io.BytesIO()

    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for blob in container_client.list_blobs(name_starts_with="current/"):
            blob_client = container_client.get_blob_client(blob.name)
            download_stream = io.BytesIO()
            try:
                blob_client.download_blob().readinto(download_stream)
                download_stream.seek(0)
                zip_file.writestr(blob.name[len("current/") :], download_stream.read())
            except Exception as e:
                logging.error(f"Error processing blob {blob.name}: {e}")
                return HttpResponse("Error processing blobs", status_code=500)

    zip_stream.seek(0)
    zip_file_name = f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.zip"

    # Upload the zip file to the same container
    try:
        zip_blob_client = container_client.get_blob_client(zip_file_name)
        zip_blob_client.upload_blob(zip_stream, overwrite=True)
    except Exception as e:
        logging.error(f"Error uploading zip file: {e}")
        return HttpResponse("Error uploading zip file", status_code=500)

    return HttpResponse("Zip file created and uploaded successfully", status_code=200)
