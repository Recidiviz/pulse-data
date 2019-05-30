# `read-pdf` Service
Due to a bug with grpc and gunicorn (grpc/grpc#18980), we need to run calls to
[tabula-py](https://github.com/chezou/tabula-py) in a separate python runtime
that does not include grpc.

## API
A single endpoint, `read_pdf`, downloads PDF files from a GCS bucket and calls
`tabula.read_pdf`. The endpoint accepts POST requests and expects both query
params and JSON post data.

The query params specify the location of the PDF in cloud storage:
* `location` the name of the GCS bucket and possibly the subdirectory the file
  is stored in, and
* `filename` the name of the file located in the specified `location`.

The JSON post data is passed directly to `tabula.read_pdf` as unpacked
`**kwargs`, so it should be structured as a dictionary keyed on string values.
The values of the dictionary may be nested objects, e.g. the `pandas_options`
argument.

The resulting DataFrame is serialized to CSV and output as the HTTP response.

## Deploying
The `read-pdf` service is a microservice of the `recidiviz` project, so separate
versions should be deployed for staging and production. As the service relies on
code shared with the `default` service, the docker build context must be the
root of the repository. The docker image can be built by specifying the location
of the Dockerfile from the top level:
`docker build -t read-pdf-image -f recidiviz/read_pdf/Dockerfile .`
