### Raw to Refined
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @r2r-request.json \
    "https://dataproc.googleapis.com/v1/projects/extreme-course-404805/locations/asia-south1/batches"

### Refined to Integrated
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @r2i-request.json \
    "https://dataproc.googleapis.com/v1/projects/extreme-course-404805/locations/asia-south1/batches"
