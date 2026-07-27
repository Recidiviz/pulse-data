FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY pg_ticket_diagnosis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY claude_agent.py pg_ticket_diagnosis/run_pg_ticket_diagnosis.py pg_ticket_diagnosis/pii_doc_parser_utils.py ./

ENTRYPOINT ["python", "/app/run_pg_ticket_diagnosis.py"]
