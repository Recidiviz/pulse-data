FROM python:3.12-slim

WORKDIR /app

COPY pg_ticket_diagnosis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY claude_agent.py pg_ticket_diagnosis/run_pg_ticket_diagnosis.py pg_ticket_diagnosis/pii_doc_parser_utils.py ./

ENTRYPOINT ["python", "/app/run_pg_ticket_diagnosis.py"]
