FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY summarization_service.py .
CMD ["python", "summarization_service.py"]