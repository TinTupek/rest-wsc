FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /upload
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
COPY upload.py .
CMD ["python", "upload.py"]