FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /flask_app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY flask_app.py .
EXPOSE 5000
CMD ["python", "flask_app.py"]