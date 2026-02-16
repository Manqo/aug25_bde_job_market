FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install dependencies for psycopg2 + SSL
RUN apt-get update && apt-get install -y \
    libpq-dev gcc ca-certificates \
 && pip install --no-cache-dir --upgrade pip
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

EXPOSE 8000

# CMD ["python", "./api/main.py"]
CMD ["python", "-m", "api.main"]
