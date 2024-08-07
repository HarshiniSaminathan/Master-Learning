
FROM python:3.8-slim


RUN apt-get update && \
    apt-get install -y \
    libpq-dev \
    build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY . .


ENV FLASK_APP=main.py
ENV FLASK_RUN_HOST=0.0.0.0

EXPOSE 5050


CMD ["flask", "run"]


