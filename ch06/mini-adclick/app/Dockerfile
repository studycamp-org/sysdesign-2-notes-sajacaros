FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && python -c "import duckdb; duckdb.execute('INSTALL httpfs')"
COPY . .
ENV PYTHONUNBUFFERED=1
