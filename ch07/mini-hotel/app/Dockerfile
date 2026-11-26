FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# SERVICE 환경 변수로 어떤 서비스를 띄울지 고른다 (같은 이미지를 서비스마다 다른 컨테이너로 띄운다)
CMD ["sh", "-c", "uvicorn ${MODULE}:app --host 0.0.0.0 --port 8000 --log-level warning"]
