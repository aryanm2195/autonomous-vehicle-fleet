FROM python:3.11-slim
WORKDIR /srv/backend
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY dashboard /srv/dashboard
ENV PYTHONUNBUFFERED=1 DASHBOARD_DIR=/srv/dashboard
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
