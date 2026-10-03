FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY telemetry_service ./telemetry_service
COPY alert_service ./alert_service
COPY analytics_service ./analytics_service
ENV PYTHONUNBUFFERED=1
# The same image runs every service; docker-compose / k8s choose the module:
#   python -m telemetry_service.main | alert_service.main | analytics_service.main
CMD ["python", "-m", "telemetry_service.main"]
