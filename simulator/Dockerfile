FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fleetsim ./fleetsim
ENV PYTHONUNBUFFERED=1
EXPOSE 9105
# Software-only virtual vehicle fleet (no hardware required)
CMD ["python", "-m", "fleetsim.main"]
