FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Environment variables
ENV PORT=8080

# Run the app
CMD ["python", "app/main.py"]