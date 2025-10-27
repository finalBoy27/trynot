# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && \
    apt-get install -y build-essential libxml2-dev libxslt-dev curl git && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY . .

# Expose Flask port
EXPOSE 8080

# Start the bot
CMD ["python", "telegram_bot.py"]
