# Use a stable Python image (close to your 3.13)
FROM python:3.12-slim

# Prevent .pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Install basic build tools (if any dependency needs compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (for better build caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project (app.py, Routes/, utils/, .env, etc.)
COPY . .

# Expose the Flask port (from app.run(..., port=4000))
EXPOSE 4000

# Default command: run exactly like you do on Windows
# If your main file name is different, change "app.py" below.
CMD ["python", "app.py"]
