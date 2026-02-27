# Use an official Python runtime as a base image
FROM python:3.12.2-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements.txt file first to leverage Docker cache
# This step only runs if requirements.txt changes
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Define the command to run the application
CMD ["python", "./main.py"]
