# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /

# Copy requirements file to the container
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project to the container
COPY . .

# Create the required folder structure inside the container
RUN mkdir -p  /uploads /output

# Expose the port your application runs on (if applicable)
EXPOSE 5000

# Define environment variables (if needed)
ENV PYTHONUNBUFFERED=1

# Define the default command to run your application
CMD ["python", "app.py"]
