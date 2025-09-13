# Use an official Python runtime as a parent image
FROM python:3.11.9-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy the dependency files to the working directory
COPY poetry.lock pyproject.toml /app/

# Install any needed packages specified in pyproject.toml
RUN poetry install --no-root --no-dev

# Copy the rest of the application code to the working directory
COPY . /app

# Create a non-root user and switch to it
RUN useradd --create-home appuser && \
    chown -R appuser:appuser /app
USER appuser

# Specify the command to run on container start
CMD ["poetry", "run", "python", "-m", "load_clinicaltrialsgov"]
