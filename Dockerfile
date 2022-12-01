# A simple container for therapy normalizer.

# The commands following all RUN instructions are run in a shell, which
# by default is /bin/sh -c on Linux or cmd /S /C on Windows.

# Initialize a new build stage and set the base image to the Docker
# python image that has the "latest" tag (currently 3.10.8).
FROM python:3.8

# Install pipenv and uvicorn from PyPI into the container.
RUN pip install pipenv uvicorn[standard]

# Copy the current working directory to /app in the container.
COPY . /app

# Set /app in the container to be the container's working directory for
# all RUN, CMD, ENTRYPOINT, COPY, and ADD instructions hereafter.
WORKDIR /app

# Lock all default and development packages listed in Pipfile and their
# dependencies into Pipfile.lock if the file doesn't already exist.
RUN if [ ! -f "Pipfile.lock" ] ; then pipenv lock && pipenv lock --dev ; else echo Pipfile.lock exists ; fi

# Install packages exactly as specified in Pipfile.lock into the virtual
# environment.
RUN pipenv sync

# The container listens on port 80; TCP by default.
EXPOSE 80

# Healthchecks service up every 5m.  
HEALTHCHECK --interval=5m --timeout=3s \
    CMD curl -f http://localhost/therapy || exit 1

CMD pipenv run uvicorn therapy.main:app --reload --port 80 --host 0.0.0.0
