FROM polusai/bfio:2.1.9

# Install poetry
RUN pip install poetry==1.6.1

# Configure poetry
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Set up Preadator
WORKDIR /app

COPY pyproject.toml ./
RUN touch README.md

# Install the dependencies without installing preadator
RUN poetry install --no-root && rm -rf $POETRY_CACHE_DIR

# Copy the source code and tests
COPY src ./src
COPY tests ./tests

# Install preadator
RUN poetry install

# Run the tests
RUN poetry run pytest -v
