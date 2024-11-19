FROM python:3.12.7

# Create the user first
RUN useradd -m -u 1000 user

# Set up working directory and install dependencies with root privileges
WORKDIR /app
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*
# Create the output folder and set the appropriate permissions
RUN mkdir -p /app/output_folder && chown -R user:user /app/output_folder
RUN mkdir -p /app/segment_folder && chown -R user:user /app/segment_folder
RUN chown -R user:user /app
# Switch to non-root user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

COPY --chown=user ./requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY --chown=user . /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]