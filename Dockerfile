FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py status_image.py history.py .
COPY fonts ./fonts
ENV PYTHONUNBUFFERED=1
CMD ["python", "bot.py"]
