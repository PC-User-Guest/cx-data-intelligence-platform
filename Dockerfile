FROM python:3.11.9-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ARG BRUIN_VERSION=0.11.508

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl ca-certificates tar nodejs npm \
	&& rm -rf /var/lib/apt/lists/*

RUN groupadd --system appgroup \
	&& useradd --system --gid appgroup --create-home appuser

RUN npm install --global --no-fund --no-audit newman

RUN curl --retry 5 --retry-delay 3 --retry-all-errors -fsSL \
	"https://github.com/bruin-data/bruin/releases/download/v${BRUIN_VERSION}/bruin_Linux_x86_64.tar.gz" \
	-o /tmp/bruin.tar.gz \
	&& tar -xzf /tmp/bruin.tar.gz -C /usr/local/bin bruin \
	&& rm -f /tmp/bruin.tar.gz

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipelines /app/pipelines
COPY tests /app/tests
COPY bruin /app/bruin
COPY postman /app/postman

RUN chown -R appuser:appgroup /app
USER appuser

CMD ["python", "-m", "pipelines.tickets_pipeline"]
