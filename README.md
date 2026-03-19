# Telemetry Validator Service

Stateless Django microservice that consumes raw telemetry from Kafka, validates envelope and payload against TelemetrySchema, applies data transformations, and routes results to clean or dead-letter topics.

## Data Flow

```
telemetry.raw ──► [Validator] ──► telemetry.clean (valid)
                       │
                       └──► telemetry.dlq (invalid)
```

**Processing pipeline per message:**

1. Decode JSON from `telemetry.raw`
2. Validate envelope (JSON Schema + UUID check)
3. Fetch TelemetrySchema by `schema_version` (Redis cache → Device API fallback)
4. Validate payload against schema
5. Apply transformation rules (rename, multiply, divide, round, remove, timestamp)
6. Resolve device by `serial_number` (Redis cache → Device API fallback)
7. Publish to `telemetry.clean` or `telemetry.dlq`

## Project Structure

```
├── app/
│   ├── manage.py                          # Django entrypoint
│   ├── telemetry_validator/
│   │   ├── settings.py                    # Django settings (env-based config)
│   │   └── urls.py
│   └── validator/
│       ├── management/commands/
│       │   └── run_validator.py           # Kafka consumer loop
│       └── services/
│           ├── envelope.py                # Envelope validation (JSON Schema)
│           ├── transform.py               # Payload transformation engine
│           ├── schema_cache.py            # Redis + HTTP cache for schemas/devices
│           └── pipeline.py                # MessageRouter: decode → validate → route
├── tests/
├── scripts/
│   └── entrypoint.sh                     # Docker entrypoint
├── Dockerfile                             # Multi-stage build
├── requirements.txt
└── .env.example
```

## Running Locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit values as needed

cd app
python manage.py run_validator
```

## Testing

```bash
cd app
DJANGO_SECRET_KEY=test DJANGO_SETTINGS_MODULE=telemetry_validator.settings \
    pytest ../tests/ -v
```

## Shared Libraries

- [IoTKafka](https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib) — Kafka consumer/producer wrapper
- [iot-logging](https://github.com/IoT-Hub-Alpha/logging-lib) — Structured JSON logging

## Related Services

- [IoT-Hub-Alpha](https://github.com/IoT-Hub-Alpha/IoT-Hub-Alpha) — Umbrella repo
- [DB-Writer-Service](https://github.com/IoT-Hub-Alpha/DB-Writer-Service) — Writes clean telemetry to DB
- [Redis-Service](https://github.com/IoT-Hub-Alpha/Redis-Service) — Shared Redis instance
