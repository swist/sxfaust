# sxfaust

Faust App with Avro Schema registry and smaxtec key schema (raw).

Integrated smaXtec partitioner

## Usage

```python
from sxfaust import App, AvroParser


app = App(
    'xxx',
    version=1,
    key_serializer='raw',
    value_serializer='raw',
    broker='kafka://localhost:29092',
    schema_registry_url="http://localhost:8081/",
    autodiscover=["xxx.streams"],
    origin='xxx'
)


def load_configuration(config_name=None):
    # Load Config File
    return None


@app.on_configured.connect
def configure(app, conf, **kwargs):
    # Additional Configuration ...
    my_config = load_configuration()
    # conf.id = my_config.FAUST_ID
    # conf.broker = my_config.KAFKA_BOOTSTRAP_SERVER
    # conf.store = my_config.FAUST_STORE_URL

    # Config Avro Registry
    if hasattr(my_config, "KAFKA_SCHEMA_REGISTRY"):
        AvroParser.set_registry_url(my_config.KAFKA_SCHEMA_REGISTRY)


def main() -> None:
    app.main()
```