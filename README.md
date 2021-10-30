# Snowdaemon 0.2.0
Helper package to facilitate configuration and management of [Snowplow](https://github.com/snowplow/snowplow) 
microservices and dataflow batch jobs.

1. Downloads configuration from a specified S3 Bucket (either a file or a folder depending on requirements)
2. Runs the service using Popen()
3. Diverts logging output from service to a logger
4. Provides SNS alerting

Usage:

`python -m snowdaemon --config CONFIG_FILE_LOCATION`

Where config file follows the format of the yaml shown in config.yaml.example of project root.

This can be run using systemd with the following configuration:

```
[Unit]
Description=Snowplow service
After=multi-user.target

[Service]
Type=idle
ExecStart=/usr/bin/python3 -m snowdaemon --config /etc/snowdaemon/config.yaml
Restart=on-failure

[Install]
WantedBy=multi-user.target

```

 