#   Copyright (c) 2020 Quantdeck Systems Ltd. All rights reserved.
#
#   This program is licensed to you under the Apache License Version 2.0, and
#   you may not use this file except in compliance with the Apache License
#   Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
#   http://www.apache.org/licenses/LICENSE-2.0.
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the Apache License Version 2.0 is distributed on an "AS
#   IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#   implied.  See the Apache License Version 2.0 for the specific language
#   governing permissions and limitations there under.
#
"""Helper functions which are common to both batch and streaming jobs
"""
import subprocess
import os
import re
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from socket import gethostname
import watchtower
import requests
import boto3


LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def get_instance_id():
    """Gets instance ID from AWS metadata, using hostname as fallback

        Returns instance ID
    """
    try:
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        instance_id = response.text
    except ConnectionError:
        instance_id = gethostname()

    return instance_id


def configure_logging(log, config, cloudwatch_stream_name):
    """Configures logging

    Adds logging outputs as specified in conf file, any of:
        - stderr
        - localfile, using the rotating file handler to avoid filling up disk space
        - cloudwatch

    Args:
        log: logger instance
        config: config dictionary
        cloudwatch_stream_name: stream name for cloudwatch logs


    """
    log_config = {}

    try:
        log_config.update(config['logging'])
    except KeyError:
        print("No logging section in conf file, setting default log configuration to stderr")
        log_config.update({'sinks': ['stderr'], 'min_level': 'WARN'})

    print('Configuring logging')
    print('Minimum loglevel set to {0}'.format(log_config['min_level']))
    log.setLevel(LOG_LEVELS.get(log_config['min_level'], 'INFO'))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if 'cloudwatch' in log_config['sinks']:
        print("Adding Cloudwatch log handler")
        log.addHandler(watchtower.CloudWatchLogHandler(
            log_group='snowdaemon-{0}'.format(config['snowplow']['service_name']),
            stream_name=cloudwatch_stream_name
        ))

    if 'localfile' in log_config['sinks']:
        print("Adding local file log handler")
        handler = RotatingFileHandler('snowdaemon.log', maxBytes=500000, backupCount=3)
        handler.setFormatter(formatter)
        log.addHandler(handler)

    if 'stderr' in log_config['sinks']:
        print("Adding stderr log handler")
        sh = StreamHandler()
        sh.setFormatter(formatter)
        log.addHandler(sh)

    print("Logging configuration complete, check logs for further messages...")


def sns_message(message, config, log_level='info'):
    """ Sends an SNS message to either the notification or error topic

    Args:
        message: string containing message
        config: dict containing configuration
        log_level: determines which topic to send message to either 'info' or 'error'.

    """
    level_details = {
        'info': ('notification', config['aws']['sns']['notification_arn']),
        'error': ('error', config['aws']['sns']['error_arn'])
    }

    msg_level, sns_arn = level_details[log_level]

    client = boto3.client('sns')
    client.publish(
        TargetArn=sns_arn,
        Message=message,
        Subject=f'Snowplow service {config["snowplow"]["service_name"]} {msg_level}'
    )


def download_service_config(config, log):
    """Downloads the configuration for the Snowplow service from an S3 bucket"""
    s3 = boto3.client('s3', region_name=config['aws']['aws_region'])
    file_key = '{0}/{1}'.format(
        config['snowplow']['service_name'],
        config['snowplow']['service_config_file'])

    local_file = '{0}/{1}'.format(
        os.getcwd(),
        config['snowplow']['service_config_file']
    )

    s3_bucket = config['aws']['s3_config_bucket']

    log.info('Downloading configuration from s3://{0}/{1}'.format(s3_bucket, file_key))
    with open(local_file, 'wb') as f:
        s3.download_fileobj(s3_bucket, file_key, f)
    log.info('Configuration downloaded successfully.')


def download_service_folder(config, log):
    """Downloads the specified folder for the Snowplow service from an S3 bucket

    Args:
        config: configuration dictionary
        log: logger object

    """
    s3 = boto3.resource('s3', region_name=config['aws']['aws_region'])
    bucket = s3.Bucket(config['aws']['s3_config_bucket'])
    remote_base_path = config['snowplow']['service_config_folder']

    for obj in bucket.objects.filter(Prefix=remote_base_path):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key)

    log.info('Configuration downloaded successfully.')


def parse_log_line(line):
    """Parses output from Snowplow logs

    Args:
        line: string containing log line

    Returns:
        tuple in format (loglevel, message)

    """
    def parse_dataflow_log_line():
        lvl_re = re.compile('level=([^ ]+)')
        msg_re = re.compile('msg="([^"]+)')
        try:
            lvl = lvl_re.search(line).group(1)
        except (IndexError, AttributeError):
            lvl = 'info'

        try:
            msg = msg_re.search(line).group(1)
        except (IndexError, AttributeError):
            msg = line

        return (
            LOG_LEVELS.get(lvl.upper(), 'INFO'),
            msg.strip('"')
        )

    if line[0:6] == '[main]':
        log_parts = line.split(' ', 2)
        level = LOG_LEVELS.get(log_parts[1], 'INFO')
        log_entry = level, log_parts[2]
    elif line[0:5] == 'Error':
        log_entry = (LOG_LEVELS['ERROR'], line[7:])
    elif line[0:5] == 'time=':
        log_entry = parse_dataflow_log_line()
    else:
        log_entry = (LOG_LEVELS['WARN'], line)

    return log_entry


def run(cmd_args, config, log):
    """ Runs the snowplow service and logs output continuously

    Runs the snowplow process as a subprocess, and captures log output
    to send to configured logging sinks

    Args:
        cmd_args: List of command args
        config: Config dictionary
        log: List of command args

    Returns:
        Exit code of process

    """
    service_name = config['snowplow']['service_name']
    log.info('Launching {0} service with command line:\n{1}'.format(service_name, ' '.join(cmd_args)))
    process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

    while True:
        output = process.stdout.readline()

        if process.poll() is not None and output == '':
            break
        if output:
            lvl, msg = parse_log_line(output.strip())
            log.log(lvl, msg)

    retval = process.poll()

    if retval == 0:
        msg = '{0} service has finished command with return code 0'.format(service_name)
        log.info(msg)
        sns_message(msg, config, 'info')
    else:
        msg = '{0} service has stopped with a non-zero return code: {1}'.format(service_name, retval)
        log.critical(msg)
        sns_message(msg, config, 'error')

    return retval
