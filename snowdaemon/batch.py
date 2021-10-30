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

"""Functions specific to running Snowplow batch jobs"""
import logging
import os
import boto3

_LOG = logging.getLogger(__name__)
_INSTANCE_ID = {'id': 'ID not set'}
_CONFIG = {}


def snowflake_ldr_cmd(config):
    """Creates command line argument to run snowflake loader service using dataflow runner

    Args:
        config: configuration Dictionary

    Returns: List of command arguments ready to pass to subprocess.Popen()

    """
    return [
        f'{config["snowplow"]["service_directory"]}/dataflow-runner',
        'run-transient',
        '--emr-config',
        './cluster.json',
        '--emr-playbook',
        './playbook.json'
    ]


def export_aws_creds():
    """Export AWS Credentials to session for EMR cluster jobs"""
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()

    os.environ['AWS_ACCESS_KEY_ID'] = creds.access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = creds.secret_key
    os.environ['AWS_SESSION_TOKEN'] = creds.token
