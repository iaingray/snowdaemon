#
#  Copyright (c) 2020 Quantdeck Systems Ltd. All rights reserved.
#
#  This program is licensed to you under the Apache License Version 2.0, and
#  you may not use this file except in compliance with the Apache License
#  Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
#  http://www.apache.org/licenses/LICENSE-2.0.
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the Apache License Version 2.0 is distributed on an "AS
#  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied.  See the Apache License Version 2.0 for the specific language
#  governing permissions and limitations there under.
#
"""Functions specific to streaming services"""

from os import getcwd
import logging


_LOG = logging.getLogger(__name__)
_INSTANCE_ID = {'id': 'ID not set'}
_CONFIG = {}


def streaming_cmd(config):
    """ Creates command line argument to run Snowplow streaming service

    Args:
        config: configuration Dictionary

    Returns: List of command arguments ready to pass to subprocess.Popen()

    """
    ddb_params = (config['aws']['aws_region'], config['aws']['dynamodb_config_table'])

    cmd_args = [
        'java',
        '-jar',
        '{0}/{1}'.format(config['snowplow']['service_directory'], config['snowplow']['jarfile']),
        '--config',
        '{0}/{1}'.format(getcwd(), config['snowplow']['service_config_file'])
    ]

    if config['snowplow']['service_name'] == 'enrich':
        cmd_args.extend([
            '--resolver',
            'dynamodb:{0}/{1}/resolver'.format(*ddb_params),
            '--enrichments',
            'dynamodb:{0}/{1}/enrichment'.format(*ddb_params)
        ])

    return cmd_args
