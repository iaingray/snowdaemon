#
#  Copyright (c) 2021 Devdrop Ltd. All rights reserved.
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

""" Helper service to faciliate running Snowplow realtime microservices and batch jobs

This wrapper provides for both scala realtime microservices and dataflow runner jobs to
be configured and managed.

It downloads the configuration file/folder from an S3 bucket, and runs the service using the
supplied configuration. It provides for logging to a local file, stderr and/or cloudwatch,
and also provides status notifications via SNS.

  Typical usage example:

  $ python -m snowdaemon --config CONFIG_FILE_LOCATION
"""

import argparse
import logging
import os
import sys
import yaml
import snowdaemon as sd


DEFAULT_CONFIG_PATH = './snowdaemon/config.yaml'

_CONFIG = {}
_INSTANCE_ID = {'id': 'not_set'}
_LOG = logging.getLogger(__name__)


def run_service():
    """Downloads service file/folder depending on the service requirements, and runs the service.

    For the dataflow runner-based Snowflake loader, it downloads the whole config folder, chdir()s into it,
    and exports some AWS creds ready for the dataflow runner to use

    Returns: exit code of process
    """
    service_name = _CONFIG['snowplow']['service_name']
    if service_name == 'snowflake_loader':
        sd.download_service_folder(_CONFIG, _LOG)
        cmd_args = sd.snowflake_ldr_cmd(_CONFIG)
        sd.export_aws_creds()
        os.chdir(_CONFIG['snowplow']['service_config_folder'])
    else:
        sd.download_service_config(_CONFIG, _LOG)
        cmd_args = sd.streaming_cmd(_CONFIG)

    return sd.run(cmd_args, _CONFIG, _LOG)


def init(config, set_instance_id=True, set_logging_config=True):
    """Initialises the module with the supplied config

    Args:
        config - dict containing configuration data
        set_instance_id - boolean default True, does not set instance id if False
        set_instance_id - boolean default True, does not configure logging if False
    """
    os.environ['AWS_DEFAULT_REGION'] = config['aws']['aws_region']
    _CONFIG.update(config)
    if set_instance_id:
        _INSTANCE_ID['id'] = sd.get_instance_id()
    if set_logging_config:
        sd.configure_logging(_LOG, _CONFIG, _INSTANCE_ID['id'])


def main():
    """Runs the snowplow service as per supplied config file
    """
    parser = argparse.ArgumentParser(
        prog='snowdaemon',
        usage='python -m %(prog)s [options]',
        description='Configure and run a Snowplow service')
    parser.add_argument("--config", help='path to Snowdaemon config file. Default {0}'.format(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()
    conf_file_path = args.config if args.config else DEFAULT_CONFIG_PATH

    try:
        with open(conf_file_path, 'rb') as f:
            config = yaml.full_load(f)
    except FileNotFoundError:
        print("Cannot find config file at {0}".format(conf_file_path))
        sys.exit(1)

    init(config)
    rtn = run_service()
    sys.exit(rtn)


if __name__ == '__main__':
    main()
