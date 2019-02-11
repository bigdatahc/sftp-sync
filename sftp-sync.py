import os
import sys
import json
import pickle
import zipfile
import argparse
import paramiko
import requests
import configparser
from datetime import date

REQUIRED_CONFIG_SECTIONS = ('source', 'dest', 'main')


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transfer files via SFTP",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("config")
    parser.add_argument("--dry-run", action='store_true', default=False,
                        help="Print what would happen, but don't transfer anything")

    return parser.parse_args()


def get_config(conf_file):
    config = configparser.ConfigParser(interpolation=None)
    config.read(conf_file)

    for section in REQUIRED_CONFIG_SECTIONS:
        if section not in config:
            print("ERROR: `{}` must be defined in config file ({})".format(section, conf_file))
            sys.exit(1)

    if 'name' not in config['main']:
        print('ERROR: Must define `name` in [main]')
        sys.exit(1)

    if 'archive_dir' not in config['main']:
        print('ERROR: Must define `archive_dir` in [main]')
        sys.exit(1)

    return config


class SftpSync:

    default_port = 22

    def __init__(self, config, dry_run=False):
        self.config = config
        self.state_file = '.{}.pickle'.format(config['main']['name'])
        self.zip = bool(config['main'].get('zip'))

        self.source = self.get_sftp_connection(config['source'])
        self.dest = self.get_sftp_connection(config['dest'])

        self.dry_run = dry_run
        self.file_details = {}

    def _validate_sftp_config(self, config):
        for key in ('HOST', 'USER', 'PASS'):
            if key not in config:
                print("ERROR: Missing key `{}`".format(key))
                sys.exit(1)

        # configparser requires storing as a string
        config['PORT'] = str(self._validate_port(config))
        return config

    def _validate_port(self, config):
        port = self.default_port
        if 'PORT' in config:
            try:
                int(config['PORT'])
            except (TypeError, ValueError):
                print("ERROR: PORT must be a number")
                sys.exit(1)

        return port

    def get_sftp_connection(self, config):
        self._validate_sftp_config(config)

        transport = paramiko.Transport((config['HOST'], int(config['PORT'])))
        transport.connect(
            username=config['USER'],
            password=config['PASS']
        )
        sftp = paramiko.SFTPClient.from_transport(transport)

        if config.get('DIR'):
            sftp.chdir(config['DIR'])

        return sftp

    def load_state(self):
        try:
            with open(self.state_file, 'rb') as fd:
                return pickle.load(fd)
        except FileNotFoundError:
            return []

    def store_state(self, files):
        with open(self.state_file, 'wb') as fd:
            pickle.dump(files, fd)

    @property
    def archive(self):
        return bool(self.config.get('archive_dir'))

    def transfer(self):
        transferred = self.load_state()
        source_files = self.read_source_files(self.source)

        diff = set(source_files) - set(transferred)
        local_files = []
        print("Found {} files to transfer.".format(len(diff)))
        for filename in diff:
            if self.dry_run:
                print("Would transfer {}".format(filename))
            else:
                if self.zip:
                    local_files.append(self.download_file(filename))
                else:
                    self.transfer_file(filename)
                    transferred.append(filename)
                    if self.archive:
                        self.archive_file(filename)

        if diff and self.zip:
            self.transfer_zip(local_files, diff)
            transferred.extend(diff)
            if self.archive:
                for filename in diff:
                    self.archive_file(filename)

        self.store_state(transferred)

    def archive(self, filename):
        archive_filename = os.path.join(self.config['archive_dir'], filename)
        self.source.rename(filename, archive_filename)

    def read_source_files(self, sftp):
        files = sftp.listdir_attr()
        for file in files:
            if file.st_size:
                self.file_details[file.filename] = file

        return self.file_details.keys()

    def download_file(self, filename):
        localpath = os.path.join(self.config['main']['archive_dir'], filename)
        self.source.get(filename, localpath)
        return localpath

    def transfer_zip(self, local_files, filenames):
        isodate = date.today().strftime('%Y-%m-%d')
        zip_filename = '{}-{}.zip'.format(self.config['main']['name'], isodate)
        zip_path = os.path.join(self.config['main']['archive_dir'], zip_filename)
        with zipfile.ZipFile(zip_path, 'w') as myzip:
            for file in local_files:
                myzip.write(file, os.path.basename(file))

        self.dest.put(zip_path, zip_filename, confirm=True)
        msg = 'Transferred {}\n```Contains:'.format(zip_filename)
        for filename in filenames:
            msg += '\n    - {} ({} bytes)'.format(filename, self.file_details[filename].st_size)
        msg += '```'
        self.notify(zip_filename, msg)

    def transfer_file(self, filename):
        localpath = self.download_file(filename)
        self.dest.put(localpath, filename, confirm=True)
        self.notify(filename)

    def notify(self, filename, message=None):
        if self.config['main'].get('slack'):
            if not message:
                message = 'Transferred {} ({} bytes)'.format(filename, self.file_details[filename].st_size)
            payload = json.dumps({'text': message})
            requests.post(self.config['main']['slack'], data=payload)


def main():
    args = parse_args()
    if args.dry_run:
        print("--dry-run specified.  Nothing will be transferred\n")

    config = get_config(args.config)
    sftp_sync = SftpSync(config, args.dry_run)
    sftp_sync.transfer()


if __name__ == '__main__':
    main()
