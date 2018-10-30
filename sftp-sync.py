import io
import sys
import json
import argparse
import paramiko
import requests
import configparser


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transfer files via SFTP",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("config")
    parser.add_argument("--dry-run", "dryrun",
                        help="Print what would happen, but don't transfer anything")

    return parser.parse_args()


def get_config(conf_file):
    config = configparser.ConfigParser()
    config.read(conf_file)

    if 'source' not in config or 'dest' not in config:
        print("ERROR: `source` and `dest` sections must be defined in config file ({})".format(conf_file))
        sys.exit(1)

    return config


class SftpSync:

    default_port = 22
    
    def __init__(self, source, dest, dry_run=False, hooks=None):
        self.source = self.get_sftp_connection(source)
        self.dest = self.get_sftp_connection(dest)
        self.dry_run = dry_run
        self.hooks = hooks
        self.file_details = {}

    def _validate_config(self, config):
        for key in ('HOST', 'USER', 'PASS'):
            if key not in config:
                print("ERROR: Missing key `{}`".format(key))
                sys.exit(1)

        config['PORT'] = self._validate_port(config)
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
        self._validate_config(config)

        transport = paramiko.Transport((config['HOST'], config['PORT']))
        transport.connect(
            username=config['USER'],
            password=config['PASS']
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        if config.get('DIR'):
            sftp.chdir(config['DIR'])

        return sftp

    def sync(self):
        source_files = self.read_files(self.source, store_details=True)
        dest_files = self.read_files(self.dest)

        diff = set(source_files) - set(dest_files)
        for filename in diff:
            if self.dry_run:
                print("Would transfer {} (but --dry-run enabled, nothing will be transferred").format(filename)
            else:
                self.transfer_file(filename)

    def read_files(self, sftp, store_details=False):
        files = sftp.listdir_attr()
        filenames = []
        for file in files:
            filenames.append(file.filename)
            if store_details and file.filename not in self.file_details:
                self.file_details[file.filename] = file
                
        return filenames

    def transfer_file(self, filename):
        # Currently doing the transfer in memory.
        # For huge files we need to change this to use the disk.
        flo = io.BytesIO()
        self.source.getfo(filename, flo)
        self.dest.putfo(flo, filename, confirm=True)

        self.notify(filename)

    def notify(self, filename):
        if self.hooks.get('slack'):
            message = 'Transferred {} ({} bytes))'.format(filename, self.file_details[filename].st_size)
            payload = json.dumps({'text': message})
            requests.post(self.hooks['slack'], data=payload)


def main():
    args = parse_args()
    config = get_config(args.config)
    sftp_sync = SftpSync(config['source'], config['dest'], config.get['hooks'])
    sftp_sync.sync()
    

if __name__ == '__main__':
    main()
