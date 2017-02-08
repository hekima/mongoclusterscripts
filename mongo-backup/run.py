#!/usr/bin/env python
import mongoclusterbackup
#import mongoclusterbackup.run as run
import getopt
import sys, os
import traceback

import boto
import boto.utils

import time
import subprocess

import emailservice
import requests
import json

def run(cmd, capture_output=False):
    print("> %s" % cmd)
    if capture_output:
        return subprocess.check_output(cmd, shell=True)
    else:
        return subprocess.call(cmd, shell=True)

def wait_volume_status(vol, status):
    while vol.status != status:
        time.sleep(1)
        vol.update()
        print "Waiting for volume to become %s. status: %s"%(status, vol.status)

def usage():
    print "Usage: python run.py [-m|--mongos <mongos_host:mongos_port>] [-p|--path <backup+path>] [-s|--s3bucket <s3_bucket>] -v"

def main(argv):
    # mongos address, host:port
    mongos = '127.0.0.1:27017'
    # path to backup base directory
    backup_basedir = 'backup'
    # backup bucket in s3. Can be left as None, and no upload will be done
    # In this case, the local backup is NOT removed
    backup_bucket = None
    # Whether we should attach a volume or not
    should_attach = False
    # Graphite server to log events
    graphite = "http://localhost:8000"

    try:
        opts, args = getopt.getopt(argv, "hm:p:s:vg:", ["help", "mongos=", "path=", "s3bucket=", "volume", "graphite="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-m", "--mongos"):
            mongos = arg
        elif opt in ("-p", "--path"):
            backup_basedir = arg
        elif opt in ("-s", "--s3bucket"):
            backup_bucket = arg
        elif opt in ("-v", "--volume"):
            should_attach = True
        elif opt in ("-g", "--graphite"):
            graphite = arg

    if should_attach:
        ec2 = boto.connect_ec2()
        instance_id = boto.utils.get_instance_metadata()['instance-id']
        region = boto.utils.get_instance_metadata()['placement']['availability-zone']
        vol = ec2.create_volume(1024, region, volume_type="gp2")
        wait_volume_status(vol, 'available')
        vol.attach(instance_id, '/dev/sdh')
        wait_volume_status(vol, 'in-use')
        while not os.path.exists('/dev/xvdh'):
            time.sleep(1)
            print 'waiting xvdh'
        run("sudo mkfs -t ext4 /dev/xvdh")
        run("sudo mkdir -p %s"%backup_basedir)
        run("sudo chown ubuntu.ubuntu %s"%backup_basedir)
        run("sudo mount -t ext4 /dev/xvdh %s"%backup_basedir)

    exp = None
    try:
        backup = mongoclusterbackup.BackupCluster(mongos, backup_basedir, backup_bucket)
        backup.backup()
    except Exception as e:
        exp = e

    if should_attach:
        run("sudo umount %s"%backup_basedir)
        vol.detach(force=True)
        wait_volume_status(vol, 'available')
        vol.delete()
    if exp is not None:
        raise exp

    graphite_url = "%s/events/" % graphite
    graphite_post_data = json.dumps({"what": "Backup successful: %s"% backup.backup_id, "tags" : "backup"})
    graphite_response = requests.post(graphite_url, graphite_post_data)
    graphite_code = graphite_response.status_code
    if graphite_code < 200 or graphite_code > 299:
        emailservice.send_email("no-reply@zahpee.com", "production Backup successful, but could not log to graphite", "success", ["system-notifications@zahpee.com"])

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except Exception as e:
        t = traceback.format_exc()
        traceback.print_exc()
        emailservice.send_email("no-reply@zahpee.com", "production Mongo backup failed!", str(e)+"\n\n"+t, ["system-notifications@zahpee.com"])
        raise e
#python run.py -v -p /mnt/backup/ -g "http://graphite1:8000" -s zahpee-automatic-backup >& output &
