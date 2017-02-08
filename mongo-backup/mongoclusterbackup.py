#!/usr/bin/env python
import logging
import pymongo
import time
import subprocess
import random
import os
import signal
import datetime
import threading
import Queue
import traceback

import xmlrpclib
import s3upload

""" Create consistent backup of your mongodb sharded cluster.

    This basically follows mongodb shard backup procedure described in mongodb
    documentation (http://bit.ly/11uNuYa). It:
        - stops cluster balancer;
        - stops one of configuration servers to prevent metadata changes, and starts it on a different port
        - stops one secondary for each shard, and starts it on a different port
        - backs up configuration database and the shards on the maintenance nodes we just started
          The backup is done using mongodump. The result is zipped, and can be automatically uploaded to S3
        - Stops the maintenance nodes, and puts them back in the cluster. The secondaries are also put in
          maintenance mode (db.adminCommand({replSetMaintenance:true}), so they can catch up without impairing
          the cluster performance. After they catch up, we unset the maintenance mode
        - Renables the balancer.

    We have a LocalBackupMongo implementation, that assumes all nodes are running in the local machine, and can be started and stopped by the user running this script.
    We also have a AWS implementation, that assumes the following:
    - Each node (replica set or config server) has an instance in the AWS cloud, that is managed by supervisor
    - Each supervisor configuration has a configuration for the node in normal execution mode,
      and a configuration for the node in maintenance mode, outside of the cluster
    - The security groups are configured in such a way that all nodes have access to the other nodes
      (typically, ports 27017, 27018, 27019), the nodes in maintenance mode (ports 47018, 47019)
      and the supervisor (port 9001)
"""

logging.basicConfig(format="%(asctime)-15s %(message)s", level=logging.INFO)

class BackupAbortedException(Exception): pass

def run(cmd, capture_output=False):
    logging.info("> %s" % cmd)
    if capture_output:
        return subprocess.check_output(cmd, shell=True)
    else:
        return subprocess.call(cmd, shell=True)

class AbstractBackupMongo:

    """ Superclass for all mongo servers """

    def __init__(self, host, name, can_restart, backup_id, backup_path, backup_bucket=None):
        self.backup_id = backup_id
        self.backup_path = backup_path
        self.backup_bucket = backup_bucket
        self.can_restart = can_restart
        host_parts = host.split(":")
        self.host = host_parts[0]
        self.port = 27017 if len(host_parts) == 0 else int(host_parts[1])
        self.maintenance_port = self.port
        self.name = name
        logging.info("Initializing %s(%s:%d)" % (name, self.host, self.port))
        self.client = pymongo.MongoClient(self.host, self.port)

    def _mongodump(self, port, errors):
        """ Dump the database using mongodump. """
        ret = run("mongodump %s --forceTableScan --host %s:%d -o %s/%s" % ("--oplog" if self.name != "config" and self.name != "mongos" and not self.can_restart else "", self.host, port, self.backup_path, self.name))
        if ret != 0:
            errors.put(Exception("Error dumping %s server" % self.name))
            traceback.print_exc()
            return

        ret = run("cd %s && tar zcvf %s.tar.gz %s && rm -rf %s" % (self.backup_path, self.name, self.name, self.name))
        if ret != 0:
            errors.put(Exception("Error zipping %s server backup" % self.name))
            traceback.print_exc()

    def _upload(self, errors):
        """ Uploads the backup to the S3 bucket,
        and removes the local files
        """
        if self.backup_bucket is None:
            return

        try:
            with open("%s/%s.tar.gz"%(self.backup_path, self.name), 'r+') as f:
                s3upload.upload_to_s3(f,
                    self.backup_bucket,
                    "%s/%s.tar.gz"%(self.backup_id, self.name))

            # Cleaning up resources, since the upload was successful
            run("rm -f %s/%s.tar.gz"%(self.backup_path, self.name))
        except Exception as e:
            logging.exception(e)
            errors.put(Exception("Error uploading %s server backup to S3" % self.name))
            traceback.print_exc()


    def _wait_secondaries_catch_up(self):
        """ Waits for the secondaries to catch up, putting them in maintenance mode,
        and removing them once the replication lag is low enough
        """

        # If this is a config server, we're done. Else, we have to wait for the secondary to catch up
        if self.name == 'config':
            logging.info("Config server does not need to check replication lag, finishing")
            return

        # Setting maintenance mode to True, and waiting for the secondary to catch up
        self.client['admin'].command({'replSetMaintenance' : True})
        while True:
            members = self.client['admin'].command('replSetGetStatus')['members']
            primary = None
            self_member = None
            most_recent_optime = datetime.datetime.fromtimestamp(0)
            for m in members:
                if m['state'] == 1:
                    primary = m
                if 'self' in m and m['self']:
                    self_member = m
                if m['optimeDate'] > most_recent_optime:
                    most_recent_optime = m['optimeDate']
            if primary is not None:
                most_recent_optime = primary['optimeDate']
            repl_lag = most_recent_optime - self_member['optimeDate']
            if repl_lag.seconds < 2:
                logging.info("Replication lag for secondary %s is %d seconds, finishing" % (self_member['name'], repl_lag.seconds))
                self.client['admin'].command({'replSetMaintenance' : False})
                break
            logging.info("Replication lag for secondary %s is %d seconds, waiting a bit more" % (self_member['name'], repl_lag.seconds))
            time.sleep(0.5)



    def mongodump(self, errors):
        self._mongodump(self.maintenance_port, errors)
        self._upload(errors)

    def prepare_maintenance(self, errors):
        pass

    def finish_maintenance(self, errors):
        pass


class AWSBackupMongo(AbstractBackupMongo):
    def __init__(self, host, name, can_restart, backup_id, backup_path, backup_bucket=None):
        AbstractBackupMongo.__init__(self, host, name, can_restart, backup_id, backup_path, backup_bucket)
        self.supervisor = xmlrpclib.Server("http://%s:9001/RPC2"%(self.host)).supervisor
        if name == 'config':
            self.supervisor_name = 'mongo-configserver'
        else:
            self.supervisor_name = 'mongo-shard'

    def prepare_maintenance(self, errors):
        if not self.can_restart:
            return

        try:
            self.client.close()

            self.supervisor.stopProcess(self.supervisor_name)
            self.supervisor.startProcess(self.supervisor_name + '-maintenance')

            self.maintenance_port = self.port + 20000
            self.client = pymongo.MongoClient(self.host, self.maintenance_port)
        except e:
            errors.put(e)
            traceback.print_exc()

    def finish_maintenance(self, errors):
        if not self.can_restart:
            return

        try:
            self.client.close()

            self.supervisor.stopProcess(self.supervisor_name + '-maintenance')
            self.supervisor.startProcess(self.supervisor_name)

            self.client = pymongo.MongoClient(self.host, self.port)
            self._wait_secondaries_catch_up()
        except e:
            errors.put(e)
            traceback.print_exc()

class LocalBackupMongo(AbstractBackupMongo):
    def __init__(self, host, name, can_restart, backup_id, backup_path, backup_bucket=None):
        AbstractBackupMongo.__init__(self, host, name, can_restart, backup_id, backup_path, backup_bucket)

    def _shutdown(self):
        dbpath = self.cmd_line_opts['parsed']['storage']['dbPath']
        fname = dbpath+'/mongod.lock'
        with open(fname, 'r') as f:
            pid = int(f.read())
        self.client.close()
        os.kill(pid, signal.SIGTERM)
        retries = 100
        while os.path.isfile(fname) and os.stat(fname).st_size > 0:
            logging.info("checking if lock still exists: %s"%fname)
            time.sleep(0.5)
            retries = retries - 1
            if retries <= 0:
                raise Exception("Could not shutdown %s"%(" ".join(self.cmd_line_opts['argv'])))


    def prepare_maintenance(self, errors):
        """ We'll stop the server, and restart it on a different port
        """
        self.cmd_line_opts = self.client['admin'].command('getCmdLineOpts')

        if not self.can_restart:
            return
        port = 27017
        specified = False
        repl_index = None
        new_cmd_line = self.cmd_line_opts['argv'][:]
        for i in range(len(new_cmd_line)):
            if new_cmd_line[i] == '--port':
                logging.info(str(new_cmd_line))
                self.maintenance_port = int(new_cmd_line[i+1]) + 20000
                new_cmd_line[i+1] = str(self.maintenance_port)
                specified = True
            if new_cmd_line[i] == '--replSet':
                repl_index = i
        if not specified:
            new_cmd_line.append('--port')
            new_cmd_line.append('47017')
        if repl_index is not None:
            del new_cmd_line[repl_index+1]
            del new_cmd_line[repl_index]
        try:
            self._shutdown()
        except Exception as e:
            errors.put(e)
            traceback.print_exc()
            return
        run(" ".join(new_cmd_line))
        self.client = pymongo.MongoClient(self.host, self.maintenance_port)

    def finish_maintenance(self, errors):
        """ We'll shutdown the maintenance server, and start it on the correct port
        """
        if not self.can_restart:
            return

        try:
            self._shutdown()
            run(" ".join(self.cmd_line_opts['argv']))
            self.client = pymongo.MongoClient(self.host, self.port)
            self._wait_secondaries_catch_up()
        except Exception as e:
            errors.put(e)
            traceback.print_exc()


# Here, we choose our implementation
class BackupMongo(LocalBackupMongo): pass

class BackupMongos(BackupMongo):

    """ Mongos instance. We will use it to stop/start balancer and wait for
        any locks.
    """

    def get_shards(self):
        shards = {}
        for shard in self.client['config']['shards'].find():
            if '/' in shard['host']:
                # This shard is a replicaset. Connect to it and find a healthy
                # secondary host with minimal replication lag
                shard_name = shard['host'].split('/')[0]
                hosts = shard['host'].split('/')[1].split(',')
                chosen = 'invalid'
                with pymongo.MongoClient(hosts) as connection:
                    rs = connection['admin'].command("replSetGetStatus")
                    good_secondaries = [member for member in rs['members']
                            if member['state'] == 2 and int(member['health'])]
                    if len(good_secondaries):
                        best = sorted(good_secondaries,
                                      key=lambda x: x['optimeDate'],
                                      reverse=True)[0]
                        chosen = best['name']
                    else:
                        # no healthy secondaries found, try to find the master
                        master = [member for member in rs['members']
                                  if member['state'] == 1][0]
                        chosen = master['name']
                    shards[shard_name] = (chosen, len(hosts)> 1)
            else:
                # standalone server rather than a replicaset
                shards[shard['host']] = (shard['host'], False)
        return shards

    def get_locks(self):
        return [lock
                for lock in self.client['config']['locks'].find({"state": 2})]

    def balancer_stopped(self):
        return self.client['config']['settings'].find({
            "_id": "balancer"
        })[0]["stopped"]

    def stop_balancer(self):
        logging.info("Stopping balancer")
        self.client['config']['settings'].update(
            {"_id":"balancer"}, {"$set": { "stopped": True }}, True)
        if not self.balancer_stopped():
            raise Exception("Could not stop balancer")

    def start_balancer(self):
        logging.info("Starting balancer")
        self.client['config']['settings'].update(
            {"_id":"balancer"}, {"$set": { "stopped": False }}, True)
        if self.balancer_stopped():
            raise Exception("Could not start balancer")

    def get_config_servers(self):
        cmd_line_opts = self.client['admin'].command('getCmdLineOpts')
        if 'sharding' in cmd_line_opts['parsed']:
            return cmd_line_opts['parsed']['sharding']['configDB'].split(',')
        else:
            return []


class BackupCluster:

    """ Main class that does all the hard work """

    def __init__(self, mongos, backup_basedir, backup_bucket=None):
        """ Initialize all children objects, checking input parameters
            sanity and verifying connections to various servers/services
        """
        logging.info("Initializing BackupCluster")
        self.backup_id = self.generate_backup_id()
        self.backup_path = os.path.join(backup_basedir, self.backup_id)
        self.backup_bucket = backup_bucket
        run("mkdir -p %s" % self.backup_path)
        logging.info("Backup ID is %s" % self.backup_id)
        self.mongos = BackupMongos(mongos, 'mongos', False, self.backup_id, self.backup_path, self.backup_bucket)
        shards = self.mongos.get_shards()
        if len(shards) > 0:
            self.shards = [BackupMongo(shards[shard][0], shard, shards[shard][1], self.backup_id, self.backup_path, self.backup_bucket) for shard in shards]
        else:
            self.shards = [self.mongos]
        config_servers = self.mongos.get_config_servers()
        if len(config_servers) > 0:
            self.config_server = BackupMongo(
                                    config_servers[-1],
                                    'config',
                                    len(config_servers) > 1,
                                    self.backup_id,
                                    self.backup_path,
                                    self.backup_bucket
                                 )
        else:
            self.config_server = None
        self.rollback_steps = []

    def generate_backup_id(self):
        """ Generate unique time-based backup ID that will be used both for
            configuration server backups and LVM snapshots of shard servers
        """
        ts = datetime.datetime.now()
        return ts.strftime('%Y%m%d-%H%M%S')

    def wait_for_locks(self):
        """ Loop until all shard locks are released.
            Give up after 30 minutes.
        """
        retries = 0
        while len(self.mongos.get_locks()) and retries < 360:
            logging.info("Waiting for locks to be released: %s" %
                         self.mongos.get_locks())
            time.sleep(5)
            retries += 1

        if len(self.mongos.get_locks()):
            raise Exception("Something is still locking the cluster,"
                            " aborting backup")

    def prepare_shards_maintenance(self):
        """ Sets the maintenance mode for all shards
            As we would like to minimize the amount of time
            the cluster stays locked, we lock each shard in a separate thread.
            The queue is used to pass any errors back from the worker threads.
        """
        errors = Queue.Queue()
        threads = []
        for shard in self.shards:
            t = threading.Thread(target=shard.prepare_maintenance, args=(errors,))
            threads.append(t)
        if self.config_server is not None:
            t = threading.Thread(target=self.config_server.prepare_maintenance, args=(errors,))
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise errors.get()

    def backup_dump(self):
        """ Creates a mongodump backup for each shard and the config server.
            The cluster is supposed to be
            in maintenance mode at this point, so we use a separate thread for
            each server to create all the snapshots as fast as possible.
            The queue is used to pass any errors back from the worker threads.
        """
        errors = Queue.Queue()
        threads = []
        for host in self.shards:
            t = threading.Thread(target=host.mongodump, args=(errors,))
            threads.append(t)
        if self.config_server is not None:
            t = threading.Thread(target=self.config_server.mongodump, args=(errors,))
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise Exception(errors.get())

    def finish_shards_maintenance(self):
        """ Unsets the maintenance mode for the shards.
        """

        errors = Queue.Queue()
        threads = []
        for shard in self.shards:
            t = threading.Thread(target=shard.finish_maintenance, args=(errors,))
            threads.append(t)
        if self.config_server is not None:
            t = threading.Thread(target=self.config_server.finish_maintenance, args=(errors,))
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not errors.empty():
            # We don't really care for all errors, so just through the first one
            raise Exception(errors.get())

    def run_step(self, function, tries=1):
        """ Try executing a function, retrying up to `tries` times if it fails
            with an exception. If the function fails after all tries, roll back
            all the changes - basically, execute all steps from rollback_steps
            ignoring any exceptions. Hopefully, this should bring the cluster
            back into pre-backup state.
        """
        for i in range(tries):
            try:
                logging.debug("Running %s (try #%d)" % (function, i+1))
                function()
                break
            except Exception as e:
                logging.info("Got an exception (%s) while running %s" %
                             (e, function))
                traceback.print_exc()
                if (i == tries-1):
                    logging.info("Rolling back...")
                    for step in self.rollback_steps:
                        try:
                            step()
                        except (Exception, pymongo.errors.OperationFailure) as e:
                            logging.info("Got an exception (%s) while rolling"
                                         " back (step %s). Ignoring" %
                                         (e, step))
                    raise BackupAbortedException
                time.sleep(2)  # delay before re-trying

    def backup(self):
        """ This is basically a runlist of all steps required to backup a
            cluster. Before executing each step, a corresponding rollback
            function is pushed into the rollback_steps array to be able to
            get cluster back into production state in case something goes
            wrong.
        """
        self.rollback_steps.insert(0, self.mongos.start_balancer)
        self.run_step(self.mongos.stop_balancer, 2)

        self.run_step(self.wait_for_locks)

        self.rollback_steps.insert(0, self.finish_shards_maintenance)
        self.run_step(self.prepare_shards_maintenance)

        self.run_step(self.backup_dump)

        self.rollback_steps.remove(self.finish_shards_maintenance)
        self.run_step(self.finish_shards_maintenance, 2)

        self.rollback_steps.remove(self.mongos.start_balancer)
        self.run_step(self.mongos.start_balancer, 4)  # it usually starts on
                                                      # the second try

        if self.backup_bucket is not None:
            run("rmdir %s" % self.backup_path)

        logging.info("Finished successfully")
