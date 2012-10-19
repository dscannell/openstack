# Copyright 2011 GridCentric Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Handles all processes relating to GridCentric functionality

The :py:class:`GridCentricManager` class is a :py:class:`nova.manager.Manager` that
handles RPC calls relating to GridCentric functionality creating instances.
"""

import time
import threading
import traceback
import os
import re
import socket
import subprocess

from nova import exception
from nova import flags
from nova import log as logging
LOG = logging.getLogger('nova.gridcentric.manager')
FLAGS = flags.FLAGS
flags.DEFINE_bool('gridcentric_use_image_service', False,
                  'Gridcentric should use the image service to store disk copies and descriptors.')
flags.DEFINE_string('gridcentric_outgoing_migration_address', None,
                    'IPv4 address to host migrations from; the VM on the '
                    'migration destination will connect to this address. '
                    'Must be in dotted-decimcal format, i.e., ddd.ddd.ddd.ddd. '
                    'By default, the outgoing migration address is determined '
                    'automatically by the host\'s routing tables.')

from nova import manager
from nova import utils
from nova import rpc
from nova import network

# We need to import this module because other nova modules use the flags that
# it defines (without actually importing this module). So we need to ensure
# this module is loaded so that we can have access to those flags.
from nova.network import manager as network_manager
from nova.compute import power_state
from nova.compute import task_states
from nova.compute import vm_states
from nova.compute import manager as compute_manager

from nova.notifier import api as notifier

from gridcentric.nova.api import API
import gridcentric.nova.extension.vmsconn as vmsconn

def memory_string_to_pages(mem):
    mem = mem.lower()
    units = { '^(\d+)tb$' : 40,
              '^(\d+)gb$' : 30,
              '^(\d+)mb$' : 20,
              '^(\d+)kb$' : 10,
              '^(\d+)b$' : 0,
              '^(\d+)$' : 0 }
    for (pattern, shift) in units.items():
        m = re.match(pattern, mem)
        if m is not None:
            val = long(m.group(1))
            memory = val << shift
            # Shift to obtain pages, at least one
            return max(1, memory >> 12)
    raise ValueError('Invalid target string %s.' % mem)

class GridCentricManager(manager.SchedulerDependentManager):

    def __init__(self, *args, **kwargs):
        self.vms_conn = None
        self._init_vms()
        self.network_api = network.API()
        self.gridcentric_api = API()
        self.compute_manager = compute_manager.ComputeManager()
        self.cond = threading.Condition()
  
        self.outgoing_migration = {}
        self.incoming_migration = {}
  
        super(GridCentricManager, self).__init__(service_name="gridcentric", *args, **kwargs)

    def _init_vms(self):
        """ Initializes the hypervisor options depending on the openstack connection type. """
        connection_type = FLAGS.connection_type
        self.vms_conn = vmsconn.get_vms_connection(connection_type)
        self.vms_conn.configure()

    def _instance_update(self, context, instance_id, **kwargs):
        """Update an instance in the database using kwargs as value."""
        return self.db.instance_update(context, instance_id, kwargs)

    def _extract_image_refs(self, metadata):
        image_refs = metadata.get('images', '').split(',')
        if len(image_refs) == 1 and image_refs[0] == '':
            image_refs = []
        return image_refs

    def _get_source_instance(self, context, instance_id):
        """ 
        Returns a the instance reference for the source instance of instance_id. In other words:
        if instance_id is a BLESSED instance, it returns the instance that was blessed
        if instance_id is a LAUNCH instance, it returns the blessed instance.
        if instance_id is neither, it returns NONE.
        """
        metadata = self.db.instance_metadata_get(context, instance_id)
        if "launched_from" in metadata:
            source_instance_id = int(metadata["launched_from"])
        elif "blessed_from" in metadata:
            source_instance_id = int(metadata["blessed_from"])
        else:
            source_instance_id = None

        if source_instance_id != None:
            return self.db.instance_get(context, source_instance_id)
        return None

    def bless_instance(self, context, instance_id, migration_url=None):
        """
        Construct the blessed instance, with the id instance_id. If migration_url is specified then 
        bless will ensure a memory server is available at the given migration url.
        """
        LOG.debug(_("bless instance called: instance_id=%s, migration_url=%s"),
                    instance_id, migration_url)

        instance_ref = self.db.instance_get(context, instance_id)

        if migration_url:
            # Tweak only this instance directly.
            source_instance_ref = instance_ref
            migration = True
        else:
            source_instance_ref = self._get_source_instance(context, instance_id)
            migration = False

        try:
            # Create a new 'blessed' VM with the given name.
            name, migration_url, blessed_files = self.vms_conn.bless(context,
                                                source_instance_ref.name,
                                                instance_ref,
                                                migration_url=migration_url,
                                                use_image_service=FLAGS.gridcentric_use_image_service)
            if not(migration):
                usage_info = utils.usage_from_instance(instance_ref)
                notifier.notify('gridcentric.%s' % self.host,
                                'gridcentric.instance.bless',
                                notifier.INFO, usage_info)
                self._instance_update(context, instance_ref.id,
                                  vm_state="blessed", task_state=None,
                                  launched_at=utils.utcnow())
        except Exception, e:
            LOG.debug(_("Error during bless %s: %s"), str(e), traceback.format_exc())
            self._instance_update(context, instance_ref.id,
                                  vm_state=vm_states.ERROR, task_state=None)
            # Short-circuit, nothing to be done.
            return

        # Mark this new instance as being 'blessed'.
        metadata = self.db.instance_metadata_get(context, instance_ref.id)
        LOG.debug("blessed_files = %s" % (blessed_files))
        metadata['images'] = ','.join(blessed_files)
        if not(migration):
            metadata['blessed'] = True
        self.db.instance_metadata_update(context, instance_ref.id, metadata, True)

        # Return the memory URL (will be None for a normal bless).
        return migration_url

    def _lock_migration(self, context, instance_id):
        self.cond.acquire()
        try:
            instance_ref = self.db.instance_get(context, instance_id)
            if instance_ref['vm_state'] != vm_states.MIGRATING:
                # We cannot lock a non migrating instance.
                return False
            if instance_id in self.outgoing_migration:
                # We are in the process of migrating this instance.
                return False
            # Grab a reference to the instance's metadata.
            metadata = self.db.instance_metadata_get(context, instance_id)
            if 'gc:migrating' in metadata:
                # This instance is already locked for migration. It's possible we are
                # the destination host and a new migration request was sent to us while
                # source was finishing off.
                return False
            metadata['gc:migrating'] = "true"
            self.db.instance_metadata_update(context, instance_id, metadata, True)
            self.outgoing_migration[instance_id] = True
        finally:
            self.cond.release()
        return True

    def _unlock_migration(self, context, instance_id):
        self.cond.acquire()
        try:
            # Grab a reference to the instance.
            metadata = self.db.instance_metadata_get(context, instance_id)
            if 'gc:migrating' in metadata:
                del metadata['gc:migrating']
            self.db.instance_metadata_update(context, instance_id, metadata, True)
            if instance_id in self.outgoing_migration:
                del self.outgoing_migration[instance_id]
        finally:
            self.cond.release()

    def _migration_reconfigure_networks(self, context, instance_id, dest):
        network_dest_queue = self.db.queue_get_for(context, FLAGS.network_topic, dest)
        network_source_queue = self.db.queue_get_for(context, FLAGS.network_topic, self.host)

        vifs = self.db.virtual_interface_get_by_instance(context, instance_id)
        for vif in vifs:
            network_ref = self.db.network_get(context, vif['network_id'])
            if network_ref['multi_host']:
                # This type of configuration only makes sense for a multi_host network where
                # the compute host is responsible for the networking of its instances. Otherwise,
                # there is a global set of network hosts performing the networking and there
                # is no need to reconfigure.
                rpc.call(context, network_dest_queue,
                         {"method":"_setup_network",
                          "args":{"network_ref":network_ref}})

                rpc.call(context, network_source_queue,
                         {"method":"_setup_network",
                          "args":{"network_ref":network_ref}})

    def migrate_instance(self, context, instance_id, dest):
        """
        Migrates an instance, dealing with special streaming cases as necessary.
        """
        LOG.debug(_("migrate instance called: instance_id=%s"), instance_id)

        if not self._lock_migration(context, instance_id):
            # This instance is in the middle of migrating so we cannot start another
            # migration.
            LOG.warn(_("Unable to migrate instance %s because it is currently being migrated."),
                       instance_id)
            return

        try:
            self.do_migrate_instance(context, instance_id, dest)
        finally:
            self._unlock_migration(context, instance_id)
            instance_ref = self.db.instance_get(context, instance_id)
            if instance_ref['vm_state'] == vm_states.MIGRATING:
                # Only update the state of the instance if it is migrating, otherwise the 
                # instance's state has been explicitly set, most likely to error, so we should
                # not change it.
                self._instance_update(context, instance_ref.id, vm_state=vm_states.ACTIVE)

    def do_migrate_instance(self, context, instance_id, dest):
        # FIXME: This live migration code does not currently support volumes,
        # nor floating IPs. Both of these would be fairly straight-forward to
        # add but probably cry out for a better factoring of this class as much
        # as this code can be inherited directly from the ComputeManager. The
        # only real difference is that the migration must not go through
        # libvirt, instead we drive it via our bless, launch routines.

        instance_ref = self.db.instance_get(context, instance_id)
        src = instance_ref['host']
        if instance_ref['volumes']:
            rpc.call(context,
                      FLAGS.volume_topic,
                      {"method": "check_for_export",
                       "args": {'instance_id': instance_id}})

        # Get a reference to both the destination and source queues
        gc_dest_queue = self.db.queue_get_for(context, FLAGS.gridcentric_topic, dest)
        compute_dest_queue = self.db.queue_get_for(context, FLAGS.compute_topic, dest)
        compute_source_queue = self.db.queue_get_for(context, FLAGS.compute_topic, self.host)

        rpc.call(context, compute_dest_queue,
                 {"method": "pre_live_migration",
                  "args": {'instance_id': instance_id,
                           'block_migration': False,
                           'disk': None}})

        # Figure out the interface to reach 'dest'.
        # This is used to construct our out-of-band network parameter below.
        dest_ip = socket.gethostbyname(dest)
        iproute = subprocess.Popen(["ip", "route", "get", dest_ip], stdout=subprocess.PIPE)
        (stdout, stderr) = iproute.communicate()
        lines = stdout.split("\n")
        if len(lines) < 1:
            raise exception.Error(_("Could not reach destination %s.") % dest)
        try:
            (destip, devstr, devname, srcstr, srcip) = lines[0].split()
        except:
            raise exception.Error(_("Could not determine interface for destination %s.") % dest)

        # Check that this is not local.
        if devname == "lo":
            raise exception.Error(_("Can't migrate to the same host."))

        # Grab the network info (to be used for cleanup later on the host).
        network_info = self.network_api.get_instance_nw_info(context, instance_ref)

        if FLAGS.gridcentric_outgoing_migration_address != None:
            migration_address = FLAGS.gridcentric_outgoing_migration_address
        else:
            migration_address = devname

        # Bless this instance for migration.
        migration_url = self.bless_instance(context, instance_id,
                                            migration_url="mcdist://%s" %
                                            migration_address)

        if migration_url == None:
            # If the migration url is None then that means there was an issue with the bless.
            # We cannot continue with the migration so we just exit.
            return

        # Run our premigration hook.
        self.vms_conn.pre_migration(context, instance_ref, network_info, migration_url)

        try:
            # Launch on the different host. With the non-null migration_url,
            # the launch will assume that all the files are the same places are
            # before (and not in special launch locations).
            #
            # FIXME: Currently we fix a timeout for this operation at 30 minutes.
            # This is a long, long time. Ideally, this should be a function of the
            # disk size or some other parameter. But we will get a response if an
            # exception occurs in the remote thread, so the worse case here is 
            # really just the machine dying or the service dying unexpectedly.
            rpc.call(context, gc_dest_queue,
                    {"method": "launch_instance",
                     "args": {'instance_id': instance_id,
                              'migration_url': migration_url}})

            # Teardown on this host (and delete the descriptor).
            metadata = self.db.instance_metadata_get(context, instance_id)
            image_refs = self._extract_image_refs(metadata)
            self.vms_conn.post_migration(context, instance_ref, network_info, migration_url,
                                         use_image_service=FLAGS.gridcentric_use_image_service,
                                         image_refs=image_refs)

            # Essentially we want to clean up the instance on the source host. This involves
            # removing it from the libvirt caches, removing it from the iptables, etc. Since we
            # are dealing with the iptables, we need the nova-compute process to handle this clean
            # up. We use the rollback_live_migration_at_destination method of nova-compute because
            # it does exactly was we need but we use the source host (self.host) instead of
            # the destination.
            rpc.call(context, compute_source_queue,
                 {"method": "rollback_live_migration_at_destination",
                  "args": {'instance_id': instance_id}})

            # This basically ensures that DHCP is configured and running on the dest host and
            # that the DHCP entries from the source host have been removed.
            self._migration_reconfigure_networks(context, instance_id, dest)

            self._instance_update(context,
                                  instance_ref.id,
                                  host=dest,
                                  task_state=None)

        except:
            # TODO(dscannell): This rollback is a bit broken right now because
            # we cannot simply relaunch the instance on this host. The order of
            # events during migration are: 1. Bless instance -- This will leave
            # the qemu process in a paused state, but alive 2. Clean up libvirt
            # state (need to see why it doesn't kill the qemu process) 3. Call
            # launch on the destination host and wait for the instance to hoard
            # its memory 4. Call discard that will clean up the descriptor and
            # kill off the qemu process Depending on what has occurred
            # different strategies are needed to rollback e.g We can simply
            # unpause the instance if the qemu process still exists (might need
            # to move when libvirt cleanup occurs).
            LOG.debug(_("Error during migration: %s"), traceback.format_exc())

            try:
                # Clean up the instance from both the source and destination.
                rpc.call(context, compute_source_queue,
                     {"method": "rollback_live_migration_at_destination",
                      "args": {'instance_id': instance_id}})
                rpc.call(context, compute_dest_queue,
                     {"method": "rollback_live_migration_at_destination",
                      "args": {'instance_id': instance_id}})

                # Prepare to relaunch here (this is the nasty bit as per above).
                metadata = self.db.instance_metadata_get(context, instance_id)
                image_refs = self._extract_image_refs(metadata)
                self.vms_conn.post_migration(context, instance_ref, network_info, migration_url,
                                             use_image_service=FLAGS.gridcentric_use_image_service,
                                             image_refs=image_refs)

                # Rollback is launching here again.
                self.launch_instance(context, instance_id, migration_url=migration_url)
                self._instance_update(context,
                                  instance_ref.id,
                                  host=self.host,
                                  task_state=None)
            except Exception as e:
                # We failed to roll back the instance. It should now be placed in an error state.
                self._instance_update(context, instance_id, vm_state=vm_states.ERROR)
                raise e

    def discard_instance(self, context, instance_id):
        """ Discards an instance so that no further instances maybe be launched from it. """

        LOG.debug(_("discard instance called: instance_id=%s"), instance_id)

        context.elevated()

        # Grab the DB representation for the VM.
        instance_ref = self.db.instance_get(context, instance_id)

        metadata = self.db.instance_metadata_get(context, instance_id)
        image_refs = self._extract_image_refs(metadata)
        # Call discard in the backend.
        self.vms_conn.discard(context, instance_ref.name,
                              use_image_service=FLAGS.gridcentric_use_image_service,
                              image_refs=image_refs)

        # Update the instance metadata (for completeness).
        metadata['blessed'] = False
        self.db.instance_metadata_update(context, instance_id, metadata, True)

        # Remove the instance.
        self._instance_update(context,
                              instance_id,
                              vm_state=vm_states.DELETED,
                              task_state=None,
                              terminated_at=utils.utcnow())
        self.db.instance_destroy(context, instance_id)
        usage_info = utils.usage_from_instance(instance_ref)
        notifier.notify('gridcentric.%s' % self.host,
                        'gridcentric.instance.discard',
                        notifier.INFO, usage_info)

    def launch_instance(self, context, instance_id, params={}, migration_url=None):
        """
        Construct the launched instance, with id instance_id. If migration_url is not none then 
        the instance will be launched using the memory server at the migration_url
        """

        LOG.debug(_("Launching new instance: instance_id=%s, migration_url=%s"),
                    instance_id, migration_url)

        # Grab the DB representation for the VM.
        instance_ref = self.db.instance_get(context, instance_id)

        if migration_url:
            self.incoming_migration[instance_id] = True
            # Just launch the given blessed instance.
            source_instance_ref = instance_ref

            # Load the old network info.
            network_info = self.network_api.get_instance_nw_info(context, instance_ref)

            # Update the instance state to be migrating. This will be set to
            # active again once it is completed in do_launch() as per all
            # normal launched instances.
            self._instance_update(context, instance_ref.id,
                                  vm_state=vm_states.MIGRATING,
                                  task_state=task_states.SPAWNING,
                                  host=self.host)
            instance_ref['host'] = self.host
        else:
            # Create a new launched instance.
            source_instance_ref = self._get_source_instance(context, instance_id)

            if not FLAGS.stub_network:
                # TODO(dscannell): We need to set the is_vpn parameter correctly.
                # This information might come from the instance, or the user might
                # have to specify it. Also, we might be able to convert this to a
                # cast because we are not waiting on any return value.
                LOG.debug(_("Making call to network for launching instance=%s"), \
                          instance_ref.name)

                self._instance_update(context, instance_ref.id,
                                      vm_state=vm_states.BUILDING,
                                      task_state=task_states.NETWORKING,
                                      host=self.host)
                instance_ref['host'] = self.host
                is_vpn = False
                requested_networks = None

                try:
                    network_info = self.network_api.allocate_for_instance(context,
                                                instance_ref, vpn=is_vpn,
                                                requested_networks=requested_networks)
                except Exception, e:
                    LOG.debug(_("Error during network allocation: %s"), str(e))
                    self._instance_update(context, instance_ref.id,
                                          vm_state=vm_states.ERROR,
                                          task_state=None)
                    # Short-circuit, can't proceed.
                    return

                LOG.debug(_("Made call to network for launching instance=%s, network_info=%s"),
                          instance_ref.name, network_info)
            else:
                network_info = []

            # Update the instance state to be in the building state.
            self._instance_update(context, instance_ref.id,
                                  vm_state=vm_states.BUILDING,
                                  task_state=task_states.SPAWNING)

        # note(dscannell): The target is in pages so we need to convert the value
        # If target is set as None, or not defined, then we default to "0".
        target = params.get("target", "0")
        if target != "0":
            try:
                target = str(memory_string_to_pages(target))
            except ValueError as e:
                LOG.warn(_('%s -> defaulting to no target'), str(e))
                target = "0"

        # Extract out the image ids from the source instance's metadata. 
        metadata = self.db.instance_metadata_get(context, source_instance_ref['id'])
        image_refs = self._extract_image_refs(metadata)
        try:
            # The main goal is to have the nova-compute process take ownership of setting up
            # the networking for the launched instance. This ensures that later changes to the
            # iptables can be handled directly by nova-compute. The method "pre_live_migration"
            # essentially sets up the networking for the instance on the destination host. We
            # simply send this message to nova-compute running on the same host (self.host)
            # and pass in block_migration:false and disk:none so that no disk operations are
            # performed.
            #
            # TODO(dscannell): How this behaves with volumes attached is an unknown. We currently
            # do not support having volumes attached at launch time, so we should be safe in
            # this regard.
            rpc.call(context,
                 self.db.queue_get_for(context, FLAGS.compute_topic, self.host),
                 {"method": "pre_live_migration",
                  "args": {'instance_id': instance_ref.id,
                           'block_migration': False,
                           'disk': None}})
            self.vms_conn.launch(context,
                                 source_instance_ref.name,
                                 str(target),
                                 instance_ref,
                                 network_info,
                                 migration_url=migration_url,
                                 use_image_service=FLAGS.gridcentric_use_image_service,
                                 image_refs=image_refs,
                                 params=params)

            # Perform our database update.
            if migration_url == None:
                usage_info = utils.usage_from_instance(instance_ref)
                notifier.notify('gridcentric.%s' % self.host,
                                'gridcentric.instance.launch',
                                notifier.INFO, usage_info)
                self._instance_update(context,
                                  instance_ref.id,
                                  vm_state=vm_states.ACTIVE,
                                  host=self.host,
                                  launched_at=utils.utcnow(),
                                  task_state=None)
        except Exception, e:
            LOG.debug(_("Error during launch %s: %s"), str(e), traceback.format_exc())
            self._instance_update(context, instance_ref.id,
                                  vm_state=vm_states.ERROR, task_state=None)
            # Raise the error up.
            raise e


    def _check_migration_status(self, context):
        filter = {
            'vm_state':vm_states.MIGRATNG,
            'host': self.host,
            'deleted':False
        }
        migrating_instances = self.compute_api.get_all(context, filter)
        for instance in migrating_instance:
            if instance['id'] not in self.outgoing_migration:
                # Possible issues:
                #   1) Everything in fine and this check occurs just before doing
                #      the migration lock.
                #   2) The service never received the migrate message.
                #   3) The service was restarted in the middle of a migration
                #   4) This service is now the destination host and the source
                #      is finishing up.
                #
                # For the 1 & 2 case we will simply switch the status back to ACTIVE
                # and have the user re-issue the migration. The 3 case is an error
                # condition and for case 4 we want to wait for the source to finish up.
                metadata = self.db.instance_metadata_get(context, instance['id'])
                if 'gc:migrating' not in metadata:
                    # This is either a case 1 or 2. In both cases we'll switch the
                    # instance's state to ACTIVE.
                    LOG.debug(_("Instance %s is in migrating state but no active migration is happening."), 
                              instance['id'])
                    self._instance_update(context, instance_ref['id'], vm_state=vm_states.ACTIVE)
                elif instance['id'] not in self.incoming_migration:
                    # This is case 3. The instance is not an incoming migration so
                    # mark it as error because we don't really know what state it
                    # is in.
                    LOG.debug(_("Instance %s had an issue during migration. WIll mark as ERROR."), instance['id'])
                    self._instance_update(context, instance_ref['id'], vm_state=vm_states.ERROR)
                # Otherwise we will ignore it.

    def _check_incoming_migration_status(self, context):
        filter = {
            'host':self.host,
            'deleted':False
        }
        instances = self.compute_api.get_all(context, filter)
        for instance in instances:
            if instance['id'] in self.incoming_migration and instance['vm_state'] != vm_states.MIGRATING:
                LOG.debug(_("Instance %s incoming migration has finished"), instance['id'])
                # The incoming migration has finished
                del self.incoming_migration[instance['id']] 

    def periodic_tasks(self, context=None):
        """Pass data back to the scheduler at a periodic interval."""
        super(GridCentricManager, self).periodic_tasks(context)
        # Do a simple check on migrations and ensure that we are aware
        # of the migrations going on.
        self.cond.acquire()
        try:
            self._check_migration_status(context)
            self._check_incoming_migration_status(context)

             
        finally:
            self.cond.release()
