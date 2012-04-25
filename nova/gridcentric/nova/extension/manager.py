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
import traceback
import os
import socket
import subprocess

from nova import exception
from nova import flags
from nova import log as logging
LOG = logging.getLogger('gridcentric.nova.manager')
FLAGS = flags.FLAGS

from nova import manager
from nova import utils
from nova import rpc
from nova import network
# We need to import this module because other nova modules use the
# flags that it defines (without actually importing this module). So
# we need to ensure this module is loaded so that we can have access
# to those flags.
from nova.network import manager as network_manager
from nova.compute import power_state
from nova.compute import task_states
from nova.compute import vm_states
from nova.compute import manager as compute_manager

from gridcentric.nova.extension import API
import gridcentric.nova.extension.vmsconn as vmsconn

# We borrow the threadpool from VMS.
import vms.threadpool

class GridCentricManager(manager.SchedulerDependentManager):

    def __init__(self, *args, **kwargs):
        self.vms_conn = None
        self._init_vms()
        self.network_api = network.API()
        self.gridcentric_api = API()
        self.compute_manager = compute_manager.ComputeManager()
        super(GridCentricManager, self).__init__(service_name="gridcentric", *args, **kwargs)

    def _init_vms(self):
        """ Initializes the hypervisor options depending on the openstack connection type. """

        connection_type = FLAGS.connection_type
        self.vms_conn = vmsconn.get_vms_connection(connection_type)
        self.vms_conn.configure()

    def _instance_update(self, context, instance_id, **kwargs):
        """Update an instance in the database using kwargs as value."""
        return self.db.instance_update(context, instance_id, kwargs)

    def _copy_instance(self, context, instance_id, new_suffix, launch=False):
        # (dscannell): Basically we want to copy all of the information from
        # instance with id=instance_id into a new instance. This is because we
        # are basically "cloning" the vm as far as all the properties are
        # concerned.

        instance_ref = self.db.instance_get(context, instance_id)
        image_ref = instance_ref.get('image_ref', '')
        if image_ref == '':
            image_ref = instance_ref.get('image_id', '')

        if launch:
            metadata = {'launched_from':'%s' % (instance_id)}
            host = self.host
        else:
            metadata = {'blessed_from':'%s' % (instance_id)}
            host = None

        instance = {
           'reservation_id': utils.generate_uid('r'),
           'image_ref': image_ref,
           'state': 0,
           'state_description': 'halted',
           'user_id': context.user_id,
           'project_id': context.project_id,
           'launch_time': '',
           'instance_type_id': instance_ref['instance_type_id'],
           'memory_mb': instance_ref['memory_mb'],
           'vcpus': instance_ref['vcpus'],
           'local_gb': instance_ref['local_gb'],
           'display_name': "%s-%s" % (instance_ref['display_name'], new_suffix),
           'display_description': instance_ref['display_description'],
           'user_data': instance_ref.get('user_data', ''),
           'key_name': instance_ref.get('key_name', ''),
           'key_data': instance_ref.get('key_data', ''),
           'locked': False,
           'metadata': metadata,
           'availability_zone': instance_ref['availability_zone'],
           'os_type': instance_ref['os_type'],
           'host': host,
        }
        new_instance_ref = self.db.instance_create(context, instance)

        elevated = context.elevated()

        security_groups = self.db.security_group_get_by_instance(context, instance_id)
        for security_group in security_groups:
            self.db.instance_add_security_group(elevated,
                                                new_instance_ref.id,
                                                security_group['id'])

        return new_instance_ref

    def _next_clone_num(self, context, instance_id):
        """ Returns the next clone number for the instance_id """

        metadata = self.db.instance_metadata_get(context, instance_id)
        clone_num = int(metadata.get('last_clone_num', -1)) + 1
        metadata['last_clone_num'] = clone_num
        self.db.instance_metadata_update(context, instance_id, metadata, True)

        LOG.debug(_("Instance %s has new clone num=%s"), instance_id, clone_num)
        return clone_num

    def _is_instance_blessed(self, context, instance_id):
        """ Returns True if this instance is blessed, False otherwise. """
        metadata = self.db.instance_metadata_get(context, instance_id)
        return metadata.get('blessed', False)

    def _is_instance_launched(self, context, instance_id):
        """ Returns True if this instance is launched, False otherwise """
        metadata = self.db.instance_metadata_get(context, instance_id)
        return "launched_from" in metadata

    def bless_instance(self, context, instance_id, migration_url=None):
        """
        Blesses an instance, which will create a new instance from which
        further instances can be launched from it.
        """

        LOG.debug(_("bless instance called: instance_id=%s"), instance_id)

        # Setup the DB representation for the new VM.
        instance_ref = self.db.instance_get(context, instance_id)

        is_blessed = self._is_instance_blessed(context, instance_id)
        is_launched = self._is_instance_launched(context, instance_id)
        if is_blessed:
            # The instance is already blessed. We can't rebless it.
            raise exception.Error(_(("Instance %s is already blessed. " +
                                     "Cannot rebless an instance.") % instance_id))
        elif is_launched:
            # The instance is a launched one. We cannot bless launched instances.
            raise exception.Error(_(("Instance %s has been launched. " +
                                     "Cannot bless a launched instance.") % instance_id))
        elif instance_ref['vm_state'] != vm_states.ACTIVE:
            # The instance is not active. We cannot bless a non-active instance.
             raise exception.Error(_(("Instance %s is not active. " +
                                      "Cannot bless a non-active instance.") % instance_id))

        context.elevated()

        if migration_url:
            # Tweak only this instance directly.
            new_instance_ref = instance_ref
        else:
            # Create a new blessed instance.
            clonenum = self._next_clone_num(context, instance_id)
            new_instance_ref = self._copy_instance(context, instance_id, str(clonenum), launch=False)

        try:
            # Create a new 'blessed' VM with the given name.
            name, migration_url = self.vms_conn.bless(instance_ref.name,
                                                new_instance_ref.name,
                                                migration_url=migration_url)
        except Exception, e:
            LOG.debug(_("Error during bless %s: %s"), str(e), traceback.format_exc())
            self._instance_update(context, new_instance_ref.id,
                                  vm_state=vm_states.ERROR, task_state=None)
            # Short-circuit, nothing to be done.
            return

        if not(migration_url):
            # Mark this new instance as being 'blessed'.
            metadata = self.db.instance_metadata_get(context, new_instance_ref.id)
            metadata['blessed'] = True
            self.db.instance_metadata_update(context, new_instance_ref.id, metadata, True)

        # Return the memory URL (will be None for a normal bless).
        return migration_url

    def migrate_instance(self, context, instance_id, dest):
        """
        Migrates an instance, dealing with special streaming cases as necessary.
        """
        LOG.debug(_("migrate instance called: instance_id=%s"), instance_id)

        # FIXME: This live migration code does not currently support volumes,
        # nor floating IPs. Both of these would be fairly straight-forward to
        # add but probably cry out for a better factoring of this class as much
        # as this code can be inherited directly from the ComputeManager. The
        # only real difference is that the migration must not go through
        # libvirt, instead we drive it via our bless, launch routines.

        if dest == self.host:
            raise exception.Error(_("Unable to migrate to the same host."))

        # Grab a reference to the instance.
        instance_ref = self.db.instance_get(context, instance_id)

        src = instance_ref['host']
        if instance_ref['volumes']:
            rpc.call(context,
                      FLAGS.volume_topic,
                      {"method": "check_for_export",
                       "args": {'instance_id': instance_id}})
        rpc.call(context,
                 self.db.queue_get_for(context, FLAGS.compute_topic, dest),
                 {"method": "pre_live_migration",
                  "args": {'instance_id': instance_id,
                           'block_migration': False,
                           'disk': None}})

        # Grab the remote queue (to make sure the host exists).
        queue = self.db.queue_get_for(context, FLAGS.gridcentric_topic, dest)

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

        # Bless this instance, given the db_copy=False here, the bless
        # will use the same name and no files will be shift around.
        migration_url = self.bless_instance(context, instance_id,
                                            migration_url="mcdist://%s" % devname)

        # After blessing we need to notify the hypvisor that the instance is no longer
        # available.
        network_info = self.network_api.get_instance_nw_info(context, instance_ref)
        self.vms_conn.migration_post_bless(instance_ref, network_info)

        # Make sure that the disk reflects all current state for this VM.
        # It's times like these that I wish there was a way to do this on a
        # per-file basis, but we have no choice here but to sync() globally.
        subprocess.call(["sync"])

        try:
            # Launch on the different host. With the non-null migration_url,
            # the launch will assume that all the files are the same places are
            # before (and not in special launch locations).
            rpc.call(context, queue,
                    {"method": "launch_instance",
                     "args": {'instance_id': instance_id,
                              'migration_url': migration_url}})

            # Teardown on this host (and delete the descriptor).
            self.vms_conn.discard(instance_ref.name)

            self.compute_manager.post_live_migration(context, instance_ref, dest, block_migration=False)

        except:
            #TODO(dscannell): This rollback is a bit broken right now because we cannot simply
            # relaunch the instance on this host. The order of events during migration are:
            #  1. Bless instance -- This will leave the qemu process in a paused state, but alive
            #  2. Clean up libvirt state (need to see why it doesn't kill the qemu process)
            #  3. Call launch on the destination host and wait for the instance to hoard its memory
            #  4. Call discard that will clean up the descriptor and kill off the qemu process
            # Depending on what has occurred different strategies are needed to rollback
            #  e.g We can simply unpause the instance if the qemu process still exists (might need
            #  to move when libvirt cleanup occurs).
            LOG.debug(_("Error during migration: %s"), traceback.format_exc())
            # Rollback is launching here again.
            self.launch_instance(context, instance_id, migration_url=migration_url)

    def discard_instance(self, context, instance_id):
        """ Discards an instance so that no further instances maybe be launched from it. """

        LOG.debug(_("discard instance called: instance_id=%s"), instance_id)

        if not self._is_instance_blessed(context, instance_id):
            # The instance is not blessed. We can't discard it.
            raise exception.Error(_(("Instance %s is not blessed. " +
                                     "Cannot discard an non-blessed instance.") % instance_id))
        elif len(self.gridcentric_api.list_launched_instances(context, instance_id)) > 0:
            # There are still launched instances based off of this one.
            raise exception.Error(_(("Instance %s still has launched instances. " +
                                     "Cannot discard an instance with remaining launched ones.") %
                                     instance_id))
        context.elevated()

        # Grab the DB representation for the VM.
        instance_ref = self.db.instance_get(context, instance_id)

        # Call discard in the backend.
        self.vms_conn.discard(instance_ref.name)

        # Update the instance metadata (for completeness).
        metadata = self.db.instance_metadata_get(context, instance_id)
        metadata['blessed'] = False
        self.db.instance_metadata_update(context, instance_id, metadata, True)

        # Remove the instance.
        self.db.instance_destroy(context, instance_id)

    def launch_instance(self, context, instance_id, migration_url=None):
        """
        Launches a new virtual machine instance that is based off of the instance referred
        by instance_id.
        """

        LOG.debug(_("Launching new instance: instance_id=%s"), instance_id)

        if not(migration_url) and not(self._is_instance_blessed(context, instance_id)):
            # The instance is not blessed. We can't launch new instances from it.
            raise exception.Error(
                  _(("Instance %s is not blessed. " +
                     "Please bless the instance before launching from it.") % instance_id))

        context.elevated()

        # Grab the DB representation for the VM.
        instance_ref = self.db.instance_get(context, instance_id)

        if migration_url:
            # Just launch the given blessed instance.
            new_instance_ref = instance_ref

            # Load the old network info.
            network_info = self.network_api.get_instance_nw_info(context, instance_ref)

            # Update the instance state to be migrating. This will be set to
            # active again once it is completed in do_launch() as per all
            # normal launched instances.
            self._instance_update(context, instance_ref.id,
                                  vm_state=vm_states.MIGRATING,
                                  task_state=task_states.SPAWNING)
        else:
            # Create a new launched instance.
            new_instance_ref = self._copy_instance(context, instance_id, "clone", launch=True)

            if not FLAGS.stub_network:
                # TODO(dscannell): We need to set the is_vpn parameter correctly.
                # This information might come from the instance, or the user might
                # have to specify it. Also, we might be able to convert this to a
                # cast because we are not waiting on any return value.
                LOG.debug(_("Making call to network for launching instance=%s"), new_instance_ref.name)
                self._instance_update(context, new_instance_ref.id,
                                      vm_state=vm_states.BUILDING,
                                      task_state=task_states.NETWORKING)
                is_vpn = False
                requested_networks = None

                try:
                    network_info = self.network_api.allocate_for_instance(context,
                                                new_instance_ref, vpn=is_vpn,
                                                requested_networks=requested_networks)
                except Exception, e:
                    LOG.debug(_("Error during network allocation: %s"), str(e))
                    self._instance_update(context, new_instance_ref.id,
                                          vm_state=vm_states.ERROR,
                                          task_state=None)
                    # Short-circuit, can't proceed.
                    return

                LOG.debug(_("Made call to network for launching instance=%s, network_info=%s"),
                          new_instance_ref.name, network_info)
            else:
                network_info = []

            # Update the instance state to be in the building state.
            self._instance_update(context, new_instance_ref.id,
                                  vm_state=vm_states.BUILDING,
                                  task_state=task_states.SPAWNING)

        # TODO(dscannell): Need to figure out what the units of measurement
        # for the target should be (megabytes, kilobytes, bytes, etc).
        # Also, target should probably be an optional parameter that the
        # user can pass down.  The target memory settings for the launch
        # virtual machine.
        target = new_instance_ref['memory_mb']

        def launch_bottom_half():
            try:
                self.vms_conn.launch(context,
                                     instance_ref.name,
                                     str(target),
                                     new_instance_ref,
                                     network_info,
                                     migration_url=migration_url)
                self.vms_conn.replug(new_instance_ref.name,
                                     self.extract_mac_addresses(network_info))

                # Perform our database update.
                self._instance_update(context,
                                      new_instance_ref.id,
                                      vm_state=vm_states.ACTIVE,
                                      host=self.host,
                                      task_state=None)
            except Exception, e:
                LOG.debug(_("Error during launch %s: %s"), str(e), traceback.format_exc())
                self._instance_update(context, new_instance_ref.id,
                                      vm_state=vm_states.ERROR, task_state=None)

        if migration_url:
            launch_bottom_half()
        else:
            # Run the actual launch asynchronously.
            vms.threadpool.submit(launch_bottom_half)

    def extract_mac_addresses(self, network_info):
        mac_addresses = {}
        vif = 0
        for network in network_info:
            mac_addresses[str(vif)] = network[1]['mac']
            vif += 1

        return mac_addresses
