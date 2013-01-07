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


"""Handles all requests relating to GridCentric functionality."""
import random

from nova import compute
from nova.compute import vm_states
from nova import flags
from nova import exception
from nova import log as logging
from nova.db import base
from nova import quota
from nova import exception as novaexc
from nova import rpc
from nova.openstack.common import cfg
from nova import utils

from gridcentric.nova import image


LOG = logging.getLogger('nova.gridcentric.api')
FLAGS = flags.FLAGS

gridcentric_api_opts = [
               cfg.StrOpt('gridcentric_topic',
               default='gridcentric',
               help='the topic gridcentric nodes listen on') ]
FLAGS.register_opts(gridcentric_api_opts)

class API(base.Base):
    """API for interacting with the gridcentric manager."""

    def __init__(self, image_service=None, **kwargs):
        super(API, self).__init__(**kwargs)
        self.compute_api = compute.API()
        self.image_service = image.ImageService()

    def get(self, context, instance_uuid):
        """Get a single instance with the given instance_uuid."""
        rv = self.db.instance_get_by_uuid(context, instance_uuid)
        return dict(rv.iteritems())

    def _cast_gridcentric_message(self, method, context, instance_uuid, host=None,
                              params=None):
        """Generic handler for RPC casts to gridcentric. This does not block for a response.

        :param params: Optional dictionary of arguments to be passed to the
                       gridcentric worker

        :returns: None
        """

        if not params:
            params = {}
        if not host:
            instance = self.get(context, instance_uuid)
            host = instance['host']
        if not host:
            queue = FLAGS.gridcentric_topic
        else:
            queue = self.db.queue_get_for(context, FLAGS.gridcentric_topic, host)
        params['instance_uuid'] = instance_uuid
        kwargs = {'method': method, 'args': params}
        rpc.cast(context, queue, kwargs)

    def _check_quota(self, context, instance_uuid):
        # Check the quota to see if we can launch a new instance.
        instance = self.get(context, instance_uuid)
        instance_type = instance['instance_type']
        metadata = instance['metadata']

        # check the quota to if we can launch a single instance.
        num_instances = quota.allowed_instances(context, 1, instance['instance_type'])
        if num_instances < 1:
            pid = context.project_id
            LOG.warn(_("Quota exceeded for %(pid)s,"
                    " tried to launch an instance"))
            if num_instances <= 0:
                message = _("Instance quota exceeded. You cannot run any "
                            "more instances of this type.")
            else:
                message = _("Instance quota exceeded. You can only run %s "
                            "more instances of this type.") % num_instances
            raise novaexc.QuotaError(code="InstanceLimitExceeded")

        # check against metadata
        metadata = self.db.instance_metadata_get(context, instance['id'])
        self.compute_api._check_metadata_properties_quota(context, metadata)

    def _copy_instance(self, context, instance_uuid, new_suffix, launch=False):
        # (dscannell): Basically we want to copy all of the information from
        # instance with id=instance_uuid into a new instance. This is because we
        # are basically "cloning" the vm as far as all the properties are
        # concerned.

        instance_ref = self.db.instance_get_by_uuid(context, instance_uuid)
        image_ref = instance_ref.get('image_ref', '')
        if image_ref == '':
            image_ref = instance_ref.get('image_id', '')

        if launch:
            metadata = {'launched_from':'%s' % (instance_ref['uuid'])}
        else:
            metadata = {'blessed_from':'%s' % (instance_ref['uuid'])}

        instance = {
           'reservation_id': utils.generate_uid('r'),
           'image_ref': image_ref,
           'vm_state': vm_states.BUILDING,
           'state_description': 'halted',
           'user_id': context.user_id,
           'project_id': context.project_id,
           'launch_time': '',
           'instance_type_id': instance_ref['instance_type_id'],
           'memory_mb': instance_ref['memory_mb'],
           'vcpus': instance_ref['vcpus'],
           'root_gb': instance_ref['root_gb'],
           'ephemeral_gb': instance_ref['ephemeral_gb'],
           'display_name': "%s-%s" % (instance_ref['display_name'], new_suffix),
           'display_description': instance_ref['display_description'],
           'user_data': instance_ref.get('user_data', ''),
           'key_name': instance_ref.get('key_name', ''),
           'key_data': instance_ref.get('key_data', ''),
           'locked': False,
           'metadata': metadata,
           'availability_zone': instance_ref['availability_zone'],
           'os_type': instance_ref['os_type'],
           'host': None,
        }
        new_instance_ref = self.db.instance_create(context, instance)

        # (dscannell) We need to reload the instance_ref in order for it to be associated with
        # the database session of lazy-loading.
        new_instance_ref = self.db.instance_get(context, new_instance_ref.id)

        elevated = context.elevated()
        security_groups = self.db.security_group_get_by_instance(context, instance_ref['id'])
        for security_group in security_groups:
            self.db.instance_add_security_group(elevated,
                                                new_instance_ref['uuid'],
                                                security_group['id'])

        return new_instance_ref

    def _instance_metadata(self, context, instance_uuid):
        """ Looks up and returns the instance metadata """

        instance_ref = self.db.instance_get_by_uuid(context, instance_uuid)
        return self.db.instance_metadata_get(context, instance_ref['id'])

    def _instance_metadata_update(self, context, instance_uuid, metadata):
        """ Updates the instance metadata """

        instance_ref = self.db.instance_get_by_uuid(context, instance_uuid)
        return self.db.instance_metadata_update(context, instance_ref['id'], metadata, True)

    def _next_clone_num(self, context, instance_uuid):
        """ Returns the next clone number for the instance_uuid """

        metadata = self._instance_metadata(context, instance_uuid)
        clone_num = int(metadata.get('last_clone_num', -1)) + 1
        metadata['last_clone_num'] = clone_num
        self._instance_metadata_update(context, instance_uuid, metadata)

        LOG.debug(_("Instance %s has new clone num=%s"), instance_uuid, clone_num)
        return clone_num

    def _is_instance_blessed(self, context, instance_uuid):
        """ Returns True if this instance is blessed, False otherwise. """
        metadata = self._instance_metadata(context, instance_uuid)
        return 'blessed_from' in metadata

    def _is_instance_launched(self, context, instance_uuid):
        """ Returns True if this instance is launched, False otherwise """
        metadata = self._instance_metadata(context, instance_uuid)
        return "launched_from" in metadata

    def _list_gridcentric_hosts(self, context):
        """ Returns a list of all the hosts known to openstack running the gridcentric service. """
        admin_context = context.elevated()
        services = self.db.service_get_all_by_topic(admin_context, FLAGS.gridcentric_topic)
        hosts = []
        for srv in services:
            if srv['host'] not in hosts:
                hosts.append(srv['host'])
        return hosts

    def bless_instance(self, context, instance_uuid):
        # Setup the DB representation for the new VM.
        instance_ref = self.get(context, instance_uuid)

        is_blessed = self._is_instance_blessed(context, instance_uuid)
        is_launched = self._is_instance_launched(context, instance_uuid)
        if is_blessed:
            # The instance is already blessed. We can't rebless it.
            raise exception.Error(_(("Instance %s is already blessed. " +
                                     "Cannot rebless an instance.") % instance_uuid))
        elif is_launched:
            # The instance is a launched one. We cannot bless launched instances.
            raise exception.Error(_(("Instance %s has been launched. " +
                                     "Cannot bless a launched instance.") % instance_uuid))
        elif instance_ref['vm_state'] != vm_states.ACTIVE:
            # The instance is not active. We cannot bless a non-active instance.
            raise exception.Error(_(("Instance %s is not active. " +
                                      "Cannot bless a non-active instance.") % instance_uuid))

        clonenum = self._next_clone_num(context, instance_uuid)
        new_instance_ref = self._copy_instance(context, instance_uuid, str(clonenum), launch=False)

        LOG.debug(_("Casting gridcentric message for bless_instance") % locals())
        self._cast_gridcentric_message('bless_instance', context, new_instance_ref['uuid'],
                                       host=instance_ref['host'])

        # We reload the instance because the manager may have change its state (most likely it 
        # did).
        return self.get(context, new_instance_ref['uuid'])

    def discard_instance(self, context, instance_uuid):
        LOG.debug(_("Casting gridcentric message for discard_instance") % locals())

        if not self._is_instance_blessed(context, instance_uuid):
            # The instance is not blessed. We can't discard it.
            raise exception.Error(_(("Instance %s is not blessed. " +
                                     "Cannot discard an non-blessed instance.") % instance_uuid))
        elif len(self.list_launched_instances(context, instance_uuid)) > 0:
            # There are still launched instances based off of this one.
            raise exception.Error(_(("Instance %s still has launched instances. " +
                                     "Cannot discard an instance with remaining launched ones.") %
                                     instance_uuid))

        self._cast_gridcentric_message('discard_instance', context, instance_uuid)

    def launch_instance(self, context, instance_uuid, params={}):
        pid = context.project_id
        uid = context.user_id

        self._check_quota(context, instance_uuid)
        instance = self.get(context, instance_uuid)

        if not(self._is_instance_blessed(context, instance_uuid)):
            # The instance is not blessed. We can't launch new instances from it.
            raise exception.Error(
                  _(("Instance %s is not blessed. " +
                     "Please bless the instance before launching from it.") % instance_uuid))

        # Create a new launched instance.
        new_instance_ref = self._copy_instance(context, instance_uuid, "clone", launch=True)

        LOG.debug(_("Casting to scheduler for %(pid)s/%(uid)s's"
                    " instance %(instance_uuid)s") % locals())
        rpc.cast(context,
                     FLAGS.scheduler_topic,
                     {"method": "launch_instance",
                      "args": {"topic": FLAGS.gridcentric_topic,
                               "instance_uuid": new_instance_ref['uuid'],
                               "params": params}})

        return self.get(context, new_instance_ref['uuid'])

    def _find_migration_target(self, context, instance_host, dest):
        gridcentric_hosts = self._list_gridcentric_hosts(context)
        if dest == None:
            # We will pick a random host.
            if instance_host in gridcentric_hosts:
                # We cannot migrate to ourselves so take that host out of the list.
                gridcentric_hosts.remove(instance_host)

            if len(gridcentric_hosts) == 0:
                raise exception.Error(_("There are no available hosts for the migration target."))
            random.shuffle(gridcentric_hosts)
            dest = gridcentric_hosts[0]

        elif dest not in gridcentric_hosts:
            raise exception.Error(_("Cannot migrate to host %s because it is not running the"
                                    " gridcentric service.") % dest)
        elif dest == instance_host:
            raise exception.Error(_("Unable to migrate to the same host."))

        return dest

    def migrate_instance(self, context, instance_uuid, dest):
        # Grab the DB representation for the VM.
        instance_ref = self.get(context, instance_uuid)

        if instance_ref['vm_state'] == vm_states.MIGRATING:
            raise exception.Error(
                              _("Unable to migrate instance %s because it is already migrating.") %
                              instance_id)
        elif instance_ref['vm_state'] != vm_states.ACTIVE:
            raise exception.Error(_("Unable to migrate instance %s because it is not active") %
                                  instance_id)
        dest = self._find_migration_target(context, instance_ref['host'], dest)

        self.db.instance_update(context, instance_ref['id'], {'vm_state':vm_states.MIGRATING})
        LOG.debug(_("Casting gridcentric message for migrate_instance") % locals())
        self._cast_gridcentric_message('migrate_instance', context,
                                       instance_ref['uuid'], host=instance_ref['host'],
                                       params={"dest" : dest})

    def list_launched_instances(self, context, instance_uuid):
         # Assert that the instance with the uuid actually exists.
        self.get(context, instance_uuid)
        filter = {
                  'metadata':{'launched_from':'%s' % instance_uuid},
                  'deleted':False
                  }
        launched_instances = self.compute_api.get_all(context, filter)
        return launched_instances

    def list_blessed_instances(self, context, instance_uuid):
        # Assert that the instance with the uuid actually exists.
        self.get(context, instance_uuid)
        filter = {
                  'metadata':{'blessed_from':'%s' % instance_uuid},
                  'deleted':False
                  }
        blessed_instances = self.compute_api.get_all(context, filter)
        return blessed_instances

    def export_blessed_instance(self, context, instance_uuid):
        """
        Exports the blessed instance in a format that can be imported.
        This is useful for moving a blessed instance between clouds.
        """
        # Ensure that the instance_uuid is blessed
        instance = self.get(context, instance_uuid)
        if not(self._is_instance_blessed(context, instance_uuid)):
            # The instance is not blessed. We can't launch new instances from it.
            raise exception.Error(
                _(("Instance %s is not blessed. " +
                   "Only blessed instances can be exported.") % instance_uuid))

        # Create an image record to store the blessed artifacts for this instance
        # and call to nova-gc to populate the record

        # Create the image in the image_service.
        image_name = "%s%s" % ("export of ", instance['display_name'])
        image_id = self.image_service.create(context, image_name)

        self._cast_gridcentric_message("export_instance",context, instance_uuid, params={'image_id': image_id})

        # sanitize the instance record to keep the important bits and pass it back,
        # along with the image record.
        #
        # The sanitized instance data will look like:
        #   need to keep instance.metadata
        #   instance.security_groups --> We should also list the values in each security group
        #
        instance_data = self.create_instance_data(instance)

        return {'instance_data': instance_data,
               'image_id': image_id}

    def rules_to_dict(self, rules):
        return [{'from_port':rule['from_port'],
                 'to_port':rule['to_port'],
                 'protocol':rule['protocol'],
                 'cidr':rule['cidr']} for rule in rules]

    def create_instance_data(self, instance):
        instance_metadata = {}
        for entry in instance['metadata']:
            instance_metadata[entry.key] = entry.value

        """
        instance_security_groups = []
        for security_group in instance['security_groups']:
            instance_security_groups.append({
                'name': security_group['name'],
                'description': security_group['description'],
                'rules': self.rules_to_dict(security_group['rules'])
            })
        """

        instance_data = {
            'image_ref': instance['image_ref'],
            'vm_state': instance['vm_state'],
            'instance_type_id': instance['instance_type_id'],
            'memory_mb': instance['memory_mb'],
            'vcpus': instance['vcpus'],
            'root_gb': instance['root_gb'],
            'ephemeral_gb': instance['ephemeral_gb'],
            'display_name': instance['display_name'],
            'display_description': instance['display_description'],
            'user_data': instance.get('user_data', ''),
            'key_name': instance.get('key_name', ''),
            'key_data': instance.get('key_data', ''),
            'locked': False,
            'metadata': instance_metadata,
            'availability_zone': instance['availability_zone'],
            'os_type': instance['os_type']
            # 'security_groups': instance_security_groups
        }

        return instance_data

    def apply_imported_security_group(self, context, instance_ref, security):
        """
        Applies the security groups to the imported instance_ref.
        """

        def are_same_rules(rules1, rules2):
            # note(dscannell): This assumes that all of the rules in a set are unique
            return len(rules1) == len(rules2) and \
                False not in [rule in rules2 for rule in rules1]

        def create_security_group_name(context, starting_name):
            new_name = starting_name
            i = 1
            while self.db.security_group_exists(context, context.project_id, new_name):
                new_name = "%s-%s" %(starting_name, str(i))

            return new_name

        db_security_groups = self.db.security_group_get_all(context)
        elevated_context = context.elevated()
        for import_group in import_security_groups:
            matching_group_found = False

            for db_group in db_security_groups:
                if are_same_rules(self.rules_to_dict(db_group['rules']), import_group['rules']):
                    # Associate this security group to the instance.
                    matching_group_found = True
                    self.db.instance_add_security_group(elevated_context, instance_ref['uuid'], db_group['id'])
                    break

            if not matching_group_found:
                # Create a new security group and associate it to the instance.
                # Need to check if this name already exists A likely scenario is if the default security group
                # between installations are different..

                new_group_values = {'user': context.user_id,
                                    'project': context.project_id,
                                    'name': create_security_group_name(context, import_group['name']),
                                    'description': import_group['description']}
                db_group = self.db.security_group_create(context, new_group_values)
                # Add the rules to this new security group
                for rule in import_group['rules']:
                    # Create new rules
                    pass
                pass



    def import_blessed_instance(self, context, instance_data, name, security_groups, image_id):
        """
        Imports the instance as a new blessed instance.
        """

        # the instance_blob will contain the image record in glance as well
        # as the instance meta-data generated with export. We will use the meta-data
        # to create a new record, and then pass this instance, with the image ref
        # to nova-gc to finish off.

        # NOTE(dscannell) we need to do all the bless quota stuff around here because we are essentially creating
        # a new blessed instance into the system.

        # Return the new import
        metadata = instance_data.pop('metadata', {})

        if name is not None:
            instance_data['display_name'] = name

        instance_data['project_id'] = context.project_id
        instance_data['user_id'] = context.user_id
        new_instance_ref = self.db.instance_create(context, instance_data)
        LOG.debug(_("Imported new instance %s" % (new_instance_ref)))
        self._instance_metadata_update(context, new_instance_ref['uuid'], metadata)

        # Apply the security groups
        for group_name in security_groups:
            secgroup = self.db.security_group_get_by_name(context, context.project_id, group_name)
            self.db.instance_add_security_group(context.elevated(), new_instance_ref['uuid'], secgroup['id'])

        self._cast_gridcentric_message('import_instance', context, new_instance_ref['uuid'], params={'image_id':image_id})

        # Essentially we want to create a new instance from this blob
        return self.get(context, new_instance_ref['uuid'])


    def _find_boot_host(self, context, metadata):

        gc_hosts = self._list_gridcentric_hosts(context)
        if metadata == None or 'gc:target_host' not in metadata:
            # Find a random host that is running the gridcentric services.
            random.shuffle(gc_hosts)
            target_host = gc_hosts[0]
        else:
            # Ensure that the target host is running the gridcentic service.
            target_host = metadata['gc:target_host']
            if target_host not in gc_hosts:
                raise exception.Error(
                              _("Only able to launch on hosts running the gridcentric service."))
        return target_host

    def create(self, context, *args, **kwargs):
        """
        This will create a new instance on a target host if one is specified in the
        gc:target-host metadata field.
        """

        if not context.is_admin:
            raise exception.Error(_("This feature is restricted to only admin users."))
        metadata = kwargs.get('metadata', None)
        target_host = self._find_boot_host(context, metadata)

        # Normally the compute_api would send a message to the sceduler. In this case since
        # we have a target host, we'll just explicity send a message to that compute manager.
        compute_api = compute.API()
        def host_schedule(rpc_method,
                    context, base_options,
                    instance_type,
                    availability_zone, injected_files,
                    admin_password, image,
                    num_instances,
                    requested_networks,
                    block_device_mapping,
                    security_group,
                    filter_properties):

            instance_uuid = base_options.get('uuid')
            now = utils.utcnow()
            self.db.instance_update(context, instance_uuid,
                               {'host': target_host,
                                'scheduled_at': now})

            rpc.cast(context, self.db.queue_get_for(context, FLAGS.compute_topic, target_host),
                     {"method": "run_instance",
                      "args": {"instance_uuid": instance_uuid,
                       "availability_zone": availability_zone,
                       "admin_password": admin_password,
                       "injected_files": injected_files,
                       "requested_networks": requested_networks}})

            # Instance was already created before calling scheduler
            return self.get(context, instance_uuid)

        # Stub out the call to the scheduler and then delegate the rest of the work to the
        # compute api. 
        compute_api._schedule_run_instance = host_schedule
        return compute_api.create(context, *args, **kwargs)
