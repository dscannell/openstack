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

import errno
import os

from nova import image
from nova import log as logging

LOG = logging.getLogger('nova.gridcentric.image')

class ImageService(object):

    def __init__(self, image_service=None):
        self.image_service = image_service or image.get_default_image_service()

    def create(self, context, name, instance_uuid=None):
        """ Creates a new image and returns its id """
        properties = {'user_id': str(context.user_id),
                      'image_state': 'creating'}
        if instance_uuid is not None:
            properties['instance_uuid'] = instance_uuid

        sent_meta = {'name': name , 'is_public': False,
                     'status': 'creating', 'properties': properties}
        image_ref = self.image_service.create(context, sent_meta)
        return image_ref['id']

    def upload(self, context, image_id, content_path):
        """ Uploads the contents to the image id """
        # Send up the file data to the newly created image.
        metadata = {'is_public': False,
                    'status': 'active',
                    'properties': {
                        'image_state': 'available',
                        'owner_id': context.project_id}
        }
        metadata['disk_format'] = "raw"
        metadata['container_format'] = "bare"

        # Upload that image to the image service
        LOG.debug(_("Uploading image %s") %(content_path))
        with open(content_path) as image_file:
            self.image_service.update(context,
                image_id,
                metadata,
                image_file)

    def download(self, context, image_id, location):
        try:
            with open(location, "wb") as image_file:
                metadata = self.image_service.get(context, image_id, image_file)
        except Exception, exc:
            try:
                os.unlink(location)
            except OSError, e:
                if e.errno != errno.ENOENT:
                    LOG.warn("unable to remove stale image '%s': %s" %
                     (location, e.strerror))
            raise exc
        return metadata

    def delete(self, context, id):
        """ Deletes the image """
        pass