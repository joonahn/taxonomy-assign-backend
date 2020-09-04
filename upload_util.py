#!/usr/bin/env python
import os
import os.path
import shutil
import json

from flask import current_app, request

BASE_DIR = os.path.dirname(__file__)

MEDIA_ROOT = os.path.join(BASE_DIR, 'media')
UPLOAD_DIRECTORY = os.path.join(MEDIA_ROOT, 'upload')
CHUNKS_DIRECTORY = os.path.join(MEDIA_ROOT, 'chunks')

# Utils
##################
def make_response(status=200, content=None):
    """ Construct a response to an upload request.
    Success is indicated by a status of 200 and { "success": true }
    contained in the content.
    Also, content-type is text/plain by default since IE9 and below chokes
    on application/json. For CORS environments and IE9 and below, the
    content-type needs to be text/html.
    """
    return current_app.response_class(json.dumps(content,
        indent=None if request.is_xhr else 2), mimetype='text/plain', status=status)


def validate(attrs):
    """ No-op function which will validate the client-side data.
    Werkzeug will throw an exception if you try to access an
    attribute that does not have a key for a MultiDict.
    """
    try:
        #required_attributes = ('qquuid', 'qqfilename')
        #[attrs.get(k) for k,v in attrs.items()]
        return True
    except Exception as e:
        return False


def handle_delete(uuid):
    """ Handles a filesystem delete based on UUID."""
    location = os.path.join(UPLOAD_DIRECTORY, uuid)
    print(uuid)
    print(location)
    shutil.rmtree(location)

def handle_upload(f, attrs):
    """ Handle a chunked or non-chunked upload.
    """

    chunked = False
    dest_folder = os.path.join(UPLOAD_DIRECTORY, attrs['qquuid'])
    dest = os.path.join(dest_folder, attrs['qqfilename'])


    # Chunked
    if 'qqtotalparts' in attrs and int(attrs['qqtotalparts']) > 1:
        chunked = True
        dest_folder = os.path.join(CHUNKS_DIRECTORY, attrs['qquuid'])
        dest = os.path.join(dest_folder, attrs['qqfilename'], str(attrs['qqpartindex']))

    save_upload(f, dest)

    if chunked and (int(attrs['qqtotalparts']) - 1 == int(attrs['qqpartindex'])):

        combine_chunks(attrs['qqtotalparts'],
            attrs['qqtotalfilesize'],
            source_folder=os.path.dirname(dest),
            dest=os.path.join(UPLOAD_DIRECTORY, attrs['qquuid'],
                attrs['qqfilename']))

        shutil.rmtree(os.path.dirname(os.path.dirname(dest)))


def save_upload(f, path):
    """ Save an upload.
    Uploads are stored in media/uploads
    """
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(path, 'wb+') as destination:
        destination.write(f.read())


def combine_chunks(total_parts, total_size, source_folder, dest):
    """ Combine a chunked file into a whole file again. Goes through each part
    , in order, and appends that part's bytes to another destination file.
    Chunks are stored in media/chunks
    Uploads are saved in media/uploads
    """

    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))

    with open(dest, 'wb+') as destination:
        for i in range(int(total_parts)):
            part = os.path.join(source_folder, str(i))
            with open(part, 'rb') as source:
                destination.write(source.read())


