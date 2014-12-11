from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


def publish(resources, target='local'):
    """
    publish given resources to wps output path and return url to access resource.
    """
    
    urls = []
    
    output_path = config.getValue('outputPath')
    ouput_url = config.getValue('ouputUrl')
    
    import uuid
    publish_id = "publish-%s" % uuid.uuid4()
    from os.path import join, basename
    publish_path = join(output_path, publish_id)
    try:
        import os
        os.mkdir(publish_path)

        for resource in resources:
            new_path = join(publish_path, basename(resource))
            os.link(resource, new_path)
            urls.append(join(output_url, basename(resource)))
    except:
        logger.exception('publish has failed.')

    return urls
