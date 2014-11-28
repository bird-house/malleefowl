"""
fetchs files from esgf using malleefowl wps services (wget, esgf_logon)
"""

import logging

from owslib.wps import WebProcessingService, monitorExecution

class FetchException(Exception):
    pass


def fetch(service, username, password, provider, filename, output):
    wps = None
    try:
        wps = WebProcessingService(service, verbose=False, skip_caps=True)
    except:
        logger.exception("Could not access WPS %s", service)
        raise
    filelist_json = wget(wps,
         cert_url=get_cert(wps, username, password, provider),
         files=load_files(filename))
    dump_files(filelist_json, output)

def load_files(filename):
    files = None
    try:
        import json
        with open(filename, 'r') as fp:
            files = json.load(fp)
            logging.debug(files)
    except:
        logging.exception("Could not read file list: %s", filename)
        raise
    return files

def get_cert(wps, username, password, provider):
    openid = "https://esgf-data.dkrz.de/esgf-idp/openid/%s" % (username)

    inputs = [('openid', openid), ('password', password)]
    outputs = [('output', True)]

    logging.info('Getting ESGF certificate for OpenID %s ...', openid)
    
    try:
        execution = wps.execute("esgf_logon", inputs=inputs, output=outputs)
        monitorExecution(execution, sleepSecs=3)
        if execution.isSucceded():
            cert_url = execution.processOutputs[0].reference
            logging.info('ESGF certificate successfully retrieved.')
        else:
            logging.error('Could not get certificate from your esgf provider.')
            for ex in execution.errors:
                logging.error('code=%s, locator=%s, text=%s' % (ex.code, ex.locator, ex.text))
            raise Exception()
    except:
        logging.exception('Could not get certificate from your esgf provider.')
        raise
    return cert_url

def wget(wps, cert_url, files):
    inputs = [('credentials', cert_url)]
    for file_url in files:
        inputs.append( ('resource', str(file_url)) )
    outputs = [('output', True), ('output_external', True)]

    logging.info('Retrieving files ...')
    
    try:
        execution = wps.execute("wget", inputs=inputs, output=outputs)
        monitorExecution(execution, sleepSecs=10)
        if execution.isSucceded():
            import json
            filelist = json.loads(execution.processOutputs[0].retrieveData())
            logging.info('Files succesfully retrieved.')
        else:
            logging.error('Could not retrieve files.')
            for ex in execution.errors:
                logging.error('code=%s, locator=%s, text=%s' % (ex.code, ex.locator, ex.text))
            raise FetchException()
    except:
        logging.exception('Could not retrieve files.')
        raise
    return filelist

def dump_files(filelist, output):
    try:
        import json
        from os.path import basename
        from urlparse import urlparse
        file_dict = {}
        for file_url in filelist:
            filename = basename(urlparse(file_url).path)
            file_dict[filename] = file_url 
    
        with open(output, 'w') as fp:
            json.dump(file_dict, fp, indent=4, sort_keys=True)
    except:
        logging.exception('Could not write %s', output)
        raise
    logging.info("unit test file list written, %s", output)
    
def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    
    import argparse

    parser = argparse.ArgumentParser(description='Fetches files from ESGF with your OpenID.')
    parser.add_argument('-v', '--verbose',
                        dest="verbose",
                        default=False,
                        action="store_true",
                        help="verbose mode (default: False)"
                      )
    parser.add_argument('-s', '--service',
                        dest="service",
                        default="http://localhost:8091/wps",
                        action="store",
                        help="Malleefowl WPS service url (default: http://localhost:8091/wps)")

    parser.add_argument('--provider',
                        dest="provider",
                        default="DKRZ",
                        action="store",
                        help="Your ESGF OpenID provider (default: DKRZ)")

    parser.add_argument('-u', '--user',
                        dest="username",
                        action="store",
                        help="Username of your ESGF OpenID")

    parser.add_argument('-p', '--password',
                        dest="password",
                        default=None,
                        action="store",
                        help="Password of your ESGF OpenID (default: enter interactivly)")

    parser.add_argument('--files',
                        dest="files",
                        default="testdata.json",
                        action="store",
                        help="JSON document with testdata urls (default: testdata.json)")

    parser.add_argument('--output',
                        dest="output",
                        default="unit_tests/testdata.json",
                        action="store",
                        help="Output: JSON document with testdata for unit tests (default: unit_tests/testdata.json)")


    args = parser.parse_args()
    password = args.password
    if password is None:
        import getpass
        password = getpass.getpass('Enter Password of your OpenID: ')


    try:
        fetch(args.service,
              args.username,
              password,
              args.provider,
              filename=args.files,
              output=args.output)
    except:
        logging.exception("Could not fetch files.")
