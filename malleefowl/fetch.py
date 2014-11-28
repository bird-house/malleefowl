"""
fetchs files from esgf using malleefowl wps services (wget, esgf_logon)
"""

import logging

from owslib.wps import WebProcessingService, monitorExecution

def fetch(service, username, password, provider, filelist):
    cert_url = get_cert(service, username, password, provider)

def get_cert(service, username, password, provider):
    wps = WebProcessingService(service, verbose=False, skip_caps=True)

    openid = "https://esgf-data.dkrz.de/esgf-idp/openid/%s" % (username)

    inputs = [('openid', openid), ('password', password)]
    outputs = [('output', True)]

    logging.info('Getting ESGF certificate for OpenID %s ...', openid)
    
    try:
        execution = wps.execute("esgf_logon", inputs=inputs, output=outputs)
        monitorExecution(execution, sleepSecs=5)
        if execution.isSucceded():
            cert_url = execution.processOutputs[0].reference
            logging.info('ESGF certificate successfully retrieved.')
        else:
            logging.error('Could not get certificate from your esgf provider.')
            for ex in execution.errors:
                loggging.error('code=%s, locator=%s, text=%s' % (ex.code, ex.locator, ex.text))
            exit(1)
    except Exception as e:
        logging.error('Could not get certificate from your esgf provider.')
        logging.debug(e.message)
        exit(1)
    return cert_url

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

    args = parser.parse_args()
    password = args.password
    if password is None:
        import getpass
        password = getpass.getpass('Enter Password of your OpenID: ')
    
    fetch(args.service,
          args.username,
          password,
          args.provider,
          filelist=None)
