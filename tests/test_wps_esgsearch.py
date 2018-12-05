import pytest
from pywps import Service
from pywps.tests import assert_response_success

from .common import TESTDATA, client_for

from malleefowl.processes.wps_esgsearch import ESGSearchProcess


@pytest.mark.online
def test_dataset():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '10', '10',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_dataset_with_spaces():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '10', '10',
        ' project: CORDEX, time_frequency : mon,variable:tas,  experiment:historical  ')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_dataset_out_of_limit():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '100', '99',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_dataset_out_of_offset():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '1', '1000',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_dataset_latest():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={};latest={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '100', '0',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical',
        'False')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_dataset_query():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={};query={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Dataset', '1', '0',
        'project:CORDEX',
        'geopotential')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_aggregation():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'Aggregation', '5', '20',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)


@pytest.mark.online
def test_file():
    client = client_for(Service(processes=[ESGSearchProcess()]))
    datainputs = "url={};search_type={};limit={};offset={};constraints={}".format(
        'http://esgf-data.dkrz.de/esg-search',
        'File', '1', '30',
        'project:CORDEX,time_frequency:mon,variable:tas,experiment:historical')
    resp = client.get(
        service='WPS', request='Execute', version='1.0.0',
        identifier='esgsearch',
        datainputs=datainputs)
    assert_response_success(resp)
