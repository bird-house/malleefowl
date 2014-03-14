Bugs ...
=========

* fix userid bug in wps with @ sign (needed for tokenmgr)
* phoenix: logging has double entries
* fix thredds install
* MyProxyClient: myProxyClient does not work with myOpenSSL=0.14 (23.2.2014), logon fails
* phoenix: enter "!" in text field breaks wps execution
  seams to be an owslib wps bug, need test case
* DONE: fix restflow python env
* supervisor: fix reload of supervisor on error and on update configs
* mongodb: sometimes shutdown fails and lock file is kept. make sure that on update mongodb will be restarted. 
* DONE: phoenix: log still with root permissions
* phoenix: exception in wizard when now local files available and local search selected
* mr.developer: git checkout of qc-processes fails with submodule init error
* restflow does not work with anaconda python
* IE (at least some versions) do not work with Phoenix (pyramid framework)
* check restflow (yaml not found) error (get ...?) with mutliple files 
  happens when previous download process has failed
* fix empty esg search query in wizard
* fix adding geoserver wps
* map uses only local wps (not currently active one)
* esgf logon does not work when X509 variables are set
* phoenix does not show exception text when exception occured. 
  Shows just default: status.xml can not be accessed.
* wps client fails when transmitting file (complex input) ...
