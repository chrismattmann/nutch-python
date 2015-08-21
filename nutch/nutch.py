#!/usr/bin/env python2.7
# encoding: utf-8
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 

USAGE = """
A simple python client for Nutch using the Nutch server REST API.
Most commands return results in JSON format by default, or plain text.

To control Nutch, use:

-- from nutch.nutch import Nutch
-- nt = Nutch(crawlId,                           # name your crawl
              confId='default',                  # pick a known config. file
              urlDir='url/',                     # directory containing the seed URL list
              serverEndpoint='localhost:8001',   # endpoint where the Nutch server is running
              **args                             # additional key=value pairs to submit as args
             )
-- response, status = nt.<command>(**args)       # where commmand is in the set
                                                 # ['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB']
 or
-- status, response = nt.crawl(**args)    # will run the commands in order with echoing of reponses

Commands (which become Hadoop jobs):
  inject   - inject a URL seed list into a named crawl
  generate - general URL list
  fetch    - fetch inital set of web pages
  parse    - parse web pages and invoke Tika metadata extraction
  updatedb - update the crawl database

To get/set the configuration of the Nutch server, use:
-- nt.configGetList()                    # get list of named configurations
-- nt.configGetInfo(id)                  # get parameters in named config.
-- nt.configCreate(id, parameterDict)    # create a new named config.

To see the status of jobs, use:
-- nt.jobGetList()                       # get list of running jobs
-- nt.jobGetInfo(id)                     # get metadata for a job id
-- nt.jobStop(id)                        # stop a job, DANGEROUS!!, may corrupt segment files

"""

import sys, os, time, getopt, json
import requests

ServerHost = "localhost"
Port = "8081"
ServerEndpoint = 'http://' + ServerHost + ':' + Port
DefaultConfig = 'default'
LegalJobs = ['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB', 'CRAWL']

TextResponseHeader = {'Accept': 'text/plain'}
JsonResponseHeader = {'Accept': 'application/json'}

Verbose = 0
Mock = 0
def echo2(*s): sys.stderr.write('nutch.py: ' + ' '.join(map(str, s)) + '\n')
def warn(*s):  echo2('Warn:', *s)
def die(*s):   warn('Error:',  *s); echo2(USAGE); sys.exit()


class Nutch:
    def __init__(self, crawlId, confId=DefaultConfig, urlDir='url/', serverEndpoint=ServerEndpoint,
                 **args):
        '''Nutch class to hold various crawl & configuraton state and methods to call the REST API.

Provides functions:
    server - getServerStatus, stopServer
    config - get list of configurations, get config. dict, get an individual config. parameter,
             and create a new named configuration.
    job - get list of running jobs, get job metadata, stop/abort a job by id, and create a new job

To start a crawl job, use:
    jobCreate - or use the methods inject, generate, fetch, parse, updatedb in that order.

To run a crawl in one method, use:
-- nt = Nutch(crawlId, configId, urlDir, **args)
-- response, status = nt.crawl(**args)

Methods return a tuple of two items, the response content (JSON or text) and the response status.
        '''
        self.crawlId = crawlId                  # id for this crawl
        self.confId = confId                    # a named configuration (XML) file
        self.urlDir = urlDir                    # directory containing the URL seed list ??
        self.serverEndpoint = serverEndpoint    # server endpoint as host:port
        self.parameters = {}
        self.parameters['crawlId'] = crawlId
        if 'url_dir' not in args: args['url_dir'] = urlDir
        self.parameters['confId'] = confId
        self.parameters['args'] = args     # additional config. args as a dictionary

    def getServerStatus(self):
        return callServer('get', self.serverEndpoint + '/admin')
    def stopServer(self):
        return callServer('post', self.serverEndpoint + '/admin/stop', headers=TextResponseHeader)

    def configGetList(self):     return Config().getList()
    def configGetInfo(self, id): return Config(id).getInfo()
    def configGetParameter(self, id, parameterId): return Config(id).getParameter(parameterId)

    def configCreate(self, id, config, **args): return Config(id).create(config, **args)
    
    def jobGetList(self):     return Job().getList()
    def jobGetInfo(self, id): return Job(id).getInfo()
    def jobStop(self, id):    return Job(id).stop()
    def jobAbort(self, id):   return Job(id).abort()

    def jobCreate(self, command, **args):
        params = self.parameters            # top-level params dict from Nutch object
        params['args'].update(args)         # add extra args in args sub-dictionary
        echo2('Creating', command.upper(), 'job for %s, %s.' % (self.crawlId, self.confId))
        if command == 'crawl':
            return self.crawl(**args)
        else:
            return Job(None, params).create(command, **args)

    def inject  (self, **args):  return self.jobCreate('INJECT',   **args)
    def generate(self, **args):  return self.jobCreate('GENERATE', **args)
    def fetch   (self, **args):  return self.jobCreate('FETCH',    **args)
    def parse   (self, **args):  return self.jobCreate('PARSE',    **args)
    def updatedb(self, **args):  return self.jobCreate('UPDATEDB', **args)
    def whatelse(self, **args):  return self.jobCreate('WHATELSE', **args)   # what else belong here ??

    def crawl(self, crawlCycle=['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB'], **args):
        '''Run a full crawl cycle, adding given extra args to the configuration.'''
        for step in crawlCycle:
            (job, status) = self.jobCreate(step, **args)
            if status != 200:
                die('Could not start %s on server %s, Aborting.' % (step, self.serverEndpoint))
            time.sleep(1)
            print self.jobGetInfo(job.id)[0]
            print >>sys.stderr, '\nPress return to proceed to next step.\n'
            sys.stdin.read(1)
            

class Config:
    '''Nutch Config class with methods to get the list of named configurations,
get parameters for a named configuration, get an individual parameter of a named configuration,
create a new named configuration using a parameter dictionary, and delete a named configuration.
    '''
    def __init__(self, id, serverEndpoint=ServerEndpoint):
        self.id = id
        self.serverEndpoint = serverEndpoint

    def getList(self):
        return callServer('get', self.serverEndpoint+'/config')

    def getInfo(self, id=None):
        if id is None: id = self.id
        return callServer('get', self.serverEndpoint + '/config/' + id)
    
    def getParameter(self, parameterId, id=None):
        if id is None: id = self.id
        return callServer('get', self.serverEndpoint + '/config/%s/%s' % (id, parameterId))
    
    def create(self, id, config, **args):
        '''Create a new named (id) configuration from a parameter dictionary (config).'''
        config.update(args)
        (id, status) = callServer('post', self.serverEndpoint+"/config/%s" % id, config,
                                  TextResponseHeader)
        self.id = id
        return (self, status)

    def delete(self, id=None):
        if id is None: id = self.id
        return callServer('delete', self.serverEndpoint + '/config/' + id)


class Job:
    '''Nutch Job class with methods to getList of jobs, getInfo for a job, create a new job,
stop a job, and abort a job.
    '''
    def __init__(self, id=None, parameters={}, serverEndpoint=ServerEndpoint):
        self.id = id
        self.parameters=parameters
        self.serverEndpoint = serverEndpoint
        
    def getList(self):
        return callServer('get', self.serverEndpoint+'/job')

    def getInfo(self, id=None):
        if id is None: id = self.id
        return callServer('get', self.serverEndpoint + '/job/' + id)
    
    def create(self, command, **args):
        command = command.upper()
        if command not in LegalJobs:
            warn('Nutch command must be one of: %s' % LegalJobs.join(', '))
        else:
            echo2('Starting %s job with args %s' % (command, str(args)))
        parameters = self.parameters
        parameters['args'].update(args)
        parameters['type'] = command
        (id, status) = callServer('post', self.serverEndpoint+"/job/create", parameters,
                                   TextResponseHeader)
        self.id = id
        return (self, status)

    def stop(self):
        return callServer('post', self.serverEndpoint+'/job/%s/stop' % self.id, TextResponseHeader) 

    def abort(self):
        return callServer('post', self.serverEndpoint+'/job/%s/abort' % self.id, TextResponseHeader) 


def callServer(verb, serviceUrl, data={}, headers=JsonResponseHeader, mock=False,
               httpVerbs={'get': requests.get, 'put': requests.put, 'post': requests.post,
                          'delete': requests.delete}):
    """Call the Nutch Server, do some error checking, and return the response."""
    global Verbose, Mock
    if verb not in httpVerbs:
        die('Server call verb must be one of %s' % str(httpVerbs.keys()))
    if Verbose:
        echo2("%s Request data:" % verb.upper(), data)
        echo2("%s Request headers:" % verb.upper(), headers)
    data = json.dumps(data)
    verbFn = httpVerbs[verb]
    if Mock: return ('mock', 200)
    resp = verbFn(serviceUrl, data, headers=headers)
    if Verbose:
        echo2("Response headers:", resp.headers)
    if resp.status_code != 200:
        warn('Nutch server returned status:', resp.status_code)
    return (resp.content, resp.status_code)


def main(argv=None):
    """Run Nutch command using REST API."""
    global Verbose, Mock
    if argv is None:
        argv = sys.argv

    if len(argv) < 5: die('Bad args')
    try:
        opts, argv = getopt.getopt(argv[1:], 'hs:p:mv',
          ['help', 'server=', 'port=', 'mock', 'verbose'])
    except getopt.GetoptError, (msg, bad_opt):
        die("%s error: Bad option: %s, %s" % (argv[0], bad_opt, msg))
        
    serverEndpoint = ServerEndpoint
    port = Port
    for opt, val in opts:
        if opt   in ('-h', '--help'):    echo2(USAGE); sys.exit()
        elif opt in ('-s', '--server'):  serverEndpoint = val
        elif opt in ('-p', '--port'):    serverEndpoint = 'http://localhost:%s' % val
        elif opt in ('-m', '--mock'):    Mock = 1
        elif opt in ('-v', '--verbose'): Verbose = 1
        else: die(USAGE)

    cmd = argv[0]
    crawlId = argv[1]
    confId = argv[2]
    urlDir = argv[3]
    args = {}
    if len(argv) > 4: args = eval(argv[4])

    nt = Nutch(crawlId, confId, urlDir, serverEndpoint)
    resp = nt.jobCreate(cmd, **args)


if __name__ == '__main__':
    resp = main(sys.argv)
    print resp[0]


# python nutch.py inject crawl01 default "url/" "{'foo': 'bar'}"

