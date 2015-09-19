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

from __future__ import print_function
from __future__ import division

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

import getopt
from getpass import getuser
import json
import sys
import time
from datetime import datetime
import requests

DefaultServerHost = "localhost"
DefaultPort = "8081"
DefaultServerEndpoint = 'http://' + DefaultServerHost + ':' + DefaultPort
DefaultConfig = 'default'

LegalJobs = ['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB', 'CRAWL']
RequestVerbs = {'get': requests.get, 'put': requests.put, 'post': requests.post, 'delete': requests.delete}

TextAcceptHeader = {'Accept': 'text/plain'}
JsonAcceptHeader = {'Accept': 'application/json'}

class NutchException(Exception):
    pass

# TODO: Replace with Python logger
Verbose = True
def echo2(*s): sys.stderr.write('nutch.py: ' + ' '.join(map(str, s)) + '\n')
def warn(*s):  echo2('Warn:', *s)
def die(*s):   echo2('Error:',  *s); echo2(USAGE); sys.exit()

def defaultCrawlId():
    """
    Provide a reasonable default crawl name using the user name and date
    """

    now = datetime.now()
    user = getuser()
    return '_'.join(('crawl', user, now.isoformat()))


class Job:
    """
    Representation of a running Nutch job, use JobClient to get a list of running jobs or to create one
    """

    def __init__(self, jid, serverEndpoint=DefaultServerEndpoint):
        self.id = jid
        self.serverEndpoint = serverEndpoint

    def info(self):
        """Get current information about this job"""

        # need to unpack this for some reason...
        return callServer('get', self.serverEndpoint + '/job/' + self.id)[0]

    def stop(self):
        return callServer('get', self.serverEndpoint+'/job/%s/stop' % self.id)

    def abort(self):
        return callServer('get', self.serverEndpoint+'/job/%s/abort' % self.id)


class ConfigClient:
    '''Nutch Config client with methods to get the list of named configurations,
get parameters for a named configuration, get an individual parameter of a named configuration,
create a new named configuration using a parameter dictionary, and delete a named configuration.
    '''
    def __init__(self, id, serverEndpoint=DefaultServerEndpoint):
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
                                  TextAcceptHeader)
        self.id = id
        return (self, status)

    def delete(self, id=None):
        if id is None: id = self.id
        return callServer('delete', self.serverEndpoint + '/config/' + id)


class JobClient:
    '''Nutch Job client with methods to list and create jobs.

    When the client is created, a crawlID and confID are associated.

    The client will automatically filter out jobs that do not match the associated crawlId or confId.
    '''
    def __init__(self, serverEndpoint, crawlId, confId, errorsFatal=False, parameters=None):
        """
        :param serverEndpoint:
        :param crawlId:
        :param confId:
        :param parameters:
        :return:
        """
        self.serverEndpoint = serverEndpoint
        self.crawlId = crawlId
        self.confId = confId
        self.errorsFatal = errorsFatal
        self.parameters=parameters if parameters else {'args': dict()}

    def _job_owned(self, job):
        return job['crawlId'] == self.crawlId and job['confId'] == self.crawlId

    def _check_status(self, status):
        if self.errorsFatal and status != 200:
            raise NutchException("Unexpected server response: %d" % status)

    def get(self, allJobs=False):
        """
        Return list of jobs at this endpoints.

        Call get(allJobs=True) to see all jobs, not just the ones managed by this Client
        """

        (jobs, status) = callServer('get', self.serverEndpoint+'/job')
        self._check_status(status)

        return [Job(job['id'], self.serverEndpoint) for job in jobs if allJobs or self._job_owned(job)]


    def create(self, command, **args):
        """
        Create a job given a command and a crawlID
        :param command: Nutch command, one of nutch.LegalJobs
        :param args: Additional arguments to pass to the job
        :return: The job id
        """

        command = command.upper()
        if command not in LegalJobs:
            warn('Nutch command must be one of: %s' % ', '.join(LegalJobs.join))
        else:
            echo2('Starting %s job with args %s' % (command, str(args)))
        parameters = self.parameters.copy()
        parameters['type'] = command
        parameters['crawlId'] = self.crawlId
        parameters['confId'] = self.confId
        parameters['args'].update(args)

        (job_info, status) = callServer('post', self.serverEndpoint+"/job/create", parameters,
                                   JsonAcceptHeader)
        self._check_status(status)

        job = Job(job_info['id'], self.serverEndpoint)
        return job

    # some short-hand functions

    def inject(self, **args):
        return self.create('INJECT', **args)

    def generate(self, **args):
        return self.create('GENERATE', **args)

    def fetch(self, **args):
        return self.create('FETCH', **args)

    def parse(self, **args):
        return self.create('PARSE', **args)

    def updatedb(self, **args):
        return self.create('UPDATEDB', **args)


class Nutch:
    def __init__(self, confId=DefaultConfig, serverEndpoint=DefaultServerEndpoint,
                 **args):
        '''
        Nutch client for interacting with a Nutch instance over its REST API.

        Constructor:

        nt = Nutch()

        Optional arguments:

        serverEndpoint - The server endpoint to use, by default: nutch.DefaultServerEndpoint
        confID - The name of the default configuration file to use, by default: nutch.DefaultConfig

        Provides functions:
            server - getServerStatus, stopServer
            config - get list of configurations, get config. dict, get an individual config. parameter,
                     and create a new named configuration.
            job - get list of running jobs, get job metadata, stop/abort a job by id, and create a new job

        To start a crawl job, use:
            jobCreate - or use the methods inject, generate, fetch, parse, updatedb in that order.

        To run a crawl in one method, use:
        -- nt = Nutch()
        -- response, status = nt.crawl()

        Methods return a tuple of two items, the response content (JSON or text) and the response status.
        '''

        self.confId = confId
        self.serverEndpoint = serverEndpoint
        self.job_parameters = dict()
        self.job_parameters['confId'] = confId
        self.job_parameters['args'] = args     # additional config. args as a dictionary

    def Jobs(self, crawlId=defaultCrawlId()):
        """
        Create a JobClient for listing and creating jobs.
        The JobClient inherits the confId from the Nutch client.

        :param crawlId: crawlIds to use for this client.  If not provided, will be generated
         by nutch.defaultCrawlId()
        :return: a JobClient
        """
        return JobClient(self.serverEndpoint, crawlId, self.confId)

    def getServerStatus(self):
        return callServer('get', self.serverEndpoint + '/admin')


    def stopServer(self):
        return callServer('post', self.serverEndpoint + '/admin/stop', headers=TextAcceptHeader)


    def configGetList(self):
        return ConfigClient().getList()


    def configGetInfo(self, id):
        return ConfigClient(id).getInfo()


    def configGetParameter(self, id, parameterId):
        return ConfigClient(id).getParameter(parameterId)

    def configCreate(self, id, config, **args): return ConfigClient(id).create(config, **args)


    def crawl(self, crawlCycle=['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB'], **args):
        '''Run a full crawl cycle, adding given extra args to the configuration.'''

        jobClient = self.Jobs(errorsFatal=True)

        for step in crawlCycle:
            (job, status) = jobClient.create(step, **args)
            if status != 200:
                die('Could not start %s on server %s, Aborting.' % (step, self.serverEndpoint))
            time.sleep(1)
            print(self.jobGetInfo(job.id)[0])
            print('\nPress return to proceed to next step.\n', file=sys.stderr)
            sys.stdin.read(1)


def callServer(verb, serviceUrl, data=None, headers=JsonAcceptHeader):
    """Call the Nutch Server, do some error checking, and return the response."""

    data = data if data else {}

    if verb not in RequestVerbs:
        die('Server call verb must be one of %s' % str(RequestVerbs.keys()))
    if Verbose:
        echo2("%s Endpoint:" % verb.upper(), serviceUrl)
        echo2("%s Request data:" % verb.upper(), data)
        echo2("%s Request headers:" % verb.upper(), headers)
    verbFn = RequestVerbs[verb]

    resp = verbFn(serviceUrl, json=data, headers=headers)
    if Verbose:
        echo2("Response headers:", resp.headers)
    if resp.status_code != 200:
        warn('Nutch server returned status:', resp.status_code)
    content_type = resp.headers['content-type']
    if content_type == 'application/json':
        return (resp.json(), resp.status_code)
    elif content_type == 'application/text':
        return (resp.text(), resp.status_code)
    else:
        die('Did not understand server response: %s' % resp.headers)

def main(argv=None):
    """Run Nutch command using REST API."""
    global Verbose, Mock
    if argv is None:
        argv = sys.argv

    if len(argv) < 5: die('Bad args')
    try:
        opts, argv = getopt.getopt(argv[1:], 'hs:p:mv',
          ['help', 'server=', 'port=', 'mock', 'verbose'])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        die()
        
    serverEndpoint = DefaultServerEndpoint
    port = Port

    # TODO: Fix this
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
    print(resp[0])
