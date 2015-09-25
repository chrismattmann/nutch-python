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

import collections
import getopt
from getpass import getuser
import sys
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
    status_code = None

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

class Server:
    """
    Implements basic interactions with a Nutch RESTful Server
    """

    def __init__(self, serverEndpoint, raiseErrors=True):
        """
        Create a Server object for low-level interactions with a Nutch RESTful Server

        :param serverEndpoint: URL of the server
        :param raiseErrors: Raise an exception for non-200 status codes

        """
        self.serverEndpoint = serverEndpoint
        self.raiseErrors = raiseErrors


    def call(self, verb, servicePath, data=None, headers=JsonAcceptHeader, forceText=False):
        """Call the Nutch Server, do some error checking, and return the response.

        :param verb: One of nutch.RequestVerbs
        :param servicePath: path component of URL to append to endpoint, e.g. '/config'
        :param data: Data to attach to this request
        :param headers: headers to attach to this request
        """

        data = data if data else {}

        if verb not in RequestVerbs:
            die('Server call verb must be one of %s' % str(RequestVerbs.keys()))
        if Verbose:
            echo2("%s Endpoint:" % verb.upper(), servicePath)
            echo2("%s Request data:" % verb.upper(), data)
            echo2("%s Request headers:" % verb.upper(), headers)
        verbFn = RequestVerbs[verb]

        resp = verbFn(self.serverEndpoint + servicePath, json=data, headers=headers)
        if Verbose:
            echo2("Response headers:", resp.headers)
            echo2("Response status:", resp.status_code)
        if resp.status_code != 200:
            if self.raiseErrors:
                error = NutchException("Unexpected server response: %d" % resp.status_code)
                error.status_code = resp.status_code
                raise error
            else:
                warn('Nutch server returned status:', resp.status_code)
        content_type = resp.headers['content-type']
        if content_type == 'application/json' and not forceText:
            return resp.json()
        elif content_type == 'application/text' or forceText:
            return resp.text
        else:
            die('Did not understand server response: %s' % resp.headers)

defaultServer = lambda: Server(DefaultServerEndpoint)

class IdEqualityMixin(object):
    """
    Mix-in class to use self.id == other.id to check for equality
    """
    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.id == other.id)

    def __ne__(self, other):
        return not self.__eq__(other)


class Job(IdEqualityMixin):
    """
    Representation of a running Nutch job, use JobClient to get a list of running jobs or to create one
    """

    def __init__(self, jid, server):
        self.id = jid
        self.server = server

    def info(self):
        """Get current information about this job"""
        return self.server.call('get', '/job/' + self.id)

    def stop(self):
        return self.server.call('get', '/job/%s/stop' % self.id)

    def abort(self):
        return self.server.call('get', '/job/%s/abort' % self.id)


class Config(IdEqualityMixin):
    """
    Representation of an active Nutch configuration

    Use ConfigClient to get a list of configurations or create a new one
    """

    def __init__(self, cid, server):
        self.id = cid
        self.server = server

    def delete(self):
        return self.server.call('delete', '/config/' + self.id)

    def info(self):
        return self.server.call('get', '/config/' + self.id)

    def parameter(self, parameterId):
        return self.server.call('get', '/config/%s/%s' % (self.id, parameterId))


class Seed(IdEqualityMixin):
    """
    Representation of an active Nutch seed list

    Use SeedClient to get a list of seed lists or create a new one
    """

    def __init__(self, sid, seedPath, server):
        self.id = sid
        self.seedPath = seedPath
        self.server = server


class ConfigClient:
    def __init__(self, server):
        """Nutch Config client

        List named configurations, create new ones, or delete them with methods to get the list of named
        configurations, get parameters for a named configuration, get an individual parameter of a named
        configuration, create a new named configuration using a parameter dictionary, and delete a named configuration.
        """
        self.server = server

    def list(self):
        configs = self.server.call('get', '/config')
        return [Config(cid, self.server) for cid in configs]

    def create(self, cid, configData):
        """
        Create a new named (cid) configuration from a parameter dictionary (config_data).
        """

        cid = self.server.call('post', "/config/%s" % cid, configData, TextAcceptHeader)
        new_config = Config(cid, self.server)
        return new_config

    def __getitem__(self, item):
        """
        Overload [] to provide get access to configurations
        :param item: the name of a configuration
        :return: the Config object if the name is valid, otherwise raise KeyError
        """

        # let's be optimistic...
        config = Config(item, self.server)
        if config.info():
            return config

        # not found!
        raise KeyError

    def __setitem__(self, key, value):
        """
        Overload [] to provide set access to configurations
        :param key: the name of the configuration to create
        :param value: the dict-like data associated with this configuration
        :return: the created Config object
        """

        if not isinstance(value, collections.Mapping):
            raise TypeError(repr(value) + "is not a dict-like object")
        return self.create(key, value)

class JobClient:
    def __init__(self, server, crawlId, confId, parameters=None):
        """
        Nutch Job client with methods to list and create jobs.

        When the client is created, a crawlID and confID are associated.
        The client will automatically filter out jobs that do not match the associated crawlId or confId.
        :param server:
        :param crawlId:
        :param confId:
        :param parameters:
        :return:
        """

        self.server = server
        self.crawlId = crawlId
        self.confId = confId
        self.parameters=parameters if parameters else {'args': dict()}

    def _job_owned(self, job):
        return job['crawlId'] == self.crawlId and job['confId'] == self.confId

    def list(self, allJobs=False):
        """
        Return list of jobs at this endpoints.

        Call get(allJobs=True) to see all jobs, not just the ones managed by this Client
        """

        jobs = self.server.call('get', '/job')

        return [Job(job['id'], self.server) for job in jobs if allJobs or self._job_owned(job)]

    def create(self, command, **args):
        """
        Create a job given a command
        :param command: Nutch command, one of nutch.LegalJobs
        :param args: Additional arguments to pass to the job
        :return: The job id
        """

        command = command.upper()
        if command not in LegalJobs:
            warn('Nutch command must be one of: %s' % ', '.join(LegalJobs))
        else:
            echo2('Starting %s job with args %s' % (command, str(args)))
        parameters = self.parameters.copy()
        parameters['type'] = command
        parameters['crawlId'] = self.crawlId
        parameters['confId'] = self.confId
        parameters['args'].update(args)

        job_info = self.server.call('post', "/job/create", parameters, JsonAcceptHeader)

        job = Job(job_info['id'], self.server)
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

class SeedClient():

    def __init__(self, server):
        """Nutch Seed client

        Client for uploading seed lists to Nutch
        """
        self.server = server

    def create(self, sid, seedList):
        """
        Create a new named (sid) Seed from a list of seed URLs

        :param sid: the name to assign to the new seed list
        :param seedList: the list of seeds to use
        :return: the created Seed object
        """

        seedUrl = lambda uid, url: {"id": uid, "url": url, "seedList": None}

        seedListData = {
            "id": "12345",
            "name": sid,
            "seedUrls": [seedUrl(uid, url) for uid, url in enumerate(seedList)]
        }

        # see https://issues.apache.org/jira/browse/NUTCH-2123
        seedPath = self.server.call('post', "/seed/create", seedListData, JsonAcceptHeader, forceText=True)
        new_seed = Seed(sid, seedPath, self.server)
        return new_seed

class Nutch:
    def __init__(self, confId=DefaultConfig, serverEndpoint=DefaultServerEndpoint, raiseErrors=True, **args):
        '''
        Nutch client for interacting with a Nutch instance over its REST API.

        Constructor:

        nt = Nutch()

        Optional arguments:

        confID - The name of the default configuration file to use, by default: nutch.DefaultConfig
        serverEndpoint - The location of the Nutch server, by default: nutch.DefaultServerEndpoint
        raiseErrors - raise exceptions if server response is not 200

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
        self.server = Server(serverEndpoint, raiseErrors)
        self.job_parameters = dict()
        self.job_parameters['confId'] = confId
        self.job_parameters['args'] = args     # additional config. args as a dictionary

    def Jobs(self, crawlId=None):
        """
        Create a JobClient for listing and creating jobs.
        The JobClient inherits the confId from the Nutch client.

        :param crawlId: crawlIds to use for this client.  If not provided, will be generated
         by nutch.defaultCrawlId()
        :return: a JobClient
        """
        crawlId = crawlId if crawlId else defaultCrawlId()
        return JobClient(self.server, crawlId, self.confId)

    def Configs(self):
        return ConfigClient(self.server)

    def Seeds(self):
        return SeedClient(self.server)

    ## convenience functions
    ## TODO: Decide if any of these should be deprecated.
    def getServerStatus(self):
        return self.server.call('get', '/admin')

    def stopServer(self):
        return self.server.call('post', '/admin/stop', headers=TextAcceptHeader)

    def configGetList(self):
        return self.Configs().list()

    def configGetInfo(self, cid):
        return Config(cid, self.server).info()

    def configGetParameter(self, cid, parameterId):
        return Config(cid, self.server).parameter(parameterId)

    def configCreate(self, cid, config_data):
        return self.Configs().create(cid, config_data)

    def crawl(self, crawlCycle=None, **args):
        '''Run a full crawl cycle, adding given extra args to the configuration.'''

        crawlCycle= crawlCycle if crawlCycle is not None else ['INJECT', 'GENERATE', 'FETCH', 'PARSE', 'UPDATEDB']
        jobClient = self.Jobs()

        for step in crawlCycle:
            job = jobClient.create(step, **args)
            print(job)

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

    nt = Nutch(crawlId, confId, serverEndpoint, urlDir)
    nt.Jobs().create(cmd, **args)


if __name__ == '__main__':
    resp = main(sys.argv)
    print(resp[0])
