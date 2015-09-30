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

# Test Nutch API
# Assumes a Nutch REST server is running on localhost
# TODO: Package into Travis tests

import nutch
import pytest
import glob
from time import sleep

slow = pytest.mark.slow

def get_nutch():
    return nutch.Nutch()


def test_nutch_constructor():
    nt = get_nutch()
    assert nt

## Configurations

def get_config_client():
    return get_nutch().Configs()

def test_config_client_constructor():
    cc = get_config_client()
    assert cc

def test_config_access():
    cc = get_config_client()
    default_config = cc['default']
    # there has to be something smarter to check here
    assert default_config.info()

def test_config_create():
    cc = get_config_client()
    cc['defaultcopy'] = {}
    assert cc['defaultcopy'].info()["db.fetch.interval.max"]

# I don't know how to get this working
def test_config_copy():
    cc = get_config_client()
    default_config = cc['default']
    default_config_data = default_config.info()
    cc['defaultcopy'] = default_config_data
    assert cc['defaultcopy'].info()["db.fetch.interval.max"]

## Seed Lists

# Fairly limited functionality for working with seed lists

def get_seed_client():
    return get_nutch().Seeds()

def test_seed_client_constructor():
    sc = get_seed_client()
    assert sc


def get_seed(seed_urls=('http://aron.ahmadia.net', 'http://www.google.com')):
    sc = get_seed_client()
    return sc.create('test_seed', seed_urls)


def test_seed_create():
    seed_urls = ('http://aron.ahmadia.net', 'http://www.google.com')
    seed = get_seed(seed_urls)
    seed_path = seed.seedPath
    with open(glob.glob(seed_path + '/*.txt')[0]) as f:
        seed_data = f.read()
    assert seed_data.split() == list(seed_urls)

## Jobs

def get_job_client():
    return get_nutch().Jobs()

def get_inject_job(jc=None):
    seed = get_seed()
    if jc is None:
        jc = get_job_client()
    return jc.inject(seed)

def test_job_client_constructor():
    jc = get_job_client()
    assert jc

def test_job_start():
    jc = get_job_client()
    old_jobs = jc.list()
    inject_job = get_inject_job(jc)
    updated_jobs = jc.list()
    assert(len(updated_jobs) == len(old_jobs) + 1)

    # awesome functionality for checking if this job is in a list of jobs
    assert(inject_job not in old_jobs)
    assert(inject_job in updated_jobs)


def test_job_client_lists():
    # the default constructor uses a timestamp to create unique crawlIds
    jc1 = get_job_client()
    jc2 = get_job_client()

    jc1_job = get_inject_job(jc1)

    # only jobs with the same crawlId are returned in the list()
    assert jc1_job in jc1.list()
    assert jc1_job not in jc2.list()

    # unless allJobs=True is passed to the list() function
    assert jc1_job in jc2.list(allJobs=True)


def test_job_inject():
    nt = get_nutch()
    inject_job = get_inject_job()
    job_info = inject_job.info()
    assert job_info['type'] == 'INJECT'
    assert job_info['msg'] == 'OK'
    # jobs have the same configuration as the Nutch instance
    assert(job_info['confId'] == nt.confId)

def test_job_generate():
    nt = get_nutch()
    # need to inject before generating...
    jc = get_job_client()
    inject = get_inject_job(jc)
    # wait until injection is done

    for wait in range(10):
        if inject.info()['state'] != 'FINISHED':
            sleep(1)
            continue
        else:
            break
    else:
        raise Exception("took too long to inject")

    assert inject.info()['state'] == 'FINISHED'

    generate = jc.generate()
    job_info = generate.info()
    assert job_info['type'] == 'GENERATE'
    assert job_info['msg'] == 'OK'
    # jobs have the same configuration as the Nutch instance
    assert(job_info['confId'] == nt.confId)


def test_job_stop():
    inject_job = get_inject_job()
    inject_job.stop()
    # bad jobs will eventually enter the 'FAILED' state
    # is there a better test here?
    assert(inject_job.info()['state'] == 'STOPPING')


def test_job_abort():
    inject_job = get_inject_job()
    inject_job.abort()
    assert(inject_job.info()['state'] == 'KILLED')
# How do we delete jobs using the REST API?  Is it even possible?

def get_crawl_client():
    seed = get_seed()
    return get_nutch().Crawl(seed)

@slow
def test_crawl_client():
    cc = get_crawl_client()
    assert cc.currentJob.info()['type'] == 'INJECT'
    rounds = cc.waitAll()
    assert len(rounds) == 1
    assert cc.currentJob is None
    jobs = rounds[0]
    assert len(jobs) == 4
    assert all([j.info()['state'] == 'FINISHED' for j in jobs])