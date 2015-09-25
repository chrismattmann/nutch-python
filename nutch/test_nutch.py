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

def get_nutch():
    return nutch.Nutch()


def get_job_client():
    return get_nutch().Jobs()


def test_nutch_constructor():
    nt = get_nutch()
    assert nt


def test_job_client_constructor():
    jc = get_job_client()
    assert jc


def test_job_start():
    jc = get_job_client()
    old_jobs = jc.list()
    inject_job = jc.inject()
    updated_jobs = jc.list()
    assert(len(updated_jobs) == len(old_jobs) + 1)

    # awesome functionality for checking if this job is in a list of jobs
    assert(inject_job not in old_jobs)
    assert(inject_job in updated_jobs)


def test_job_client_lists():
    # the default constructor uses a timestamp to create unique crawlIds
    jc1 = get_job_client()
    jc2 = get_job_client()

    jc1_job = jc1.inject()

    # only jobs with the same crawlId are returned in the list()
    assert jc1_job in jc1.list()
    assert jc1_job not in jc2.list()

    # unless allJobs=True is passed to the list() function
    assert jc1_job in jc2.list(allJobs=True)


def test_job_inject():
    nt = get_nutch()
    jc = nt.Jobs()
    inject_job = jc.inject()
    job_info = inject_job.info()
    assert job_info['type'] == 'INJECT'

    # jobs have the same configuration as the Nutch instance
    assert(inject_job.info()['confId'] == nt.confId)


def test_job_stop():
    jc = get_job_client()
    inject_job = jc.inject()
    inject_job.stop()
    # bad jobs will eventually enter the 'FAILED' state
    # is there a better test here?
    assert(inject_job.info()['state'] == 'STOPPING')


def test_job_abort():
    jc = get_job_client()
    inject_job = jc.inject()
    inject_job.abort()
    assert(inject_job.info()['state'] == 'KILLED')

# How do we delete jobs using the REST API?  Is it even possible?
