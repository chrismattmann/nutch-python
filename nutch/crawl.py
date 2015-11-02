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

import sys
import argparse
import nutch

#TODO: set this on when -verbose flag is requested in CLI args
nutch.Verbose = False


class Crawler(object):

    def __init__(self, args):
        self.args = args
        self.server_url = args['url'] if 'url' in args else nutch.DefaultServerEndpoint
        self.conf_id = args['conf_id'] if 'conf_id' in args else nutch.DefaultConfig
        self.proxy = nutch.Nutch(self.conf_id, self.server_url)

    def crawl_cmd(self, seed_list, n):
        '''
        Runs the crawl job for n rounds
        :param seed_list: lines of seed URLs
        :param n: number of rounds
        :return: number of successful rounds
        '''

        print("Num Rounds "+str(n))

        cc = self.proxy.Crawl(seed=seed_list, rounds=n)
        rounds = cc.waitAll()
        print("Completed %d rounds" % len(rounds))
        return len(rounds)

    def load_xml_conf(self, xml_file, id):
        '''
        Creates a new config from xml file.
        :param xml_file: path to xml file. Format : nutch-site.xml or nutch-default.xml
        :param id:
        :return: config object
        '''

        # converting nutch-site.xml to key:value pairs
        import xml.etree.ElementTree as ET
        tree = ET.parse(xml_file)
        params = {}
        for prop in tree.getroot().findall(".//property"):
            params[prop.find('./name').text.strip()] = prop.find('./value').text.strip()
        return self.proxy.Configs().create(id, configData=params)


    def create_cmd(self, args):
        '''
        'create' sub-command
        :param args: cli arguments
        :return:
        '''
        cmd = args.get('cmd_create')
        if cmd == 'conf':
            conf_file = args['conf_file']
            conf_id = args['id']
            return self.load_xml_conf(conf_file, conf_id)
        else:
            print("Error: Create %s is invalid or not implemented" % cmd)

    
def main(argv=sys.argv):
    parser = argparse.ArgumentParser(description="Nutch Rest Client CLI")   
    
    subparsers = parser.add_subparsers(help ="sub-commands", dest="cmd")
    create_parser = subparsers.add_parser("create", help="command for creating seed/config")
    crawl_parser = subparsers.add_parser("crawl", help="Runs Crawl")

    create_subparsers = create_parser.add_subparsers(help ="sub-commands of 'create'", dest="cmd_create")
    conf_create_parser = create_subparsers.add_parser("conf", help="command for creating config")

    conf_create_parser.add_argument('-cf', '--conf-file', required=True, help='Path to conf file, nutch-site.xml')
    conf_create_parser.add_argument('-id', '--id', required=True, help='Id for config')

    crawl_subseeds = crawl_parser.add_subparsers(help = "sub-commands of 'seed'", dest="cmd_crawl")
    crawl_subseeds.required = True
    subseeds_crawl_parser = crawl_subseeds.add_parser("seed", help="command for creating seeds")
    subseeds_crawl_parser.add_argument("-sf", "--seed-file", help="Seed file path (local path)")
    subseeds_crawl_parser.add_argument("-sl", "--seed-list", help="Comma separated set of seeds to crawl")
    
    crawl_parser.add_argument("-ci", "--conf-id", help="Config Identifier", required=True)
    crawl_parser.add_argument('-n', '--num-rounds', required=True, type=int, help='Number of rounds/iterations')

    parser.add_argument('-u', '--url', help='Nutch Server URL', default=nutch.DefaultServerEndpoint)
    
    args = vars(parser.parse_args(argv))

    res = None
    crawler = Crawler(args)
    if args['cmd'] == 'crawl':
        if args['seed_file'] != None:
            seed_file = args['seed_file']
            with open(seed_file) as rdr:
                res = crawler.crawl_cmd(rdr.readlines(), args['num_rounds'])
        elif args['seed_list'] != None:
                seed_list = args['seed_list']
                res = crawler.crawl_cmd(str(seed_list).rsplit(','), args['num_rounds'])
    elif args['cmd'] == 'create':
        res = crawler.create_cmd(args)
    else:
        print("Command is invalid or not implemented yet")
        exit(1)
    print(res)

if __name__ == '__main__':
    main(sys.argv[1:])
    print("==Done==")