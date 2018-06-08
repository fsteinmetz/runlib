#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import tornado
import json
from tornado import web, httpserver, netutil
from datetime import datetime
from textwrap import dedent
from time import sleep
from jinja2 import Template
from subprocess import check_output
from threading import Thread
import argparse


"""
A web app for monitoring HTCondor jobs
"""

jobstatus_color = {
    0 : 'Crimson',     # failed
    1 : 'LimeGreen',   # completed
    2 : 'DodgerBlue',  # running
    3 : 'White',       # idle
    4 : 'IndianRed',   # failed (other)
}

jobstatus_names = {
    0 : 'Failed',
    1 : 'Completed',
    2 : 'Running',
    3 : 'Idle',
    4 : 'Failed (Condor)',
}

html_header = dedent(
    '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Condor status</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
        <meta http-equiv="refresh" content="10" >
    <style>
    p,h1,li {
        font-family: Arial, Helvetica;
    }
    th {
        color: white;
        background-color: #59a836;
    }
    table {
        font-family: Arial, Helvetica;
        text-align: left;
        border-collapse: collapse;
    }
    table, th, td {
        border: 1px solid #ddd;
        padding: 5px;
    }
    tr:nth-child(even){
        background-color: #f2f2f2;
    }
    </style>
    </head>
    ''')

def parsedate(ts):
    """
    Parse date from timestamp ts
    """
    if int(ts) == 0:
        return None
    return datetime.fromtimestamp(int(ts))


class Job(object):
    def __init__(self, jdata):
        """
        Create a Job object from json data
        """
        self.clusterid = jdata['ClusterId']
        self.id = jdata['ProcId']
        self.owner = jdata['Owner']
        self.jobstatus = jdata['JobStatus']
        try:
            self.exitcode = jdata['ExitCode']
        except KeyError:
            self.exitcode = None

        if self.jobstatus == 4: # Completed
            if self.exitcode != 0:
                self.status = 0  # failed
            else:
                self.status = 1  # completed
        elif self.jobstatus == 2: # running
            self.status = 2
        elif self.jobstatus == 1: # idle
            self.status = 3
        else:
            self.status = 4  # failed (other)

        self.out = jdata['Out']
        self.err = jdata['Err']

        self.startdate = parsedate(jdata.get('JobStartDate', 0))
        self.completiondate = parsedate(jdata.get('CompletionDate', 0))

        if (self.startdate is not None) and (self.completiondate is not None):
            self.elapsed = self.completiondate - self.startdate
        else:
            self.elapsed = ''


class Cluster(object):
    def __init__(self, clusterid):
        self.id = clusterid
        self.jobs = {}
        self.owner = None

    def insert(self, job):
        self.jobs[job.id] = job
        self.owner = job.owner

    def count(self, op, value):
        """
        Cumulated count for all statuses for which op(status, value) is True
        """
        expr = {'eq': lambda x: x == value,
                'lt': lambda x: x < value,
                'le': lambda x: x <= value,
               }[op]
        return len([j for j in self if expr(j.status)])

    def percent(self, op, value):
        """
        Cumulated percentage for all statuses for which op(status, value) is True
        """
        return 100*self.count(op, value)/len(self)

    def __iter__(self):
        for k in sorted(self.jobs.keys()):
            yield self.jobs[k]

    def __len__(self):
        return len(self.jobs)

    def __getitem__(self, k):
        return self.jobs[k]


class Clusters(object):
    def __init__(self):
        self.clust = {}
        self.last_updated = None
        self.terminate = False
        self.currentuser = None
        self.need_update = False

    def setterminate(self):
        self.terminate = True

    def setuser(self, user):
        self.need_update = True
        self.currentuser = user

    def update(self):
        '''
        Update the object by fetching data from condor_q or
        condor_history

        Store data as follows:
        {
            <cluster_id1>: <Cluster1>,
            <cluster_id2>: <Cluster2>,
            ...
        }
        '''
        for jdata in self.get_jobs():
            job = Job(jdata)
            if (self.currentuser is None) or (job.owner == self.currentuser):
                self.append(job)

        # clean clusters that are not of the appropriate owner
        cids = list(self.clust.keys())
        for cid in cids:
            c = self[cid]
            if (self.currentuser is not None) and (c.owner != self.currentuser):
                self.clust.pop(cid)

        self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_jobs(self):
        """
        Returns HTCondor tasks using condor_q and condor_hist
        """
        json_raw_all = check_output(['condor_q', '-allusers', '-json'])
        json_raw_hist = check_output(['condor_history', '-json'])

        if len(json_raw_all) == 0:
            data_all = []
        else:
            data_all = json.loads(json_raw_all)

        if len(json_raw_hist) == 0:
            data_hist = []
        else:
            data_hist = json.loads(json_raw_hist)

        return data_all + data_hist

    def __getitem__(self, clusterid):
        """
        Returns Cluster by clusterid
        """
        return self.clust[clusterid]

    def append(self, job):
        """
        Add job to the appropriate Cluster
        """
        clusterid = job.clusterid
        if clusterid not in self.clust:
            self.clust[clusterid] = Cluster(clusterid)
        self[clusterid].insert(job)

    def __len__(self):
        return len(self.clust)

    def __iter__(self):
        for k in sorted(self.clust.keys())[::-1]:
            yield self.clust[k]

    def owners(self):
        """ returns a list of all owners """
        return list(set([c.owner for c in self]))

    def count(self, op, value, owner=None):
        return sum([c.count(op, value) for c in self
                    if (owner is None) or (c.owner == owner)])


def BaseHandlerMaker(clusters):
    class BaseHandler(web.RequestHandler):
        def get(self, user=None):
            clusters.setuser(user)
            html = Template(dedent('''
            {{ header }}
            <body>
            <h1>Condor Status</h1>

            {% if clusters.last_updated is not none %}

                <p>Last updated at {{clusters.last_updated}}</p>

                <p>Showing clusters from:
                <ul>
                    {% for u in clusters.owners() %}
                        <li><b><a href="/user/{{u}}">{{u}}</a></b>:
                            {% for k, v in jobstatus_names.items() %}
                                {% if clusters.count('eq', k, u) > 0 %}
                                    <div style="border:1px solid black; height: 15px; width: 15px;
                                                display: inline-block;
                                                background-color:{{color[k]}}"></div>
                                    {{v}}: {{clusters.count('eq', k, u)}}
                                {% endif %}
                            {% endfor %}
                            </li>
                    {% endfor %}
                </ul>
                </p>
                <p><a href="/">[Show all users]</a></p>

                {% if clusters|length == 0 %}
                    <p>Empty !</p>
                {% else %}
                <table>
                <tr>
                    <th>Cluster</th>
                    <th>Status</th>
                    <th>Completed</th>
                    <th>User</th>
                </tr>
                {% for c in clusters %}
                    <tr>
                    <td><a href="/cluster/{{c.id}}">{{c.id}}</a></td>
                    <td><div style="border: 1px solid #000;
                                    height: 15px;
                                    width: 150px;
                                    background: linear-gradient(to right,
                                        {{color[0]}} {{c.percent('lt', 0)}}%, {{color[0]}} {{c.percent('le', 0)}}%,
                                        {{color[1]}} {{c.percent('lt', 1)}}%, {{color[1]}} {{c.percent('le', 1)}}%,
                                        {{color[2]}} {{c.percent('lt', 2)}}%, {{color[2]}} {{c.percent('le', 2)}}%,
                                        {{color[3]}} {{c.percent('lt', 3)}}%, {{color[3]}} {{c.percent('le', 3)}}%,
                                        {{color[4]}} {{c.percent('lt', 4)}}%, {{color[4]}} {{c.percent('le', 4)}}%
                                        )"> </div></td>
                    <td>{{c.count('eq', 1)}}/{{c|length}}</td>
                    <td>{{c.owner}}</td>
                    </tr>
                {% endfor %}
                </table>
                {% endif %}
            {% else %}
                <p>Waiting for first update...</p>
            {% endif %}

            </body>
            </html>
            ''')).render(clusters=clusters, header=html_header,
                         jobstatus_names=jobstatus_names, color=jobstatus_color,
                         user=user)

            self.set_header('Content-type', 'text/html')
            self.write(html)

    return BaseHandler

def JobHandlerMaker(clusters):
    class JobHandler(web.RequestHandler):
        def get(self, cid, jid):
            try:
                job = clusters[int(cid)][int(jid)]
            except IndexError:
                html = Template(dedent('''
                {{ header }}
                <p>Invalid job</p>
                ''')).render(header=html_header)
                self.set_header('Content-type', 'text/html')
                self.write(html)
                return

            out_data = open(job.out, 'r').read()
            err_data = open(job.err, 'r').read()

            html = Template(dedent('''
            {{ header }}
            <body>
            
            <h1>Job: {{job.clusterid}}.{{job.id}}</h1>
            <p>Owner: {{job.owner}}</p>
            <p><b>stderr: {{job.err}}</b></p>
            <code style=display:block;white-space:pre-wrap>{{err_data}}</code>
            <p><b>stdout: {{job.out}}</b></p>
            <code style=display:block;white-space:pre-wrap>{{out_data}}</code>
            </body>
            </html>
            ''')).render(job=job, header=html_header,
                         out_data=out_data, err_data=err_data)

            self.set_header('Content-type', 'text/html')
            self.write(html)

    return JobHandler

def ClusterHandlerMaker(clusters):
    class ClusterHandler(web.RequestHandler):
        def get(self, cid):
            try:
                cid_int = int(cid)
                cluster = clusters[cid_int]
            except:
                html = Template(dedent('''
                {{ header }}
                <p>Invalid clusterid "{{cid}}"</p>
                ''')).render(header=html_header, cid=cid)
                self.set_header('Content-type', 'text/html')
                self.write(html)
                return

            html = Template(dedent('''
            {{ header }}
            <body>
            <h1>Clusterid: {{cluster.id}}</h1>
            <p>Owner: {{cluster.owner}}</p>
            {% for k, v in jobstatus_names.items() %}
                {% if cluster.count('eq', k) > 0 %}
                    <p>{{v}}: {{cluster.count('eq', k)}}</p>
                {% endif %}
            {% endfor %}
            <table>
            <tr>
                <th>Job</th>
                <th>Status</th>
                <th>ExitCode</th>
                <th>Started</th>
                <th>Elapsed</th>
            </tr>
            {% for j in cluster %}
                <tr>
                <td><a href="/cluster/{{j.clusterid}}/{{j.id}}">{{j.clusterid}}.{{j.id}}</a></td>
                <td style='background: {{color[j.status]}}'>{{jobstatus_names[j.status]}}</td>
                <td>{{j.exitcode}}</td>
                <td>{{j.startdate}}</td>
                <td>{{j.elapsed}}</td>
                </tr>
            {% endfor %}
            </table>
            </body>
            </html>
            ''')).render(cluster=cluster, header=html_header,
                         jobstatus_names=jobstatus_names, color=jobstatus_color)

            self.set_header('Content-type', 'text/html')
            self.write(html)

    return ClusterHandler

class UpdateThread(Thread):
    def __init__(self, clusters):
        Thread.__init__(self)
        self.clusters = clusters
    def run(self):
        while True:
            self.clusters.update()
            for _ in range(20):
                sleep(1)
                if self.clusters.terminate:
                    return
                if self.clusters.need_update:
                    self.clusters.need_update = False
                    break


def web_app(port=0, hostname=None):
    """
    Creates and serves the web app
    """
    # get available port
    sockets = netutil.bind_sockets(port, '')
    if hostname is None:
        hostname = socket.gethostname()
    ports = [s.getsockname()[1] for s in sockets
             if s.family == socket.AddressFamily.AF_INET] # select IPV4
    assert len(ports) == 1
    port = ports[0]

    server_url = 'http://{address}:{port}'.format(address=hostname, port=port)

    clusters = Clusters()
    ut = UpdateThread(clusters)
    ut.start()

    handlers = [
        ('/', BaseHandlerMaker(clusters)),
        ('/user/(.*)', BaseHandlerMaker(clusters)),
        ('/cluster/(.*)/(.*)', JobHandlerMaker(clusters)),
        ('/cluster/(.*)', ClusterHandlerMaker(clusters)),
    ]
    app = web.Application(handlers)
    server = httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    print('Started web server at {}'.format(server_url))

    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        print('Quitting...')
        clusters.setterminate()
        ut.join()
        print('Done')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=0,
                        help='port number to use instead of a random one.')
    args = parser.parse_args()
    web_app(port=args.port)
