# coding=utf-8

#  Copyright (c) 2008-2012 Aerospike, Inc. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Collect stats from Aerospike

#### Dependencies

 * aerospike

"""


import diamond.collector
from diamond.collector import str_to_bool

try:
    import citrusleaf
    citrusleaf
except ImportError:
    citrusleaf = None


class AerospikeCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(AerospikeCollector, self).get_default_config_help()
        config_help.update({
            'host': 'Aerospike Hostname',
            'port': 'Aerospike Port',
            'service_stats': 'Should we collect service related stats?',
            'set_stats': 'Should we collect sets related stats?',
            'latency_stats': 'Should we collect latency related stats?',
            'namespace_stats': 'Should we collect namespace related stats?',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(AerospikeCollector, self).get_default_config()
        config.update({
            'host':            'localhost',
            'port':             3000,
            'path':            'aerospike',
            'service_stats':   'True',
            'set_stats':       'True',
            'latency_stats':   'True',
            'namespace_stats': 'True',

        })
        return config

    def _is_num(self, x):
        try:
            float(x)
            return True
        except ValueError:
            return False

    def _get_aerospike_conn(self, param):
        r = -1
        try:
            r = citrusleaf.citrusleaf_info(self.config['host'],
                                           self.config['port'], param)
        except Exception, e:
            self.log.error('Couldnt connect to aerospike for %s: %s', param, e)
            pass
        return r

    def _get_node_statistics(self):
        r = self._get_aerospike_conn('statistics')
        if (-1 != r):
            results = {}
            for string in r.split(';'):
                key, value = string.split('=')
                if self._is_num(value):
                    results["service.%s" % key] = value
            return results

    def _get_node_sets(self):
        r = self._get_aerospike_conn('sets')
        if (-1 != r):
            results = {}
            for string in r.split(';'):
                if len(string) == 0:
                    continue
                setList = string.split(':')
                namespace = setList[0]
                sets = setList[1]
                for set_tuple in setList[2:]:
                    key, value = set_tuple.split('=')
                    if self._is_num(value):
                        results["sets.%s.%s.%s" % (
                            namespace, sets, key)] = value
            return results

    def _get_node_latency(self):
        r = self._get_aerospike_conn('latency:')
        if (-1 != r):
            results = {}
            latency_type = ""
            header = []
            for string in r.split(';'):
                if len(string) == 0:
                    continue
                if len(latency_type) == 0:
                    latency_type, rest = string.split(':', 1)
                    header = rest.split(',')
                else:
                    val = string.split(',')
                    for i in range(1, len(header)):
                        key = latency_type + "." + header[i]
                        key = key.replace('>', 'over_')
                        key = key.replace('ops/sec', 'ops_per_sec')
                        value = val[i]
                        if self._is_num(value):
                            results["latency.%s" % key] = value
                    latency_type = ""
                    header = []
            return results

    def _get_node_namespace(self):
        r = self._get_aerospike_conn('namespaces')
        if (-1 != r):
            results = {}
            namespaces = filter(None, r.split(';'))
        if len(namespaces) > 0:
            for namespace in namespaces:
                r = self._get_aerospike_conn('namespace/' + namespace)
                if (-1 != r):
                    for string in r.split(';'):
                        key, value = string.split('=')
                        if self._is_num(value):
                            results["namespaces.%s.%s" % (
                                namespace, key)] = value
            return results

    def _publish_kv(self, results):
        """
        Recursively publish key/value
        """
        for key, value in results.iteritems():
            self.publish(key, value)

    def collect(self):
        if citrusleaf is None:
            self.log.error('Unable to import aerospike')
            return

        if str_to_bool(self.config['service_stats']):
            node_statistics = self._get_node_statistics()
            self._publish_kv(node_statistics)

        if str_to_bool(self.config['set_stats']):
            node_sets = self._get_node_sets()
            self._publish_kv(node_sets)

        if str_to_bool(self.config['latency_stats']):
            node_latency = self._get_node_latency()
            self._publish_kv(node_latency)

        if str_to_bool(self.config['namespace_stats']):
            node_namespace = self._get_node_namespace()
            self._publish_kv(node_namespace)
