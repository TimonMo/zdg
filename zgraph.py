import os
import pandas as pd
import datetime
import sys
import subprocess
import traceback
import argparse
import getpass
import configparser


def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)


def get_node_timestamp(env, config, node):
    f = modification_date(env['working_dir'] + '/' + node.name + '.csv')
    o = modification_date(env['working_dir'] + '/targets/' + node.name + '.out')
    return f, o


def setup_environment(args):
    env = dict()
    if args.local:
        env['working_dir'] = os.path.dirname(os.path.realpath(__file__))
    else:
        env['working_dir'] = os.path.dirname(os.path.realpath(__file__))

    return env


class RuleWrapper(object):
    # Create based on class name:
    def factory(type):
        # return eval(type + "()")
        if type == "a":
            return Apple()
        if type == "b":
            return Banana()
        if type == "c":
            return Citron()
        if type == "d":
            return Donnut()
        if type == "e":
            return Egg()
        if type == "f":
            return Flower()
        if type == "g":
            return Grape()
        assert 0, "Bad food creation: " + type

    factory = staticmethod(factory)


class Apple(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/a.out'.format(env['working_dir']))


class Banana(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/b.out'.format(env['working_dir']))


class Citron(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/c.out'.format(env['working_dir']))


class Donnut(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/d.out'.format(env['working_dir']))


class Egg(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/e.out'.format(env['working_dir']))


class Flower(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/f.out'.format(env['working_dir']))


class Grape(RuleWrapper):
    def run(self, env, config):
        os.system('touch {0}/g.out'.format(env['working_dir']))


# Generate shape name strings:
# def shapeNameGen(n):
# types = RuleWrapper.__subclasses__()
# for i in range(n):
# yield random.choice(types).__name__


def travel_graph(nodes, edges, env, config):
    nodes.set_index('name', inplace=True)
    nodes['traveled'] = 0
    nodes['script_timestamp'], nodes['target_timestamp'] = zip(
        *nodes.apply(lambda x: get_node_timestamp(env, config, x)))

    rule_map = {key: RuleWrapper.factory(key) for key in nodes.index.to_list()}

    dependant_stack = list()

    dependant_stack.append('a')

    while len(dependant_stack) > 0:
        node_name = dependant_stack[-1]

        edges = edges[edges['ant'] == node_name]

        node = nodes.loc[node_name]

        if len(edges) == 0:
            # this is a sink
            rule_map[node.name].run(env, config)
            node['script_timestamp'], node['target_timestamp'] = get_node_timestamp(env, config, node)
        else:
            tees = nodes[edges['tee'].unique()]

            if all(tees['traveled']):
                edge_target_max_timestamp = tees['target_timestamp'].max()
                self_rule_timestamp = node['script_timestamp']
                latest_timestamp = max(edge_target_max_timestamp, self_rule_timestamp)

                if latest_timestamp > node['target_timestamp']:
                    rule_map[node.name].run(env, config)
                    node['target_timestamp'] = latest_timestamp
                else:
                    print('node is up to date')
            else:
                print('node {0} depends on the following node(s)'.format(node['name']))
                print(tees[tees['traveled'] == 0])
                dependant_stack = dependant_stack + tees[tees['traveled'] == 0].name.to_list()
                print('new dependant stack is ')
                print(dependant_stack)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-g", "--graph", help="start date")
    parser.add_argument("-c", "--config", help="get config file")
    parser.add_argument("-l", "--local", help="local mode", action='store_true')
    parser.add_argument("-b", "--branch", help="if NOT local mode, work on specific branch")
    parser.add_argument("-t", "--tag", help="if NOT local mode, work on specific tag")

    args = parser.parse_args()

    config = configparser.ConfigParser()

    env = setup_environment(args)

    config.read(env['working_dir'] + '/' + args.config)

    nodes = pd.read_csv(env['working_dir'] + '/' + args.graph + '/' + 'nodes.csv')
    edges = pd.read_csv(env['working_dir'] + '/' + args.graph + '/' + 'edges.csv')

    travel_graph(nodes, edges, env, config)


if __name__ == "__main__":
    if getpass.getuser() != 'ubuntu':
        raise NameError('operational scripts can only be run by ubuntu')

    try:
        main()
    except Exception as e:
        print('\n')
        traceback.print_exc()
        print("\n")
        sys.exit(1)
