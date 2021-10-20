import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'reference'
db = client[DB_NAME]

with open('oncotree.json') as json_file:
    oncotree = json.load(json_file)

node_dict = {}
type_dict = {}
tissue_dict = {}


def get_all_leaves(node):
    child_nodes = node.get('children', None)
    children = []
    if child_nodes:
        for key, val in child_nodes.items():
            children += get_all_leaves(val)
    else:
        children += [node['code']]
    return children


def get_all_nodes(node):
    child_nodes = node.get('children', None)
    children = []
    if child_nodes:
        for key, val in child_nodes.items():
            children.append(key)
            children += get_all_nodes(val)
    print('Node with code', node['code'], 'and name', node['name'], 'has children:', children)
    print('Node', node['code'], 'with name', node['name'], 'main type:', node['mainType'])
    node_dict[node['code']] = {'code': node['code'],
                               'children': children + [node['code']],
                               'mainType': node['mainType'],
                               'tissue': node['tissue']}
    if node['tissue'] in tissue_dict:
        tissue_dict[node['tissue']].append(node['code'])
    else:
        tissue_dict[node['tissue']] = [node['code']]
    if node['mainType'] in type_dict:
        type_dict[node['mainType']].append(node['code'])
    else:
        type_dict[node['mainType']] = [node['code']]
    return children


leaves = []
leaves += get_all_leaves(oncotree['TISSUE'])
print("leaves")
print(leaves)
print(len(leaves))
print("nodes")
nodes = ['TISSUE']
nodes += get_all_nodes(oncotree['TISSUE'])
print(nodes)
print(len(nodes))
print(len(node_dict))
print(len(type_dict))
for code, node in node_dict.items():
    node_dict[code]['mainTypeCodes'] = type_dict[node['mainType']]
    node_dict[code]['tissueCodes'] = tissue_dict[node['tissue']]
    if code not in type_dict[node['mainType']]:
        print("This should probably never happen.")
to_mongo = []
for key, val in node_dict.items():
    to_mongo.append(val)
print(len(to_mongo))
collection = 'oncotree'
collection = db[collection]
insert_ids = collection.insert_many(to_mongo)
