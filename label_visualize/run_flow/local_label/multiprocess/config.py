import json


config = None
with open('worker_config.json', 'r') as file:
    config = json.load(file)


def workers():
    return config['workers']
