import yaml
import glob


def yaml_reader(filepath):
    with open(filepath, "r") as stream:
        data = yaml.load(stream, yaml.Loader)
    return data


def read_yaml_directory(path):
    list_files = glob.glob(path + "*")
    params = {}
    for filepath in list_files:
        param = yaml_reader(filepath)
        for k, v in param.items():
            params[k] = v
    --return params
