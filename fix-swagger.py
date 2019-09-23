import yaml


"""
    There are some issues with the serverless plugin adding the options (cors) endpoints to the swagger spec,
    without using correct path parameters etc. This clears the options endpoints from the swagger file, as they are
    generally not needed / implicit. 
"""

def filterOptions(path):
    if "options" in path:
        del path["options"]
    return path


def read():
    with open("swagger.yaml", "r") as f:
        swagger = yaml.safe_load(f)
        paths = swagger['paths']
        swagger['paths'] = {name:filterOptions(paths[name]) for name in paths}
        return swagger

def write(content):
    with open("swagger.yaml", "w") as f:
        f.write(content)


fixed = read()
write(yaml.dump(fixed, sort_keys=False))