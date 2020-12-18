import jinja2
import json
import glob
import os

def build_readme(metadata):
    templateLoader = jinja2.FileSystemLoader("./")
    templateEnv = jinja2.Environment(loader=templateLoader)
    template_file = './tools/readme_builder/README.jinja2'
    template = templateEnv.get_template(template_file)
    instance = template.render(metadata)
    return instance

paths = glob.glob('../../**/*.metadata', recursive=True)

for path in paths:
    metadata = json.loads(open(path, 'r').read())
    readme = build_readme(metadata)

    path, filename = os.path.split(path)
    readme_path = os.path.join(path, "README.MD")
    print(F'Wrote {readme_path}')

    open(readme_path, 'w').write(readme)