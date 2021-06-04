# TMP
# python tmp_render.py <datast_type> <content_type> | pygmentize -l python

import sys
import isort
import jinja2
from black import FileMode, format_str

templateLoader = jinja2.FileSystemLoader(searchpath="../templates")
templateEnv = jinja2.Environment(
    loader=templateLoader,
    extensions=["jinja2.ext.do"],
)
templateEnv.trim_blocks = True
template = templateEnv.get_template("code_example.jinja")

content_types = {
    "application/geo+json": "geojson",
    "application/json": "json",
    "application/parquet": "parquet",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "text/csv": "csv",
}

snippet = template.render(
    **{
        "dataset_id": "testdatasett-foo",
        "version": "1",
        "dataset_type": sys.argv[1],
        "content_type": content_types.get(sys.argv[2]) if len(sys.argv) > 2 else None,
        "delimiter": ",",
        "access_rights": "restricted",
        "api_url": "https://geoserver.data.oslo.systems/geoserver/bym/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=bym%3Apoi-skilt&outputFormat=application/json",
        "requirements": [],
        "imports": [],
    }
)
snippet = format_str(snippet, mode=FileMode())
snippet = isort.code(snippet)

print("--" * 50)
print(snippet)

# with open("test.py", "w") as f:
#     f.write(snippet)
