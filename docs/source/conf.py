import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from lector import __version__  # noqa

project = "lector"
copyright = "2022, Thomas Buhrmann"
author = "Thomas Buhrmann"
version = __version__
release = version

extensions = [
    # "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    # "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    # "sphinx_autodoc_typehints",
    "sphinx.ext.autodoc.typehints",
    "sphinx.ext.todo",
    "autoapi.extension",
]

autodoc_typehints = "description"

autoapi_dirs = ["../../lector"]
autoapi_member_order = "groupwise"
autoapi_add_toctree_entry = False
autoapi_template_dir = "_templates"

autosectionlabel_prefix_document = True


templates_path = ["_templates"]
exclude_patterns = []

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]

html_theme_options = {
    "icon_links": [
        {
            "name": "Github",
            "url": "https://github.com/graphext/lector",
            "icon": "fab fa-github-square",
            "type": "fontawesome",
        },
        {
            "name": "Graphext",
            "url": "https://www.graphext.com",
            "icon": "_static/gx_logo_sq_blue.jpg",
            "type": "local",
        },
    ]
}
