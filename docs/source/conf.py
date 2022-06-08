import os
import sys

sys.path.insert(0, os.path.abspath("../.."))


project = "lector"
copyright = "2022, Thomas Buhrmann"
author = "Thomas Buhrmann"
release = "0.1"

extensions = [
    # "sphinx.ext.autodoc",
    # "sphinx.ext.autosectionlabel",
    # "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    # "sphinx_autodoc_typehints",
    "sphinx.ext.autodoc.typehints",
    "autoapi.extension",
]

autodoc_typehints = "description"

autoapi_dirs = ["../../lector"]
autoapi_member_order = "groupwise"
autoapi_add_toctree_entry = False
autoapi_template_dir = "_templates"

# https://stackoverflow.com/questions/2701998/sphinx-autodoc-is-not-automatic-enough/62613202#62613202
# https://github.com/sphinx-doc/sphinx/issues/7912
# autosummary_generate = True
# autosummary_imported_members = True

# autodoc_member_order = "bysource"  # "groupwise"
# autodoc_inherit_docstrings = True
# add_module_names = False
# autoclass_content = "both"
# html_show_sourcelink = False
# set_type_checking_flag = True

# Napoleon options for docstring parsing:
# http://www.sphinx-doc.org/en/stable/ext/napoleon.html
# napoleon_google_docstring = True
# napoleon_numpy_docstring = True
# napoleon_include_init_with_doc = True
# napoleon_include_private_with_doc = True
# napoleon_include_special_with_doc = True
# napoleon_use_admonition_for_examples = True
# napoleon_use_admonition_for_notes = True
# napoleon_use_admonition_for_references = True
# napoleon_use_ivar = True
# napoleon_use_param = True
# napoleon_use_rtype = True
# napoleon_use_keyword = True
# napoleon_preprocess_types = True
# napoleon_attr_annotations = True

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
            "icon": "_static/graphext.png",
            "type": "local",
        },
    ]
}
