from setuptools import setup
import os
import pathlib
import re
import sys

init_text = pathlib.Path(__file__).parent / "csmlog" / "__init__.py"
version = None
for line in init_text.read_text().splitlines():
    if line.startswith("__version__"):
        version = eval(line.split("=")[1])

assert version, "Could not find __version__!"

setup(
    name="csmlog",
    author="csm10495",
    author_email="csm10495@gmail.com",
    url="http://github.com/csm10495/csmlog",
    version=version,
    packages=["csmlog"],
    license="MIT License",
    python_requires=">=3.6",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    include_package_data=True,
    install_requires=["six", "gspread"],
    entry_points={"console_scripts": ["csmlogudp = csmlog.udp_handler_receiver:main"]},
)
