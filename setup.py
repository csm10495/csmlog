from setuptools import setup
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from csmlog import __version__

setup(
    name='csmlog',
    author='csm10495',
    author_email='csm10495@gmail.com',
    url='http://github.com/csm10495/csmlog',
    version=__version__,
    packages=['csmlog'],
    license='MIT License',
    python_requires='>=3.6',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    include_package_data = True,
    install_requires=['six'],
    entry_points={
        'console_scripts': [
            'csmlogudp = csmlog.udp_handler_receiver:main'
        ]
    },
)