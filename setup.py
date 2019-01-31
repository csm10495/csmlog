from distutils.core import setup

setup(
    name='csmlog',
    author='csm10495',
    author_email='csm10495@gmail.com',
    url='http://github.com/csm10495/csmlog',
    version='0.2a',
    packages=['csmlog'],
    license='MIT License',
    python_requires='>=2.7',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    include_package_data = True,
    install_requires=['six'],
    #entry_points={
    #    'console_scripts': [
    #        'csmlog' = ?' # todo... interactive?
    #    ]
    #},
)