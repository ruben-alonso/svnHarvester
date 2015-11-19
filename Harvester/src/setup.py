from setuptools import setup, find_packages

setup(
    name = 'Harvester',
    version = '0.9.0',
    packages = find_packages(),
    install_requires = [
        "requests",
        "jper",
        "utils",
        "engine"
    ],
    url = 'http://jisc.ac.uk/',
    author = 'Ruben Romartinez',
    author_email = 'ruben.alonso@jisc.ac.uk',
    description = 'Harvester',
    license = 'Copyheart',
    classifiers = [
        'Development Status :: 1 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Copyheart',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
