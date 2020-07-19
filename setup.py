#!/usr/bin/env python

from os import path

from setuptools import setup


packages = [
    'paramsurvey',
]

requires = []
extras_require = {
    'ray': ['ray', 'pyarrow'],
    'mpi': ['mpi4py'],
}

setup_requires = ['setuptools_scm']
test_requirements = ['pytest>=3.0.0']  # 'coverage', 'pytest-cov']

scripts = []

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    description = f.read()

setup(
    name='paramsurvey',
    use_scm_version=True,
    description='A toolkit for doing parameter surveys',
    long_description=description,
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/wumpus/paramsurvey',
    packages=packages,
    python_requires=">=3.5.*",
    extras_require=extras_require,
    setup_requires=setup_requires,
    install_requires=requires,
    scripts=scripts,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
