#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name="lglass-sql",
    version="1.1",
    packages=[
            "lglass_sql"
    ],
    author="Fritz Grimpen",
    author_email="fritz@grimpen.net",
    url="https://github.com/fritz0705/lglass-sql.git",
    license="https://opensource.org/licenses/MIT",
    description="SQL database backend for lglass",
    classifiers=[
            "Development Status :: 4 - Beta",
            "Programming Language :: Python :: 3 :: Only",
            "Topic :: System :: Networking",
            "Topic :: Database"
    ],
    install_requires=[
        "lglass",
        "psycopg2"
    ],
    package_data={
    }
)
