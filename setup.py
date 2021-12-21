# pylint:skip-file
"""
Wrapper for the functionality for various installation and project setup commands
see:
    `python setup.py help`
for more details
"""
from setuptools import setup, find_packages

setup(
    name="autoreduce_scripts",
    version="22.0.0.dev30",
    description="ISIS Autoreduce helper scripts",
    author="ISIS Autoreduction Team",
    url="https://github.com/autoreduction/autoreduce-scripts/",
    install_requires=[
        "autoreduce_db==22.0.0.dev21",
        "autoreduce_utils==22.0.0.dev7",
        "django",  # will be matched with requirement in autoreduce_db
        "fire==0.4.0",
        "h5py==2.10.0",  # for reading the RB number from the datafile
        "GitPython==3.1.14"  # for backup_reduction_scripts.py
    ],
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "autoreduce-manual-submission = autoreduce_scripts.manual_operations.manual_submission:fire_entrypoint",
            "autoreduce-manual-remove = autoreduce_scripts.manual_operations.manual_remove:fire_entrypoint",
            "autoreduce-check-time-since-last-run = autoreduce_scripts.checks.daily.time_since_last_run:main"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
    ])
