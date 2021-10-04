from setuptools import setup, find_packages

setup(
    name="files-dagster",
    version="0.1",
    description="Meltano project files for dagster",
    packages=find_packages(),
    package_data={
        "bundle": [
            "orchestrate/dagster.yaml",
            "orchestrate/meltano.py",
            "orchestrate/workspace.yaml",
        ]
    },
)
