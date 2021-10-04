from setuptools import setup, find_packages

setup(
    name="files-dagster",
    version="0.1",
    description="Meltano project files for dagster",
    packages=find_packages(),
    package_data={
        "bundle": [
            "transform/dagster.yaml",
            "transform/meltano.py",
            "transform/workspace.yaml",
        ]
    },
)
