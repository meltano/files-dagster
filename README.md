# files-dagster

Meltano project [file bundle](https://meltano.com/docs/command-line-interface.html#file-bundle) for Dagster.

Files:
dagster.yaml meltano.py workspace.yaml

```py
# Add dagster
meltano add --custom utility dagit
meltano add --custom utility dagster


# Add  this file bundle to your Meltano project
meltano add --custom files dagster
```
