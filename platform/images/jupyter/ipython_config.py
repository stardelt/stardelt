"""System IPython config of the notebook image: every kernel loads %sql and has `spark` and
`display` (stardelt-python: stardelt/sqlmagic.py, stardelt/notebook.py)."""

c = get_config()  # noqa: F821  (IPython provides it)
c.InteractiveShellApp.extensions = ["stardelt.sqlmagic", "stardelt.notebook"]
