"""The kernel `stardelt setup vscode` installs: a local IPython kernel with the notebook's native names.

The kernelspec carries STARDELT_NOVA_URL and SPARK_REMOTE in its `env`; this module only loads
stardelt.notebook, so a cell has `spark` and `display` exactly as on the platform.
"""
from ipykernel.kernelapp import IPKernelApp


def main(argv=None):
    app = IPKernelApp.instance()
    app.config.InteractiveShellApp.extensions = ["stardelt.notebook"]
    app.initialize(argv)
    app.start()


if __name__ == "__main__":
    main()
