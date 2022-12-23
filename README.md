# EDCompanion
 Companion application for Elite Dangerous exploration


### Python Environment
This is optional, if your development system has the versions of the
libraries you need installed globally or for your user, you're good to go.

yaml files to build conda environments are provided:

* conda-jupyter-base.yml for building a development environment with cpu-based machine-learning

#### Create an environment using conda

    conda env create -f conda-jupyter-base.yml
    conda activate jupyter-base
    pip install -r conda-extra-reqs

And to update the environment

    conda activate jupyter-base
    conda env update -f conda-jupyter-base.yml --prune

To add the environment to Jupyter:

    conda activate jupyter-base
    python -m ipykernel install --user --name=jupyter-base
    


#### Activate the environment and install requirements

    conda activate jupyter-base


#### Extending
Create a local.yml file:
    channels:

    dependencies:
    - pip:
        - boto3==1.4.4
    imports:
    - requirements/conda-jupyter-base 

    conda activate jupyter-base
    conda env update --file local.yml --prune




