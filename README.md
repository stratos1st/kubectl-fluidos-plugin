# FLUIDOS kubectl plugin

This plugin is designed to simplify the utilisation of the meta-schedulers.
It operates as an additional utility that can be installed on the client side (operator machine).

## Requirements

The only assumption is to have python 3 installed, the plugin is tested with versions 3.10, 3.11, 3.12, and pypy 3.
Refer to [tox.ini](tox.ini) for details about which versions are being tested.


## How to install

To install the plugin simply execute:

`pip install git+https://github.com/fluidos-project/kubectl-fluidos-plugin`

The above command will install the package `kubectl-fluidos` module in the target environment.
It will also create a binary named `kubectl-fluidos` that should be available directly from command line.

## How to use

Once installed, the plugin is accessed by `kubectl` once the command `kubectl fluidos` is issued.

## Examples

### Example with MSPL

Missing, add example with options specific to MSPL.

### Example with Intent within pod template

Missing, add example with templated spec.

### Example of no requirement and fallback to normal behavior

Missing, add example with no requirements and default fallback to `apply`
