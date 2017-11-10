===================
target-datadotworld
===================

A `Singer <https://singer.io>`_ target that writes data to `data.world <https://data.world>`_

How to use it
=============

``target-datadotworld`` works together with any other `Singer Tap <https://www.singer.io/#taps>`_ to move
data from sources like `SalesForce <https://github.com/singer-io/tap-salesforce>`_, `HubSpot <https://github.com/singer-io/tap-hubspot>`_, `Marketo <https://github.com/singer-io/tap-marketo>`_, `MySQL <https://github.com/singer-io/tap-mysql>`_  and `more <https://github.com/search?p=3&q=org%3Asinger-io+tap-&type=Repositories>`_ to data.world.

Install and Run
---------------

First, make sure Python 3.6 is installed on your system.

``target-datadotworld`` can be run with any Singer Tap, but we'll use
`tap-fixerio <https://github.com/singer-io/tap-fixerio>`_ which pulls currency exchange rate data - as an example.

These commands will install ``tap-fixerio`` and ``target-datadotworld`` with pip,
and then run them together, piping the output of ``tap-fixerio`` to
``target-datadotworld``::

  ? pip install target-datadotworld tap-fixerio 
  ? tap-fixerio | target-datadotworld -c config.json
  INFO Replicating the latest exchange rate data from fixer.io
  INFO Tap exiting normally

The data will be written to the dataset specified in ``config.json``. In this specific case, under a stream named ``exchange-rates``.

If you're using a different Tap, substitute ``tap-fixerio`` in the final
command above to the command used to run your Tap.

Configuration
-------------

`target-datadotworld` requires configuration file that is used to store your data.world API token, dataset information and other additional configuration.

The following attributes are required:

* ``api_token``: Your data.world `API token <https://data.world/settings/advanced>`_
* ``dataset_id``: The title of the dataset where the data is to be stored. Must only contain lowercase letters, numbers, and dashes.

Additionally, the following optional attributes can be provided. They determine the parameters for creating a new dataset if ``dataset_id`` refers to a dataset that doesn't yet exist:

* ``dataset_title``: Text with no more than 60 characters
* ``dataset_visibility``: OPEN or PRIVATE
* ``dataset_license``: Public Domain, PDDL, CC-0, CC-BY, ODC-BY, CC-BY-SA, ODC-ODbL, CC BY-NC, CC BY-NC-SA or Other
* ``dataset_owner``: If not the same as the owner of the API token (e.g. if the dataset is to be created under an organization account, as opposed to the user's own)

Example:

.. code-block:: json

    {
        "api_token": "your_token",
        "dataset_id": "fixerio-data",
        "dataset_title": "Fixerio Data",
        "dataset_license": "Other",
        "dataset_owner": "my-company",
        "dataset_visibility": "PRIVATE"
    }
