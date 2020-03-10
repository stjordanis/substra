.. Substra documentation master file, created by
   sphinx-quickstart on Fri Mar  6 15:51:49 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Substra's documentation!
===================================

0. Extract from py file
=======================

.. literalinclude:: conf.py
   :language: python
   :lines: 36-43

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   README
   local_install_skaffold
   add_data_samples

1. Installation
===============

2. Running the Substra platform locally
=======================================

3. Usage
========

CLI Interface
=============

.. automodule:: cli.interface
   :members:

SDK
===

.. automodule:: sdk.client
   :members:

.. autoclass:: sdk.client.Client
   :members:

Runner Methods
==============

.. automethod:: runner::compute_train
.. automethod:: runner::compute_test
.. automethod:: runner::compute_perf
.. automethod:: runner::compute

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
