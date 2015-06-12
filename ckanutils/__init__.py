#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CKAN Utils
~~~~~~~~~~

Miscellaneous CKAN utility scripts
"""

from __future__ import (
    absolute_import, division, print_function, with_statement,
    unicode_literals)

__title__ = 'CKAN Utils'
__package_name__ = 'ckanutils'
__author__ = 'Reuben Cummings'
__description__ = 'Miscellaneous CKAN utility scripts'
__email__ = 'reubano@gmail.com'
__version__ = '0.1.0'
__license__ = 'MIT'
__copyright__ = 'Copyright 2015 Reuben Cummings'


class Ckanutils(object):
    """This is a description of the class."""

    def __init__(self, argument, kwarg=None):
        """
        Initialization method.

        Parameters
        ----------
        argument : an example argument (string)
        kwarg : an optional argument (string)

        Returns
        -------
        New instance of :class:`Ckanutils`
        :rtype: Ckanutils

        Examples
        --------
        >>> from . import Ckanutils
        >>> ckanutils = Ckanutils('argument')
        >>> ckanutils  #doctest: +ELLIPSIS
        <class object Ckanutils at 0x...>
        >>> Ckanutils()  #doctest: +ELLIPSIS
        <script.Ckanutils object at 0x...>
        """

        self.argument = argumentName
        self.kwarg = kwarg

    @property
    def argument(self):
        """
        Show argument.

        Returns
        -------
        Argument : string

        Examples
        --------
        >>> Ckanutils('hello').argument
        'hello'
        """
        return self.argument

    def multiply(self, value):
        """
        Double argument.

        Parameters
        ----------
        value : int
            number to multiply by

        Returns
        -------
        Argument : string

        Examples
        --------
        >>> Ckanutils('piki').multiply(2)
        'pikipiki'
        """
        return self.argument * value
