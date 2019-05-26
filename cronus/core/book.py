#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Book of objects
Key-Value store for metaobjects held in Cronus Stores
Derived from DIANA-HEP histbook package
Wraps Physt rather than histbook hist
BaseBook just implements common dictionary methods
"""

import collections
import fnmatch


class BaseBook(collections.MutableMapping):

    def __init__(self, hists={}):

        self._content = collections.OrderedDict()

        if isinstance(hists, dict):
            for n, x in hists.items():
                self[n] = x

        self._updated()

    @classmethod
    def load_from_dicts(cls, content):
        out = cls.__new__(cls)
        out._content = collections.OrderedDict()

        for k, v in content.items():
            out[k] = v

        return out

    def compatible(self, other):
        '''
        books have equivalent keys
        re-implement in derived classes
        '''

        return set(self._iter_keys()) == set(other._iter_keys())

    def _updated(self):
        pass

    def __eq__(self, other):
        '''
        book1 == book2
        '''
        return self.__class__ == other.__class__ \
            and self._content == other._content

    def __ne__(self, other):
        '''
        book1 != book2
        '''
        return not self.__eq__(other)

    def __len__(self):
        '''
        len(book)
        '''
        return len(self._content)

    def __contains__(self, name):
        '''
        if book has key
        '''
        try:
            self[name]
        except KeyError:
            return False
        else:
            return True

    def _get(self, name):
        attempt = self._content.get(name, None)
        if attempt is not None:
            return attempt
        return None

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError("keys of a {0} must be strings".
                            format(self.__class__.__name__))

        if "*" in name:
            return [x for n, x in self if fnmatch.fnmatchcase(n, name)]
        else:
            out = self._get(name)
            if out is not None:
                return out
            else:
                raise KeyError("could not find {0} and could not interpret \
                                as a glob pattern".format(repr(name)))

    def _set(self, name, value):
        self._content[name] = value
        self._updated()

    def __setitem__(self, name, value):
        '''
        book[key] = value
        '''

        if not isinstance(name, str):
            raise TypeError
        if not isinstance(value, str):
            raise TypeError

        self._set(name, value)

    def _del(self, name):
        if name in self._content:
            del self._content[name]
            self._updated()
        else:
            raise KeyError

    def __delitem__(self, name):
        '''
        del book[key]
        '''
        if not isinstance(name, str):
            raise TypeError

        if '*' in name:
            keys = [n for n in self._contents.keys()
                    if fnmatch.fnmatchcase(n, name)]
            for k in keys:
                self._del(k)
        else:
            self._del(name)

    def __iter__(self):
        '''
        for k, v in book.items()
        '''
        for k, v in self._content.items():
            yield k, v

    def _iter_keys(self):
        for k, v in self._content.items():
            yield k

    def _iter_values(self):
        for k, v in self._content.items():
            yield v

    def keys(self):
        return list(self._iter_keys())

    def values(self):
        return list(self._iter_values())

    def items(self):
        '''
        book.items()
        '''
        return list(self._content.items())

    def __add__(self, other):
        '''
        book = book1 + book2
        '''
        if not isinstance(other, BaseBook):
            raise TypeError("histogram books can only be added to other books")

        content = collections.OrderedDict()
        for n, x in self:
            if n in other:
                content[n] = x + other[n]
            else:
                content[n] = x
        for n, x in other:
            if n not in self:
                content[n] = x
        return self.__class__.load_from_dicts(content)

    def __iadd__(self, other):
        '''
        book += book1
        '''
        if not isinstance(other, BaseBook):
            raise TypeError("books can only be added to other books")

        for n, x in other:
            if n not in self:
                self[n] = x
            else:
                self[n] += x
        return self

