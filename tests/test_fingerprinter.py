# Copyright 2018 Spotify AB. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the fingerprinter utils."""

from comet_core.fingerprint import comet_event_fingerprint

ORIG_DICT = {"a": "b", "b": "c", "res": {"lel": "wahat", "gl": "hf"}}

ORIG_DICT_FP = "30c3e95d8fb3665ba70c6e3630a57d9a"

BLACKLIST = ["a", ["res", "gl"], "lol"]

AFTER_BLACKLIST_FP = "a4e1f36de415f8ab64da9fd8d76c8bbc"


def test_event_fingerprint_no_blacklist():  # pylint: disable=invalid-name,missing-docstring
    fingerprint = comet_event_fingerprint(ORIG_DICT)
    assert fingerprint == ORIG_DICT_FP


def test_event_fingerprint_blacklist():  # pylint: disable=invalid-name,missing-docstring
    fingerprint = comet_event_fingerprint(ORIG_DICT, BLACKLIST)
    assert fingerprint == AFTER_BLACKLIST_FP


def test_event_fingerprint_blacklist_prefix():  # pylint: disable=invalid-name,missing-docstring
    fingerprint = comet_event_fingerprint(ORIG_DICT, BLACKLIST, "test")
    assert fingerprint != AFTER_BLACKLIST_FP
