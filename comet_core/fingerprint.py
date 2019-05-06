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

"""Helper function to compute the fingerprint of alerts."""

import collections
import hmac
import json
from copy import deepcopy
from hashlib import sha256, shake_256

HASH_BYTES = 16  # 128 bits of entropy, will result in 32 character hexdigest string


def comet_event_fingerprint(data_dict, blacklist=None, prefix=''):
    """Computes the fingerprint of an event by hashing it's data dictionary.

    Args:
        data_dict (dict): the dictionary to be hashed (excluding the fields in blacklist)
        blacklist (list): fields to ignore. List of str (for toplevel fields) or str list (for nested fields)
        prefix (str): string that should be prepended to the fingerprint hash
    Returns:
        str: the fingerprint
    """
    data_dict_copy = deepcopy(data_dict)
    filtered_dict = filter_dict(data_dict_copy, blacklist if blacklist is not None else [])
    data_hash_str = dict_to_hash(filtered_dict)
    return f'{prefix}{data_hash_str}'


def filter_dict(orig_dict, blacklist):
    """Filter the keys in blacklist from the orig_dict

    The blacklist consist of strings, lists of strings or a mix. A string should be a key to remove from the orig_dict.
    A list would be a path of keys to remove from the orig_dict. E.g. with the dictionary
    {
        'a': {
            'b': 'c',
            'd': 'e'
        }
    }
    One could remove "a" by giving the string "a" in the blacklist.
    To remove a sub-key, one would provide the path to it as a list: ['a', 'b'].

    Args:
        orig_dict (dict): the dict to filter.
        blacklist (list): strings and lists of strings as described above.
    Returns:
        dict: the filtered dict.
    """
    for item in blacklist:
        if isinstance(item, str) and item in orig_dict:
            del orig_dict[item]
        elif isinstance(item, collections.Iterable):
            pointer = orig_dict
            for sub in item[:-1]:
                pointer = pointer.get(sub, {})
            if item[-1] in pointer:
                del pointer[item[-1]]

    return orig_dict


def dict_to_hash(input_dict):
    """Converts a dictionary into a hash string.

    The fields of the dictionary are sorted before hashing, so changing the order of fields does not change the hash.

    Args:
        input_dict (dict): input that will be hashed

    Returns:
        str: hash in hexadecimal representation
    """
    data_str = json.dumps(input_dict, sort_keys=True)
    return str_to_hash(data_str)


def str_to_hash(input_str):
    """Converts a string into a hash string.

    Uses the SHA-3 shake function to reduce the hash output (and by this it's entropy) to HASH_BYTES.
    Args:
        input_str (str): input that will be hashed
    Returns:
        str: hash in hexadecimal representation (2 characters per byte)
    """
    input_bytes = input_str.encode('utf-8')
    hash_str = shake_256(input_bytes).hexdigest(HASH_BYTES)
    return hash_str


def fingerprint_hmac(fingerprint, hmac_secret):
    """Generate an hmac for the given fingerprint.

    Can be used for authenticating

    Args:
        fingerprint (str): fingerprint to generate hmac for
        hmac_secret (str): secret key for hmac generation

    Returns:
        str: hmac as hexdigest str

    """
    return hmac.new(bytes(hmac_secret, 'utf-8'), msg=bytes(fingerprint, 'utf-8'), digestmod=sha256).hexdigest()
