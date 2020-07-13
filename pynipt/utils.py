import os
from shleeh.utils import *


def intensive_mkdir(abspaths, interface=None):
    """Intensive mkdir, make all parent paths if it not exists.
    """
    if interface is None:
        interface = os
    if isinstance(abspaths, str):
        abspaths = [abspaths]
    elif isinstance(abspaths, list):
        pass
    else:
        raise Exception
    for abspath in abspaths:
        subpaths = []
        target_path, subpath = interface.path.split(abspath)
        subpaths.append(subpath)
        while not interface.path.exists(target_path):
            target_path, subpath = interface.path.split(target_path)
            subpaths.append(subpath)
        subpaths = subpaths[::-1]

        for subpath in subpaths:
            target_path = interface.path.join(target_path, subpath)
            if not interface.path.exists(target_path):
                interface.mkdir(target_path)


def remove_ext(filename):
    """Remove all extension as possible"""
    pattern = re.compile(r'([^.]*)\..*')
    return pattern.sub(r'\1', filename)


def split_ext(filename):
    if '.' in filename:
        pattern = re.compile(r'([^.]*)\.(.*)')
        return pattern.sub(r'\1', filename), pattern.sub(r'\2', filename)
    else:
        return filename, None


def change_ext(filename, ext):
    if ext is False:
        return remove_ext(filename)
    else:
        return '{}.{}'.format(remove_ext(filename), ext)


def change_fname(filename, find, replace):
    # pattern = re.compile(r'^(.*){}(.*)$'.format(find))
    # return pattern.sub(r'\1{}\2'.format(replace), filename)
    return replace.join(filename.split(find))


def get_list_addons():
    p_plugin = r'^pynipt[-_]addon[-_](?P<name>.*)$'
    list_plugin = get_installed_pkg(regex=p_plugin)

    added = dict()
    for p in list_plugin:
        pkg_name = re.match(p_plugin, p.project_name).group('name')
        added[pkg_name] = os.path.basename(p.module_path)
    return added
