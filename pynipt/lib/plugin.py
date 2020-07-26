import importlib
import importlib.util as imp_util
import re
from inspect import getsource
from shleeh.utils import get_installed_pkg, user_warning
from ..errors import *


class PluginParser:
    """ plugin parser, the input must be single file module not whole pipeline package """
    def __init__(self, obj):
        # privates
        self._code = getsource(obj).split('\n')
        self._obj = obj
        self._meta = dict()
        self._avail = list()
        self._num_plugins = 0
        self._type = None

        # execute protected methods
        self._meta_parser()
        self._construct()

    @property
    def avail(self):
        return sorted(self._avail)

    @property
    def type(self):
        return self._type

    @property
    def obj(self):
        return self._obj

    def _meta_parser(self):
        if self._type is not None:   # raise error when executing this multiple times.
            raise InvalidApproach

        # patterns
        p_space = re.compile(r'^\s*$')
        p_class = re.compile(r'^class\s(?P<name>[0-9a-zA-Z_]+)\((?P<type>Processor|PipelineBuilder)\):')
        p_method = re.compile(r'^def\s[0-9a-zA-Z_]+\(self.*')
        p_closed = re.compile(r'.*\):')
        p_complete_method = re.compile(r'^def\s(?P<name>[0-9a-zA-Z_]+)\((?P<argv>self.*)\):$')

        # parsing meta data from plugin
        class_parser = self._meta
        class_name = None
        for idx, line in enumerate(self._code):
            if self.indentation(line) == 0:  # class header level
                if not p_space.match(line):
                    if p_class.match(line):
                        class_type = p_class.match(line).group('type')
                        if class_type == 'Processor':
                            self._type = 'interface'
                        else:
                            self._type = 'pipeline'
                        class_name = p_class.match(line).group('name')
                        class_parser[class_name] = dict(idx=idx, args=[], kwargs=dict(), methods=dict())

            if class_name is not None:
                if self.indentation(line) == 1:  # method header level
                    if p_method.match(line.strip()) and p_closed.match(line.strip()):
                        parsed_method = line.strip()
                    elif p_method.match(line.strip()) and not p_closed.match(line.strip()):
                        opened = True
                        line_parser = []
                        i = idx + 1
                        while opened:
                            # loop until fine the last line
                            stripped_line = self.strip_code(self._code[i])
                            line_parser.append(stripped_line.strip())
                            if p_closed.match(stripped_line):
                                opened = False
                            i += 1
                        parsed_method = f'{line.strip()}{"".join(line_parser)}'
                    else:
                        parsed_method = ''
                    if bool(parsed_method):
                        method = p_complete_method.match(parsed_method)
                        if method:
                            method_name = method.group('name')

                            p_args = re.compile(r'(?P<key>[a-zA-Z0-9_]+)\s*=\s*[\'\"]?(?P<value>[^\'\"]+)[\'\"]?')
                            if method_name == '__init__':
                                for arg in method.group('argv').split(','):
                                    arg = arg.strip()
                                    if arg != 'self':
                                        arg_obj = p_args.match(arg)
                                        if arg_obj:
                                            class_parser[class_name]['kwargs'][arg_obj.group('key')] \
                                                = self.convert_value(arg_obj.group('value'))
                                        else:
                                            if re.match(r'^[*]+.*', arg):
                                                pass
                                            else:
                                                class_parser[class_name]['args'].append(arg)
                            else:
                                method_parser = class_parser[class_name]['methods']
                                method_parser[method_name] = dict(idx=idx, args=[], kwargs=dict())
                                for arg in method.group('argv').split(','):
                                    arg = arg.strip()
                                    if arg != 'self':
                                        arg_obj = p_args.match(arg)
                                        if arg_obj:
                                            key = arg_obj.group('key')
                                            value = self.convert_value(arg_obj.group('value'))
                                            method_parser[method_name]['kwargs'][key] = value
                                        else:
                                            method_parser[method_name]['args'].append(arg)
        if class_name is None:
            raise InvalidPlugin

    def _construct(self):
        if self._num_plugins:   # raise error when executing this multiple times.
            raise InvalidApproach
        if self.type == 'interface':
            for _, plugin in self._meta.items():
                self._num_plugins += 1
                for method, param in plugin['methods'].items():
                    self._avail.append(method)

        elif self.type == 'pipeline':
            for pkg_name, pipelines in self._meta.items():
                self._num_plugins += 1
                self._avail.append(pkg_name)
                setattr(self, 'pipelines', list(pipelines['methods'].keys()))
                setattr(self, 'arguments', pipelines['args'] + list(pipelines['kwargs'].keys()))
        else:
            raise UnexpectedError

    @staticmethod
    def strip_code(text: str) -> str:
        p_comment = re.compile(r'(?P<code>.*)(?P<comment>\s+#.*)')
        match_obj = p_comment.match(text)
        if match_obj:
            if re.match(r'^#.*', text.strip()):
                return ''
            else:
                return match_obj.group('code').strip()
        else:
            return text.strip()

    @staticmethod
    def convert_value(value: str) -> object:
        if re.compile(r'^[0-9]+\.[0-9]+$').match(value):
            return float(value)
        if value == 'None':
            return None
        elif value == 'True':
            return True
        elif value == 'False':
            return False
        elif value.isdigit():
            return int(value)
        elif value.isdecimal():
            return float(value)
        else:
            return value

    @staticmethod
    def indentation(text: str, tabsize: int = 4) -> int:
        expanded_text = text.expandtabs(tabsize)
        return 0 if expanded_text.isspace() else int((len(expanded_text) - len(expanded_text.lstrip()))/4)


class PluginLoader:
    """ Plugin loader for Pipeline class """
    def __init__(self):
        # public
        self.pipeline_objs = dict()
        self.interface_objs = dict()

        # privates
        self._imported_interfaces = []
        self._imported_pipelines = []
        self._installed = []
        self._invalid_plugins = dict()
        self._duplicated_interfaces = dict()
        self._duplicated_pipelines = dict()

        # execute protected methods
        self._parse_plugins()
        self._check_conflicts()

    @property
    def avail_pkgs(self):
        return self._available_pkgs()

    def _parse_plugins(self):
        import os
        p_plugin = r'^pynipt[-_]plugin[-_](?P<name>.*)$'
        list_plugin = get_installed_pkg(regex=p_plugin)

        issued_pkg = dict()

        for p in list_plugin:
            pkg_name = re.match(p_plugin, p.project_name).group('name')
            with open(os.path.join(p.egg_info, 'top_level.txt'), 'r') as f:
                mod_name = f.readline().strip()
            if not os.path.exists(os.path.join(p.module_path, mod_name)):
                raise UnexpectedError('Module name does not matched.')
            module = importlib.import_module(mod_name)
            if not hasattr(module, 'interface') and not hasattr(module, 'pipeline'):
                if pkg_name not in issued_pkg.keys():
                    issued_pkg[pkg_name] = 'invalid'
            else:
                if hasattr(module, 'interface'):
                    if mod_name in self._imported_interfaces:
                        if pkg_name not in issued_pkg.keys():
                            issued_pkg[pkg_name] = []
                        issued_pkg[pkg_name].append('interface')
                    else:
                        try:
                            self.interface_objs[mod_name] = PluginParser(module.interface)
                            self._imported_interfaces.append(mod_name)
                        except InvalidPlugin:
                            user_warning(f'The interface plugin in {pkg_name} is invalid.')
                if hasattr(module, 'pipeline'):
                    if mod_name in self._imported_pipelines:
                        if pkg_name not in issued_pkg.keys():
                            issued_pkg[pkg_name] = []
                        issued_pkg[pkg_name].append('pipeline')
                    else:
                        try:
                            self.pipeline_objs[mod_name] = PluginParser(module.pipeline)
                            self._imported_pipelines.append(mod_name)
                        except InvalidPlugin:
                            user_warning(f'The pipeline plugin in {pkg_name} is invalid.')
            if pkg_name not in issued_pkg.keys():
                self._installed.append(pkg_name)
        if issued_pkg:
            self._invalid_plugins = issued_pkg

    def _check_conflicts(self):
        interface_items = []
        for mod_name, itfs in self.interface_objs.items():
            for m in itfs.avail:
                if m in interface_items:
                    if mod_name not in self._duplicated_interfaces.keys():
                        self._duplicated_interfaces[mod_name] = []
                    self._duplicated_interfaces[mod_name].append(m)
                else:
                    interface_items.append(m)
        pipeline_items = []
        for mod_name, pkgs in self.pipeline_objs.items():
            for p in pkgs.avail:
                if p in pipeline_items:
                    if mod_name not in self._duplicated_pipelines.keys():
                        self._duplicated_pipelines[mod_name] = []
                    self._duplicated_pipelines[mod_name].append(p)
                else:
                    pipeline_items.append(p)

    def from_file(self, name: str, file_path: str):
        import os
        file_path = os.path.expanduser(file_path)
        if not os.path.isfile(file_path):
            raise FileNotFoundError
        spec = imp_util.spec_from_file_location(name, file_path)
        module = imp_util.module_from_spec(spec)
        plugin_obj = PluginParser(module)

        if plugin_obj.type == 'interface':
            if name in self._imported_interfaces:
                raise ConflictPlugin
            self.interface_objs[name] = plugin_obj
            self._imported_interfaces.append(name)

        elif plugin_obj.type == 'pipeline':
            if name in self._imported_pipelines:
                raise ConflictPlugin
            self.pipeline_objs[name] = plugin_obj
            self._imported_pipelines.append(name)
        else:
            raise FileNotValidError
        self._check_conflicts()

    def get_interfaces(self):
        """ Return combined interface object """
        imported_interfaces = []
        for _, itf in self.interface_objs.items():
            imported_interfaces.append(itf.obj.Interface)

        class ImportedInterface(*imported_interfaces):
            def __init__(self, *args, **kwargs):
                # from ..config import config
                # from collections import OrderedDict
                #
                # cfg = config['Preferences']
                # if 'n_threads' in kwargs.keys():
                #     if kwargs['n_threads'] is None:
                #         self._n_threads = cfg.getint('number_of_threads')
                #     else:
                #         self._n_threads = kwargs.pop('n_threads')
                # else:
                #     self._n_threads = cfg.getint('number_of_threads')
                super(ImportedInterface, self).__init__(*args, **kwargs)

                # install default interface in plugin folder
                # to control scheduling issues,
                # self._waiting_list = []
                # self._processed_list = []
                # self._running_obj = OrderedDict()
                # self.update()

        return ImportedInterface

    def get_pkgs(self, idx):
        pkg_name = self.avail_pkgs[idx]
        for _, pkg in self.pipeline_objs.items():
            if pkg_name in pkg.avail:
                return pkg.obj

    def _available_pkgs(self):
        imported_pkgs = []
        for _, pkgs in self.pipeline_objs.items():
            imported_pkgs.extend(pkgs.avail)
        n_pipe = len(imported_pkgs)
        return dict(zip(range(n_pipe), imported_pkgs))


if __name__ == '__main__':
    plugins = PluginLoader()
    # plugins.from_file('test', '~/.pynipt/plugin/interface_default.py')
    # plugins.from_file('test', '~/.pynipt/plugin/pipeline_default.py')

    for name_, contents in plugins.interface_objs.items():
        print(name_)
        print(f"\t{contents.avail}")
    for name_, contents in plugins.pipeline_objs.items():
        print(name_)
        print(f"\t{contents.avail}")
        print(f"\t{contents.pipelines}")
        print(f"\t{contents.arguments}")

    a = plugins.get_interfaces()
