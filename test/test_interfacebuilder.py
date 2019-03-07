# The script for checking the method in the InterfaceBuilder class is functioning

import pynipt as pn

prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_2ses_2runs'
dset = pn.Bucket(prjpath)
proc = pn.Processor(dset, 'A_PipelineTesting', logger=True)

#%% input_method 1 test (group statistics)
step = pn.InterfaceBuilder(proc)
step.init_step(title='GroupAnalysisTestStep', suffix='func', idx=1, subcode=0,
               mode='reporting')

step.set_input(label='input', input_path='func', method=1,
               filter_dict=dict(regex=r'sub-F\d+_.*_bold', ext='nii.gz'))
step.set_output(label='output')
step.set_input(label='input2', input_path='func', method=1,
               filter_dict=dict(regex=r'sub-M\d+_.*_bold', ext='nii.gz'))

#%%
print(step._input_set)
print(step._input_ref)
print(step._output_set)

#%% static_input test
step = pn.InterfaceBuilder(proc)
step.init_step(title='StaticInputTest', suffix='func', idx=2, subcode=0,
               mode='processing')
step.set_input(label='input', input_path='func', method=0)
# set static input using first file of the filtered dset
step.set_static_input(label='static_input1', input_path='func')
# set static input using indexed file of the filtered dset
step.set_static_input(label='static_input2', input_path='func', idx=1)
# set static input using regex
step.set_static_input(label='static_input3', input_path='func',
                      filter_dict=dict(regex=r'.*run-02'))
step.set_output(label='output')

#%%
for label in step._input_set.keys():
    print(label)
    for path in step._input_set[label]:
        print('\t{}'.format(path))

#%% reset
proc.clear()


#%%
import shlex
from subprocess import Popen, PIPE
proc = Popen(shlex.split('ls'),
                               # stdin=PIPE,   # Executor not use stdin, activate later when it becomes available
                               stdin=None,
                               stdout=PIPE,
                               stderr=PIPE)

stdout, stderr = proc.communicate()