# The script for checking the method in the InterfaceBuilder class is functioning

import pynipt as pn

# prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_2ses_2runs'
prjpath = '/Users/shlee419/Projects/JupyterNotebooks/05_STN-opto'
dset = pn.Bucket(prjpath)
# proc = pn.Processor(dset, 'A_PipelineTesting', logger=True)
proc = pn.Processor(dset, 'A_fMRI_Preprocessing', logger=True)


#%% input_method 1 test (group statistics)
step = pn.InterfaceBuilder(proc)
step.init_step(title='GroupAnalysisTestStep', suffix='func', idx=6, subcode=0,
               mode='reporting')

step.set_input(label='input', input_path='050', method=1,
               filter_dict=dict(regex=r'sub-oSTN\d{3}_task-130Hz10mW_run-\d{2}$', ext='nii.gz'))
# step.set_output(label='output', modifier='130Hz10mW_stim')
step.set_output(label='output', ext='nii.gz')
#%%
print(step._input_set)
print(step._input_ref)
print(step._output_set)

#%%
step.set_cmd('3dttest++ -setA *[input] -prefix *[output]')

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
