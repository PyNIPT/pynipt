# The script to check the Pipeline class is functioning.

import pynipt as pn
prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_1ses'
# prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_2ses_2runs'

pn.restore_config()
pipe = pn.Pipeline(prjpath)
pipe.plugin(interface='test/00_interface_plugin.py',
            pipeline='test/00_pipeline_plugin.py')
#%%
tmp_path = '/Users/shlee419/Projects/dataset/Template/Rat_Paxinos_400um_Template.nii.gz'
pipe.select_package(1)
pipe.set_param(template_path=tmp_path)
#%%
pipe.run(0)
pipe.check_progression()
#%%
pipe.run(1)
pipe.check_progression()
#%%
# pipe.interface.destroy_step('010')
# pipe.interface.destroy_step('01A')
# pipe.interface.destroy_step(step_code='01B', mode='masking')
# pipe.interface.destroy_step(step_code='01C', mode='masking')
# # pipe.interface.destroy_step('020')
# pipe.detach_package()
#%%
a = ('asdasdas'
    'dasd')