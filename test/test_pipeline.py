# The script to check the Pipeline class is functioning.
import sys
paralexe_module = '/Users/shlee419/Projects/Released/paralexe'
sys.path.append(paralexe_module)

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
pipe.interface.ants_ApplySpatialNorm(input_path='030', ref_path='04A',
                                             step_idx=4, sub_code=0,
                                             suffix='func')