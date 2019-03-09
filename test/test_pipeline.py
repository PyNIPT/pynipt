# The script to check the Pipeline class is functioning.

import pynipt as pn
# prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_1ses'
# prjpath = '/Users/shlee419/Projects/dataset/3Drat_fMRI_2ses_2runs'
# prjpath = '/Users/shlee419/Projects/JupyterNotebooks/05_STN-opto'
prjpath = '/Users/shlee419/Projects/JupyterNotebooks/02_mPFC-DBS'

pn.restore_config()
pipe = pn.Pipeline(prjpath)
pipe.plugin(interface='../pynipt/test/00_interface_plugin.py',)
            # pipeline='../pynipt/test/00_pipeline_plugin.py')
#%%
tmp_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_EPI.nii.gz'
msk_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_Mask.nii.gz'
pipe.select_package(0)
pipe.set_param(template_path=tmp_path)
#%%
pipe.run(0)
pipe.check_progression()
#%%
pipe.run(1)
pipe.check_progression()

#%%

tmp_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_EPI.nii.gz'
msk_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_Mask.nii.gz'
pipe.select_package(0)
pipe.set_param(template_path=tmp_path)

pipe.interface.afni_Scailing(input_path='040', mask_path=msk_path,
                             mean=100, max=200, step_idx=5, sub_code='A')
pipe.interface.afni_BlurToFWHM(input_path='05A', fwhm=0.5,
                               step_idx=5, sub_code='B')
pipe.interface.afni_Deconvolution(input_path='05B', mask_path=msk_path,
                                  regex=r'.+\d{3}Hz\d{2}mW_run-\d{2}$',
                                  onset_time=[20, 50, 80, 110, 140, 170], model='BLOCK',
                                  parameters=[10, 1],
                                  step_idx=5, sub_code=0)
#%%
tmp_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_EPI.nii.gz'
msk_path = '/Users/shlee419/Projects/JupyterNotebooks/00_Templates/01_Rat/01_DukeUNC_190123/Rat_Paxinos_400um_Mask.nii.gz'
pipe.select_package(0)
pipe.set_param(template_path=tmp_path)

pipe.interface.afni_Scailing(input_path='040', mask_path=msk_path,
                             mean=100, max=200, step_idx=5, sub_code='A')
pipe.interface.afni_BlurToFWHM(input_path='05A', fwhm=0.5,
                               step_idx=5, sub_code='B')
pipe.interface.afni_Deconvolution(input_path='05B', mask_path=msk_path,
                                  regex=r'.*_task-[PI]L\d{3}Hz_.*',
                                  onset_time=[20, 50, 80], model='BLOCK',
                                  parameters=[10, 1],
                                  step_idx=5, sub_code=0)