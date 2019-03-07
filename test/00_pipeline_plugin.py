from pynipt import PipelineBuilder


class A_fMRI_Preprocessing(PipelineBuilder):
    def __init__(self, interface,
                 # User defined arguments
                 # -- start -- #
                 template_path=None,
                 anat='anat',
                 func='func',
                 tr=2,
                 tpattern='altplus',
                 # --  end  -- #
                 ):
        """Standard fMRI pipeline package for the University of North Carolina at Chapel Hill,
        Center of Animal MRI (CAMRI)
        Author  : SungHo Lee(shlee@unc.edu)
        Revised :
            ver.1: Dec.11st.2017
            ver.2: Mar.7th.2019

        template_path(str): absolute path of brain template image (default=None)
        anat(str):          datatype for anatomical image (default='anat')
        func(str):          datatype for functional image (default='func')
        tr(int):            the repetition time of EPI data
        tpattern(str):      slice order of image
            alt+z = altplus   = alternating in the plus direction
            alt+z2            = alternating, starting at slice #1 instead of #0
            alt-z = altminus  = alternating in the minus direction
            alt-z2            = alternating, starting at slice #nz-2 instead of #nz-1
            seq+z = seqplus   = sequential in the plus direction
            seq-z = seqminus  = sequential in the minus direction
        """
        super(A_fMRI_Preprocessing, self).__init__(interface)
        # User defined attributes for storing arguments
        # -- start -- #
        self.template_path = template_path
        self.anat = anat
        self.func = func
        self.tr = tr
        self.tpattern = tpattern
        # --  end  -- #

    def pipe_01_EmptyMaskPreparation(self):
        """
        The dataset will be first slice timing corrected, and average intensity map of functional image
        will be calculated on motion corrected data. In the end of this pipeline, empty image file will be
        generated in the masking path as a place holder of mask with '_mask' suffix.

        If the anatomical data were inputted, the empty mask files will be generated as well.
        """
        # Series of user defined interface commands to executed for the pipeline
        # -- start -- #
        self.interface.afni_SliceTimingCorrection(input_path=self.func,
                                                  tr=self.tr, tpattern=self.tpattern,
                                                  step_idx=1, sub_code=0)
        self.interface.afni_MotionCorrection(input_path='010', file_idx=0,
                                             fourier=True, verbose=True, mparam=False,
                                             step_idx=2, sub_code='A', suffix='base')
        self.interface.afni_MeanImageCalc(input_path='010',
                                          step_idx=1, sub_code='A')
        self.interface.afni_MakeEmptyMask(input_path='01A', file_idx=0,
                                          step_idx=1, sub_code='B', suffix=self.func)
        if self.anat is not None:
            self.interface.afni_MakeEmptyMask(input_path=self.anat, file_idx=0,
                                              step_idx=1, sub_code='C', suffix=self.anat)


    def pipe_02_CorePreprocessing(self):
        """
        Prior to run this pipeline, template_path argument need to be inputted.

        All the dataset will be motion corrected, and skull stripping will be applied.
        If the anatomical data are inputted, then functional data will be co-registered into
        anatomical space using affine registration.

        Finally, the functional data will be normalized into template space using ANTs SyN non-linear
        registration.
        """
        # Series of user defined interface commands to executed for the pipeline
        # -- start -- #

        # Check if the brain template image is exist in given path
        if self.template_path is None:
            raise Exception('No brain template image is provided.')
        else:
            if not self.interface.msi.path.exists(self.template_path):
                raise Exception('No brain template image found on given path.')

        self.interface.afni_MotionCorrection(input_path='010', base='02A',
                                             fourier=True, verbose=True, mparam=True,
                                             step_idx=2, sub_code=0, suffix=self.func)
        self.interface.afni_SkullStripping(input_path='01A', mask_path='01B', file_idx=0,
                                           step_idx=3, sub_code='A', suffix='mean{}'.format(self.func))
        if self.anat is not None:
            # if anatomy dataset is provided, then co-registration process will be applied
            self.interface.afni_SkullStripping(input_path=self.anat, mask_path='01C',
                                               step_idx=3, sub_code='B', suffix=self.anat)
            self.interface.ants_N4BiasFieldCorrection(input_path='03A', file_idx=0,
                                                      step_idx=3, sub_code='C',
                                                      suffix='mean{}'.format(self.func))
            self.interface.ants_N4BiasFieldCorrection(input_path='03B', file_idx=0,
                                                      step_idx=3, sub_code='D',
                                                      suffix=self.anat)
            self.interface.afni_Coregistration(input_path='03C', ref_path='03D', file_idx=0,
                                               step_idx=3, sub_code='E',
                                               suffix='mean{}'.format(self.func))
            self.interface.ants_SpatialNorm(input_path='03D', ref_path=self.template_path,
                                            step_idx=4, sub_code='A',
                                            suffix=self.anat)
            self.interface.afni_SkullStripping(input_path='020', mask_path='01B',
                                               step_idx=3, sub_code='F',
                                               suffix=self.func)
            self.interface.afni_ApplyTransform(input_path='03F', ref_path='03E',
                                               step_idx=3, sub_code=0,
                                               suffix=self.func)
        else:
            self.interface.afni_SkullStripping(input_path='020', mask_path='01B',
                                               step_idx=3, sub_code=0,
                                               suffix=self.func)
            self.interface.ants_SpatialNorm(input_path='03C', ref_path=self.template_path,
                                            step_idx=4, sub_code='A',
                                            suffix=self.anat)

        self.interface.ants_ApplySpatialNorm(input_path='030', ref_path='04A',
                                             step_idx=4, sub_code=0,
                                             suffix=self.func)
        # --  end  -- #