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
        """Pipeline template for DEMO, This docstring can be used for help document.
        anat(str):      datatype for anatomical image
        func(str):      datatype for functional image
        tr(int):        the repetition time
        tpattern(str):  slice order of image
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
        Docstring can be located here
        """
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
        Docstring can be located here
        """
        self.interface.afni_MotionCorrection(input_path='010', base='02A',
                                             fourier=True, verbose=True, mparam=True,
                                             step_idx=2, sub_code=0, suffix='func')
