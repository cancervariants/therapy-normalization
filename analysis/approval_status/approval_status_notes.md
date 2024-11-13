# Notes on approval status

## ChEMBL
"Max Phase":
 * **Phase 0**: "Research: The compound has not yet reached clinical trials (preclinical/research compound)"
 * **Phase 1**: "The compound has reached Phase I clinical trials (safety studies, usually with healthy volunteers)"
 * **Phase 2**: "The compound has reached Phase II clinical trials (preliminary studies of effectiveness)"
 * **Phase 3**: "The compound has reached Phase III clinical trials (larger studies of safety and effectiveness)"
 * **Phase 4**: "The compound has been approved in at least one country or area."

## Drugs@FDA

"Marketing Status":
 * **Prescription**: "A prescription drug product requires a doctor's authorization to purchase."
 * **Over-the-counter**: "FDA defines OTC drugs as safe and effective for use by the general public without a doctor's prescription."
 * **Discontinued**: "approved products that have never been marketed, have been discontinued from marketing, are for military use, are for export only, or have had their approvals withdrawn for reasons other than safety or efficacy after being discontinued from marketing"
 * **None (Tentatively Approved)**: "If a generic drug product is ready for approval before the expiration of any patents or exclusivities accorded to the reference listed drug product, FDA issues a tentative approval letter to the applicant. FDA delays final approval of the generic drug product until all patent or exclusivity issues have been resolved. "

## RxNorm

 * **Current Prescribable Content**: "The RxNorm Current Prescribable Content is a subset of currently prescribable drugs found in RxNorm. We intend it to be an approximation of the prescription drugs currently marketed in the US. The subset also includes many over-the-counter drugs." https://www.nlm.nih.gov/research/umls/rxnorm/docs/prescribe.html

## HemOnc.org

 * **Discontinued**: "Drugs that have lost FDA approval" https://hemonc.org/wiki/Style_guide#Drugs_that_have_lost_FDA_approval. This is not actually included in the downloadable data -- for example, Mylotarg was withdrawn from the market but may still be worth investigating for other indications.
 * **"Was FDA approved yr"**: Appears to just be a claim about (first?) FDA approval date. No indication of later withdrawal.

# Proposed nomenclature

 * **null/not categorized**:
   * Drugs@FDA discontinued
   * Drugs@FDA tentatively approved
 * **PRECLINICAL**: not yet in trials
   * ChEMBL phase 0
 * **CLINICAL_TRIAL**: currently in active clinical trials
   * ChEMBL phase 1
   * ChEMBL phase 2
   * ChEMBL phase 3
 * **APPROVED**: has been approved, somewhere, at some point (may be discontinued)
   * ChEMBL phase 4
   * Drugs@FDA prescription
   * Drugs@FDA over-the-counter
   * RxNorm current prescribable content
   * HemOnc was FDA approved

~Conflict resolution: highest out of approved > clinical trial > preclinical > null gets added to normalized group~

New requirements:
 * DGIdb to include a list of all NDA/ANDA entries associated with a drug + FDA marketing status


def ApprovalStatus(Enum):

    CHEMBL_0 = "chembl_phase_0"
    CHEMBL_1 = "chembl_phase_1"
    CHEMBL_2 = "chembl_phase_2"
    CHEMBL_3 = "chembl_phase_3"
    CHEMBL_4 = "chembl_phase_4"
    HEMONC_APPROVED = "hemonc_approved"
    FDA_OTC = "fda_otc"
    FDA_PRESCRIPTION = "fda_prescription"
    FDA_DISCONTINUED = "fda_discontinued"
    FDA_TENTATIVE = "fda_tentative"
