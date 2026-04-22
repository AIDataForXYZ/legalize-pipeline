"""Iterative fidelity loop for Spain parser refactor.

Three phases per iteration:
1. sample: pick BOE IDs stratified across rango × decade × content-tags
2. score:  fetch XML + BOE HTML, render via current parser, diff against BOE
3. report: aggregate defects across the cohort, identify top fix target

Exit criteria: §5.5 of RESEARCH-ES-v2.md.
"""
