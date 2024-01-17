"""
This module was imported from cdisutils, but it has been removed: https://github.com/NCI-GDC/cdisutils/pull/39.
We need a copy because it is still used by `add_assesion_number.py`.
"""


from collections import defaultdict


class Mapping(object):
    def __init__(self):
        self.PHSID_TREE = {
            "phs000178": [],
            "phs000218": [
                "phs000463",
                "phs000464",
                "phs000465",
                "phs000515",
                "phs000471",
                "phs000466",
                "phs000470",
                "phs000467",
                "phs000468",
                "phs000469",
            ],
            "phs000235": [
                "phs000527",
                "phs000528",
                "phs000529",
                "phs000530",
                "phs000531",
                "phs000532",
                "phs000533",
            ],
        }

        self.PHSID_INV_TREE = dict(
            (v, k) for k in self.PHSID_TREE for v in self.PHSID_TREE[k]
        )

        self.PHSID_TO_PROJECT = {
            "phs000178": {
                "TCGA-LAML",
                "TCGA-ACC",
                "TCGA-BLCA",
                "TCGA-LGG",
                "TCGA-BRCA",
                "TCGA-CESC",
                "TCGA-CHOL",
                "TCGA-COAD",
                "TCGA-ESCA",
                "TCGA-GBM",
                "TCGA-HNSC",
                "TCGA-KICH",
                "TCGA-KIRC",
                "TCGA-KIRP",
                "TCGA-LIHC",
                "TCGA-LUAD",
                "TCGA-LUSC",
                "TCGA-DLBC",
                "TCGA-MESO",
                "TCGA-OV",
                "TCGA-PAAD",
                "TCGA-PCPG",
                "TCGA-PRAD",
                "TCGA-READ",
                "TCGA-SARC",
                "TCGA-SKCM",
                "TCGA-STAD",
                "TCGA-TGCT",
                "TCGA-THYM",
                "TCGA-THCA",
                "TCGA-UCS",
                "TCGA-UCEC",
                "TCGA-UVM",
                "TCGA-MISC",
                "TCGA-LCML",
                "TCGA-FPPP",
                "TCGA-CNTL",
            },
            "phs000218": [
                "TARGET-ALL-P1",
                "TARGET-ALL-P2",
                "TARGET-AML",
                "TARGET-AML-IF",
                "TARGET-WT",
                "TARGET-CCSK",
                "TARGET-RT",
                "TARGET-NBL",
                "TARGET-OS",
                "TARGET-MDLS",
            ],
            "phs000235": [
                "CGCI-BLGSP",
                "CGCI-HTMCP-CC",
                "CGCI-HTMCP-DLBCL",
                "CGCI-HTMCP-LC",
                "CGCI-MB",
                "CGCI-NHL-DLBCL",
                "CGCI-NHL-FL",
            ],
            "phs000463": ["TARGET-ALL-P1"],
            "phs000464": ["TARGET-ALL-P2"],
            "phs000465": ["TARGET-AML"],
            "phs000515": ["TARGET-AML-IF"],
            "phs000471": ["TARGET-WT"],
            "phs000466": ["TARGET-CCSK"],
            "phs000470": ["TARGET-RT"],
            "phs000467": ["TARGET-NBL"],
            "phs000468": ["TARGET-OS"],
            "phs000469": ["TARGET-MDLS"],
            "phs000527": ["CGCI-BLGSP"],
            "phs000528": ["CGCI-HTMCP-CC"],
            "phs000529": ["CGCI-HTMCP-DLBCL"],
            "phs000530": ["CGCI-HTMCP-LC"],
            "phs000531": ["CGCI-MB"],
            "phs000532": ["CGCI-NHL-DLBCL"],
            "phs000533": ["CGCI-NHL-FL"],
        }

        self.PROJECT_TO_PHSID = defaultdict(list)
        for key, projects in self.PHSID_TO_PROJECT.iteritems():
            for project in projects:
                self.PROJECT_TO_PHSID[project].append(key)

        self.PROJECT_TO_PHSID = dict(self.PROJECT_TO_PHSID)

    def get_projects(self, phsid):
        """Get a list of projects a program/project phsid map to"""
        return self.PHSID_TO_PROJECT.get(phsid, [])

    def get_phsids(self, project):
        """Get a list of phsids a project map to"""
        return self.PROJECT_TO_PHSID.get(project, [])

    def get_project_level_phsid(self, project):
        """
        Get project level phsid for a project name,
        return None if the project doesn't exist
        """
        phsids = self.get_phsids(project)
        for phsid in phsids:
            if phsid not in self.PHSID_TREE:
                return phsid

    def get_program_level_phsid(self, project):
        """
        Get program level phsid for a project name,
        return None if the project doesn't exist
        """
        phsids = self.get_phsids(project)
        for phsid in phsids:
            if phsid in self.PHSID_TREE:
                return phsid

    def get_project(self, phsid):
        """
        Get project name for a project phsid,
        return None for program phsid
        """
        if phsid in self.PHSID_TREE:
            return None
        projects = self.get_projects(phsid)
        if len(projects):
            return projects[0]
        return None

    def get_parent(self, phsid):
        """
        Get program phsid for a project phsid
        """
        return self.PHSID_INV_TREE.get(phsid)
