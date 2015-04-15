from node import *


class Annotation(Node):
    @hybrid_property
    def submitter_id(self):
        return self.properties['submitter_id']

    @submitter_id.setter
    def submitter_id(self, value):
        self.properties['submitter_id'] = value

    @hybrid_property
    def category(self):
        return self.properties['category']

    @category.setter
    def category(self, value):
        self.properties['category'] = value

    @hybrid_property
    def classification(self):
        return self.properties['classification']

    @classification.setter
    def classification(self, value):
        self.properties['classification'] = value

    @hybrid_property
    def creator(self):
        return self.properties['creator']

    @creator.setter
    def creator(self, value):
        self.properties['creator'] = value

    @hybrid_property
    def created_datetime(self):
        return self.properties['created_datetime']

    @created_datetime.setter
    def created_datetime(self, value):
        self.properties['created_datetime'] = value

    @hybrid_property
    def status(self):
        return self.properties['status']

    @status.setter
    def status(self, value):
        self.properties['status'] = value

    @hybrid_property
    def notes(self):
        return self.properties['notes']

    @notes.setter
    def notes(self, value):
        self.properties['notes'] = value
