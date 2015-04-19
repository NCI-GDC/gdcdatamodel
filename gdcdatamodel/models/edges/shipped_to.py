from edge import Edge
from sqlalchemy.ext.hybrid import hybrid_property


class ShippedTo(object):
    __label__ = 'shipped_to'


class AliquotShippedToCenter(ShippedTo, Edge):
    __src_class__ = 'Aliquot'
    __dst_class__ = 'Center'
    __src_dst_assoc__ = 'centers'
    __dst_src_assoc__ = 'aliquots'

    @hybrid_property
    def plate_id(self):
        return self.properties['plate_id']

    @plate_id.setter
    def plate_id(self, value):
        self.properties['plate_id'] = value

    @hybrid_property
    def plate_row(self):
        return self.properties['plate_row']

    @plate_row.setter
    def plate_row(self, value):
        self.properties['plate_row'] = value

    @hybrid_property
    def plate_column(self):
        return self.properties['plate_column']

    @plate_column.setter
    def plate_column(self, value):
        self.properties['plate_column'] = value

    @hybrid_property
    def shipment_datetime(self):
        return self.properties['shipment_datetime']

    @shipment_datetime.setter
    def shipment_datetime(self, value):
        self.properties['shipment_datetime'] = value

    @hybrid_property
    def shipment_reason(self):
        return self.properties['shipment_reason']

    @shipment_reason.setter
    def shipment_reason(self, value):
        self.properties['shipment_reason'] = value

    @hybrid_property
    def shipment_center_id(self):
        return self.properties['shipment_center_id']

    @shipment_center_id.setter
    def shipment_center_id(self, value):
        self.properties['shipment_center_id'] = value


class PortionShippedToCenter(Edge, ShippedTo):
    __src_class__ = 'Portion'
    __dst_class__ = 'Center'
    __src_dst_assoc__ = 'centers'
    __dst_src_assoc__ = 'portions'

    @hybrid_property
    def plate_id(self):
        return self.properties['plate_id']

    @plate_id.setter
    def plate_id(self, value):
        self.properties['plate_id'] = value

    @hybrid_property
    def plate_row(self):
        return self.properties['plate_row']

    @plate_row.setter
    def plate_row(self, value):
        self.properties['plate_row'] = value

    @hybrid_property
    def plate_column(self):
        return self.properties['plate_column']

    @plate_column.setter
    def plate_column(self, value):
        self.properties['plate_column'] = value

    @hybrid_property
    def shipment_datetime(self):
        return self.properties['shipment_datetime']

    @shipment_datetime.setter
    def shipment_datetime(self, value):
        self.properties['shipment_datetime'] = value

    @hybrid_property
    def shipment_reason(self):
        return self.properties['shipment_reason']

    @shipment_reason.setter
    def shipment_reason(self, value):
        self.properties['shipment_reason'] = value

    @hybrid_property
    def shipment_center_id(self):
        return self.properties['shipment_center_id']

    @shipment_center_id.setter
    def shipment_center_id(self, value):
        self.properties['shipment_center_id'] = value
